module KubectlClient
  module Get
    @logger : ::Log = Log.for("get")

    @@schedulable_nodes_template : String = <<-GOTEMPLATE.strip
    {{- range .items -}}
      {{$taints:=""}}
      {{- range .spec.taints -}}
        {{- if eq .effect "NoSchedule" -}}
          {{- $taints = print $taints .key "," -}}
        {{- end -}}
      {{- end -}}
      {{- if not $taints -}}
        {{- .metadata.name}}
        {{- "\\n" -}}
      {{end -}}
    {{- end -}}
    GOTEMPLATE

    def self.resource(kind : String, resource_name : String?, namespace : String? = nil,
                      all_namespaces : Bool = false, field_selector : String = "") : JSON::Any
      logger = @logger.for("resource")
      logger.info { "Get resource #{kind}" }
      logger.info { "/#{resource_name.to_s}" } if resource_name

      # resource_name.to_s will expand to "" in case of nil
      cmd = "kubectl get #{kind} #{resource_name.to_s}"
      cmd = "#{cmd} --field-selector #{field_selector}" if field_selector
      cmd = "#{cmd} -n #{namespace}" if namespace && !all_namespaces
      cmd = "#{cmd} -A" if !namespace && all_namespaces
      # RLTODO: ensure -o json in all methods is always at the end
      cmd = "#{cmd} -o json"

      result = ShellCMD.raise_exc_on_error &.ShellCMD.run(cmd, logger)

      parse_get_result(result)
    end

    def self.privileged_containers(namespace = "--all-namespaces") : JSON::Any
      logger = @logger.for("privileged_containers")
      cmd = "kubectl get pods #{namespace} -o jsonpath='{.items[*].spec.containers[?(@.securityContext.privileged==true)].name}'"
      result = ShellCMD.raise_exc_on_error &.ShellCMD.run(cmd, logger)

      parse_get_result(result)
    end

    private def self.resource_map(k8s_manifest, &block)
      if nodes["items"]?
        items = nodes["items"].as_a.map do |item|
          if nodes["metadata"]?
            metadata = nodes["metadata"]
          else
            metadata = JSON.parse(%({}))
          end
          yield item, metadata
        end
        items
      else
        [JSON.parse(%({}))]
      end
    end

    private def self.resource_select(&block)
      if nodes["items"]?
        items = nodes["items"].as_a.select do |item|
          if nodes["metadata"]?
            metadata = nodes["metadata"]
          else
            metadata = JSON.parse(%({}))
          end
          yield item, metadata
        end
        items
      else
        [] of JSON::Any
      end
    end

    private def self.resource_select(k8s_manifest, &block)
      if nodes["items"]?
        items = nodes["items"].as_a.select do |item|
          if nodes["metadata"]?
            metadata = nodes["metadata"]
          else
            metadata = JSON.parse(%({}))
          end
          yield item, metadata
        end
        items
      else
        [] of JSON::Any
      end
    end

    # TODO: why is this retried at all and even if this is needed, why inside method and not by callers?
    # why is 'nodes' method failing when its very simple call?
    def self.schedulable_nodes_list(retry_limit : Int32 = 20) : Array(JSON::Any)
      logger = @logger.for("schedulable_nodes_list")
      logger.info { "Retrieving list of schedulable nodes" }

      retries = 1
      empty_json_any = [] of JSON::Any
      nodes = empty_json_any
      # Get.nodes seems to have failures sometimes
      until (nodes != empty_json_any) || retries > retry_limit
        nodes = KubectlClient::Get.resource_select(KubectlClient::Get.nodes) do |item, metadata|
          taints = item.dig?("spec", "taints")
          if (taints && taints.as_a.find { |x| x.dig?("effect") == "NoSchedule" })
            false
          else
            true
          end
        end
        sleep 1
        retries = retries + 1
      end

      if nodes == empty_json_any
        logger.warn { "Could not retrieve any schedulable nodes" }
      end
      logger.debug { "Schedulable nodes list: #{nodes}" }

      nodes
    end

    # TODO: why is this retried at all and even if this is needed, why inside method and not by callers?
    # why is 'nodes' method failing when its very simple call?
    def self.nodes_by_resource(resource, retry_limit = 20) : Array(JSON::Any)
      logger = @logger.for("nodes_by_resource")
      full_resource = "#{resource.dig?("kind")}/#{resource.dig?("metadata", "name")}"
      logger.info {
        "Retrieving list of that have resource: #{full_resource}"
      }

      retries = 1
      empty_json_any = [] of JSON::Any
      nodes = empty_json_any
      # Get.nodes seems to have failures sometimes
      until (nodes != empty_json_any) || retries > retry_limit
        # TODO: matching only by the name might not be enough and this can lead to unexpected behavior
        nodes = KubectlClient::Get.resource_select() do |item, metadata|
          item.dig?("metadata", "name") == resource.dig?("metadata", "name")
        end
        retries = retries + 1
      end

      if nodes == empty_json_any
        logger.warn { "Could not retrieve any nodes with #{full_resource}" }
      end
      logger.debug { "Nodes with resource #{full_resource} list: #{nodes}" }

      nodes
    end

    # TODO: why is this retried at all and even if this is needed, why inside method and not by callers?
    # why is 'nodes' method failing when its very simple call?
    def self.nodes_by_pod(pod, retry_limit = 3) : Array(JSON::Any)
      logger = @logger.for("nodes_by_pod")
      pod_name = "#{pod.dig?("metadata", "name")}"
      logger.info { "Finding nodes with pod/#{pod_name}" }

      retries = 1
      empty_json_any = [] of JSON::Any
      nodes = empty_json_any
      # Get.nodes seems to have failures sometimes
      until (nodes != empty_json_any) || retries > retry_limit
        nodes = KubectlClient::Get.resource_select() do |item, metadata|
          item.dig?("metadata", "name") == pod.dig?("spec", "nodeName")
        end
        retries = retries + 1
      end

      if nodes == empty_json_any
        logger.warn { "Could not retrieve any node with pod/#{pod_name}" }
      end
      logger.debug { "Nodes with pod/#{pod_name} list: #{nodes}" }

      nodes
    end

    def self.pods_by_nodes(nodes_json : Array(JSON::Any))
      logger = @logger.for("pods_by_nodes")
      logger.info { "Creating list of pods found on nodes" }

      pods_a = nodes_json.map { |item|
        node_name = item.dig?("metadata", "labels", "kubernetes.io/hostname")
        pods = KubectlClient::Get.pods.as_h["items"].as_a.select do |pod|
          if pod.dig?("spec", "nodeName") == "#{node_name}"
            pod_name = pod.dig?("metadata", "name")
            true
          else
            false
          end
        end
      }.flatten
      logger.debug { "Pods found: #{pods_a}" }

      pods
    end

    # todo default flag for schedulable pods vs all pods
    def self.pods_by_resource_labels(resource_json : JSON::Any, namespace : String? = nil) : Array(JSON::Any)
      logger = @logger.for("pods_by_resources")
      kind = resource_json["kind"]?
      name = resource_json["metadata"]["name"]?
      logger.info { "Creating list of pods by resource : #{kind}/#{name} labels" }

      return [resource_json] if resource_yml["kind"].as_s.downcase == "pod"
      if !name || !kind
        logger.warn { "Passed resource is nil" }
        return [] of JSON::Any
      end

      pods = pods()
      # todo deployment labels may not match template metadata labels.
      # -- may need to match on selector matchlabels instead
      labels = resource_spec_labels(resource_json["kind"].as_s, name.as_s, namespace: namespace).as_h
      filtered_pods = pods_by_labels(pods, labels)

      filtered_pods
    end

    def self.pods_by_labels(pods_json : Array(JSON::Any), labels : Hash(String, JSON::Any))
      logger = @logger.for("pods_by_labels")
      logger.info { "Creating list of pods that have labels: #{labels}" }

      pods_json.select do |pod|
        if labels == Hash(String, JSON::Any).new
          match = false
        else
          match = true
        end
        # todo deployment labels may not match template metadata labels.
        # -- may need to match on selector matchlabels instead
        labels.map do |key, value|
          if pod.dig?("metadata", "labels", key) == value
            match = true
          else
            match = false
          end
        end
        match
      end
      logger.debug { "Matched #{pods_json.size} pods: #{pods_json.map { |item| item.dig?("metadata", "name").as_s }.join(", ")}" }

      pods_json
    end

    def self.service_by_pod(pod) : JSON::Any?
      logger = @logger.for("service_by_pod")
      pod_name = pod.dig("metadata", "name")
      logger.info { "Matching pod: #{pod_name} to service" }

      services = KubectlClient::Get.resource("service", all_namespaces: true)
      matched_service : JSON::Any?
      services["items"].as_a.each do |service|
        service_labels = service.dig?("spec", "selector")
        next unless service_labels

        pods = KubectlClient::Get.resource("pods", all_namespaces: true)
        service_pods = KubectlClient::Get.pods_by_labels(pods["items"].as_a, service_labels.as_h)
        service_pods.each do |service_pod|
          service_name = service_pod.dig("metadata", "name")
          matched_service = service if service_name == pod_name
        end
      end

      if matched_service.nil?
        logger.warn { "Could not match pod to any service" }
      end
      logger.debug { "Pod: #{pod_name} matched to service: #{matched_service.dig("metadata", "name").as_s}" }

      matched_service
    end

    def self.pods_by_service(service) : Array(JSON::Any)?
      logger = @logger.for("pods_by_service")
      logger.info { "Matching pods to service: #{service.dig?("metadata", "name")}" }

      service_labels = service.dig?("spec", "selector")
      return unless service_labels

      pods = KubectlClient::Get.pods
      service_pods = KubectlClient::Get.pods_by_labels(pods["items"].as_a, service_labels.as_h)
    end

    def self.pods_by_digest(container_digest) : Array(JSON::Any)
      logger = @logger.for("pods_by_digest")
      logger.info { "Matching pods to digest: #{container_digest}" }

      matched_pods = [] of JSON::Any
      pods = KubectlClient::Get.resource("pods", all_namespaces: true)
      pods["items"].as_a.each do |pod|
        statuses = pod.dig?("status", "containerStatuses")
        if statuses
          statuses.as_a.each do |status|
            matched_pods << pod if status.dig("imageID").as_s.includes?("#{container_digest}")
          end
        end
      end
      logger.debug { "Matched #{matched_pods.size} pods: #{matched_pods.map { |item| item.dig?("metadata", "name").as_s }.join(", ")}" }

      matched_pod
    end

    def self.resource_containers(kind : String, resource_name : String, namespace : String? = nil) : JSON::Any
      logger = @logger.for("resource_containers")
      logger.info { "Get containers of #{kind}/#{resource_name}" }

      case kind.downcase
      when "pod"
        resp = resource(kind, resource_name, namespace).dig?("spec", "containers")
      when "deployment", "statefulset", "replicaset", "daemonset"
        resp = resource(kind, resource_name, namespace).dig?("spec", "template", "spec", "containers")
      end
    end

    def self.resource_volumes(kind : String, resource_name : String, namespace : String? = nil) : JSON::Any
      logger = @logger.for("resource_volumes")
      logger.info { "Get volumes of #{kind}/#{resource_name}" }

      case kind.downcase
      when "pod"
        resp = resource(kind, resource_name, namespace).dig?("spec", "volumes")
      when "deployment", "statefulset", "replicaset", "daemonset"
        resp = resource(kind, resource_name, namespace).dig?("spec", "template", "spec", "volumes")
      end
    end

    private def self.replica_count(kind : String, resource_name : String,
                                   namespace : String? = nil) : NamedTuple(current: Int32, desired: Int32, unavailable: Int32)
      logger = @logger.for("replica_count")
      logger.debug { "Get replica count of #{kind}/#{resource_name}" }

      case kind.downcase
      when "replicaset", "deployment", "statefulset"
        current = resource(kind, resource_name, namespace).dig?("status", "readyReplicas")
        desired = resource(kind, resource_name, namespace).dig?("status", "replicas")
        unavailable = resource(kind, resource_name, namespace).dig?("status", "unavailableReplicas")
      when "daemonset"
        current = resource(kind, resource_name, namespace).dig?("status", "numberAvailable")
        desired = resource(kind, resource_name, namespace).dig?("status", "desiredNumberScheduled")
        unavailable = resource(kind, resource_name, namespace).dig?("status", "unavailableReplicas")
      end
      logger.trace { "replicas: current = #{current}, desired = #{desired}, unavailable = #{unavailable}" }

      {current: current, desired: desired, unavailable: unavailable}
    end

    # TODO add a spec for this
    def self.pod_ready?(pod_name_prefix : String, field_selector : String = "", namespace : String? = nil) : Bool
      logger = @logger.for("pod_status")
      logger.info { "Get status of pod/#{pod_name_prefix}* with field selector: #{field_selector}" }

      all_pods_json = resource("pod", pod_name, namespace, field_selector: field_selector)
      all_pods = all_pods_json.dig?("items").as_a
        .select do |pod_json|
          pod_name = pod_json.dig?("metadata", "name").as_s
          pod_name =~ /pod_name_prefix/ &&
            pod_json.dig?("status", "containerStatuses").as_a { |cstatus| cstatus.dig?("ready").as_b }
        end
        .map { |pod_json| pod_json.dig?("metadata", "name").as_s }

      logger.debug { "'Ready' pods: #{all_pods.join(", ")}" }
      return all_pods.size > 0 ? true : false
    end

    # RLTODO: IM HERE
    def self.node_status(node_name)
      cmd = "kubectl get nodes #{node_name} -o jsonpath='{.status.conditions[?(@.type == \"Ready\")].status}'"
      result = ShellCMD.run(cmd, "KubectlClient::Get.node_status")
      result[:output]
    end

    def self.resource_spec_labels(kind : String, resource_name : String, namespace : String? = nil) : JSON::Any
      Log.debug { "resource_labels kind: #{kind} resource_name: #{resource_name}" }
      case kind.downcase
      when "service"
        resp = resource(kind, resource_name, namespace: namespace).dig?("spec", "selector")
      when "deployment", "statefulset", "replicaset", "daemonset", "job"
        resp = resource(kind, resource_name, namespace: namespace).dig?("spec", "selector", "matchLabels")
      else
        resp = resource(kind, resource_name, namespace: namespace).dig?("spec", "template", "metadata", "labels")
      end
      Log.debug { "resource_labels: #{resp}" }
      if resp
        resp
      else
        JSON.parse(%({}))
      end
    end

    def self.container_image_tags(deployment_containers) : Array(NamedTuple(image: String,
      tag: String))
      image_tags = deployment_containers.as_a.map do |container|
        Log.debug { "container (should have image and tag): #{container}" }
        {image: container.as_h["image"].as_s.rpartition(":")[0],
        # TODO an image may not have a tag
         tag: container.as_h["image"].as_s.rpartition(":")[2]?}
      end
      Log.debug { "image_tags: #{image_tags}" }
      image_tags
    end

    def self.worker_nodes : Array(String)
      # Full command:
      #
      # kubectl get nodes --selector='!node-role.kubernetes.io/master' -o 'go-template={{range .items}}{{$taints:=""}}{{range .spec.taints}}{{if eq .effect "NoSchedule"}}{{$taints = print $taints .key ","}}{{end}}{{end}}{{if not $taints}}{{.metadata.name}}{{ "\\n"}}{{end}}{{end}}'

      cmd = "kubectl get nodes --selector='!node-role.kubernetes.io/master' -o 'go-template=#{@@schedulable_nodes_template}'"
      result = ShellCMD.run(cmd, "KubectlClient::Get.worker_nodes")
      result[:output].split("\n")
    end

    def self.pv_items_by_claim_name(claim_name)
      items = pv["items"].as_a.map do |x|
        begin
          if x["spec"]["claimRef"]["name"] == claim_name
            x
          else
            nil
          end
        rescue ex
          Log.info { ex.message }
          nil
        end
      end.compact
      Log.debug { "pv items : #{items}" }
      items
    end

    def self.container_runtimes
      runtimes = nodes["items"].as_a.map do |x|
        x["status"]["nodeInfo"]["containerRuntimeVersion"].as_s
      end
      Log.info { "runtimes: #{runtimes}" }
      runtimes.uniq
    end

    # *pod_exists* returns true if a pod containing *pod_name* exists, regardless of status.
    # If *check_ready* is set to true, *pod_exists* validates that the pod exists and
    # has a ready status of true
    def self.pod_exists?(pod_name, check_ready = false, all_namespaces = false)
      Log.debug { "pod_exists? pod_name: #{pod_name}" }
      exists = pods(all_namespaces)["items"].as_a.any? do |x|
        (name_comparison = x["metadata"]["name"].as_s? =~ /#{pod_name}/
        (x["metadata"]["name"].as_s? =~ /#{pod_name}/) ||
          (x["metadata"]["generateName"]? && x["metadata"]["generateName"].as_s? =~ /#{pod_name}/)) &&
          (check_ready && (x["status"]["conditions"].as_a.find { |x| x["type"].as_s? == "Ready" } && x["status"].as_s? == "True") || check_ready == false)
      end
      Log.debug { "pod exists: #{exists}" }
      exists
    end

    def self.all_pod_statuses
      statuses = pods["items"].as_a.map do |x|
        x["status"]
      end
      Log.debug { "pod statuses: #{statuses}" }
      statuses
    end

    def self.all_pod_container_statuses
      statuses = all_pod_statuses.map do |x|
        # todo there are some pods that dont have containerStatuses
        if x["containerStatuses"]?
          x["containerStatuses"].as_a
        else
          [] of JSON::Any
        end
      end
      statuses
    end

    def self.all_container_repo_digests
      imageids = all_pod_container_statuses.reduce([] of String) do |acc, x|
        acc | x.map { |i| i["imageID"].as_s }
      end
      Log.debug { "pod container image ids: #{imageids}" }
      imageids
    end

    def self.pod_statuses_by_nodes(nodes)
      pods = KubectlClient::Get.pods_by_nodes(nodes)
      Log.debug { "pod_statuses_by_nodes pods_by_nodes pods: #{pods}" }
      statuses = pods.map do |x|
        x["status"]
      end
      Log.debug { "pod_statuses_by_nodes statuses: #{statuses}" }
      statuses
    end

    def self.pod_container_statuses_by_nodes(nodes)
      statuses = pod_statuses_by_nodes(nodes).map do |x|
        # todo there are some pods that dont have containerStatuses
        if x["containerStatuses"]?
          x["containerStatuses"].as_a
        else
          [] of JSON::Any
        end
      end
      Log.debug { "pod_container_statuses_by_nodes containerStatuses: #{statuses}" }
      statuses
    end

    def self.container_digests_by_nodes(nodes)
      Log.debug { "container_digests_by_nodes nodes: #{nodes}" }
      imageids = pod_container_statuses_by_nodes(nodes).reduce([] of String) do |acc, x|
        if x
          acc | x.map { |i| i["imageID"].as_s }
        else
          acc
        end
      end
      Log.info { "container_digests_by_nodes image ids: #{imageids}" }
      imageids
    end

    def self.container_images_by_nodes(nodes)
      Log.debug { "container_images_by_nodes nodes: #{nodes}" }
      images = pod_container_statuses_by_nodes(nodes).reduce([] of String) do |acc, x|
        if x
          acc | x.map { |i| i["image"].as_s }
        else
          acc
        end
      end
      Log.info { "container_images_by_nodes images: #{images}" }
      images
    end

    # todo match against multiple images
    # def self.container_tag_from_image_by_nodes(images : Array(String), nodes)
    #   images.map{|x| container_tag_from_image_by_nodes(x, nodes)}.flatten.concat
    # end
    def self.container_tag_from_image_by_nodes(image, nodes)
      Log.info { "container_tag_from_image_by_nodes image: #{image}" }
      Log.debug { "container_tag_from_image_by_nodes nodes: #{nodes}" }
      # TODO Remove duplicates & and support multiple?
      all_images = container_images_by_nodes(nodes).flatten
      Log.info { "container_tag_from_image_by_nodes all_images: #{all_images}" }
      matched_image = all_images.select { |x| x.includes?(image) }
      Log.info { "container_tag_from_image_by_nodes matched_image: #{matched_image}" }
      parsed_image = DockerClient.parse_image("#{matched_image[0]}") if matched_image.size > 0
      tags = parsed_image["tag"] if parsed_image
      Log.info { "container_tag_from_image_by_nodes tags: #{tags}" } if tags
      tags
    end

    def self.pods_by_digest_and_nodes(digest, nodes = KubectlClient::Get.nodes["items"].as_a)
      Log.info { "pods_by_digest_and_nodes" }
      digest_pods = nodes.map { |item|
        Log.info { "items labels: #{item.dig?("metadata", "labels")}" }
        node_name = item.dig?("metadata", "labels", "kubernetes.io/hostname")
        Log.debug { "NodeName: #{node_name}" }
        pods = KubectlClient::Get.pods.as_h["items"].as_a.select do |pod|
          found = false
          # todo add another pod comparison for sha hash
          if pod["status"]["containerStatuses"]?
            found = pod["status"]["containerStatuses"].as_a.any? do |container_status|
              Log.debug { "container_status imageid: #{container_status["imageID"]}" }
              Log.debug { "pods_by_digest_and_nodes digest: #{digest}" }
              match_found = container_status["imageID"].as_s.includes?("#{digest}")
              Log.debug { "container_status match_found: #{match_found}" }
              match_found
            end
            Log.debug { "found pod: #{pod}" }
            pod_name = pod.dig?("metadata", "name")
            Log.debug { "found PodName: #{pod_name}" }
            if found && pod.dig?("spec", "nodeName") == "#{node_name}"
              Log.debug { "found pod and node: #{pod} #{node_name}" }
              true
            else
              Log.debug { "spec node_name: No Match: #{node_name}" }
              false
            end
          else
            Log.info { "no containerstatuses" }
            false
          end
        end
      }.flatten
      if digest_pods.empty?
        Log.info { "match not found for digest: #{digest}" }
        [EMPTY_JSON]
      else
        digest_pods
      end
    end
  end
end
