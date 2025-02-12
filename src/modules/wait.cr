# RLTODO: scope some main log messages as debug anyway

module KubectlClient
  # Using sleep() to wait for terminating resources is unreliable.
  #
  # 1. Resources still in terminating state can interfere with test runs.
  #    and result in failures of the next test (or spec test).
  #
  # 2. Helm uninstall wait option and kubectl delete wait options,
  #    do not wait for child resources to be fully deleted.
  #
  # 3. The output from kubectl json does not clearly indicate when a resource is in a terminating state.
  #    To wait for uninstall, we can use the app.kubernetes.io/name label,
  #    to lookup resources belonging to a CNF to wait for uninstall.
  #    We only use this helper in the spec tests, so we use the "kubectl get" output to keep things simple.
  #
  module Wait
    @logger : ::Log = Log.for("wait")

    def self.wait_for_terminations(namespace : String? = nil, wait_count : Int32 = 30) : Bool
      logger = @logger.for("wait_for_terminations")
      cmd = "kubectl get all"
      # Check all namespaces by default
      cmd = namespace ? "#{cmd} -n #{namespace}" : "#{cmd} -A"

      # By default assume there is a resource still terminating.
      found_terminating = true
      second_count = 0
      while (found_terminating && second_count < wait_count)
        ShellCMD.raise_exc_on_error &.result = ShellCMD.run(cmd, logger)
        if result[:output].match(/([\s+]Terminating)/)
          found_terminating = true
          second_count = second_count + 1
          sleep(1)
        else
          found_terminating = false
          return true
        end

        if second_count % RESOURCE_WAIT_LOG_INTERVAL == 0
          logger.info { "Waiting until resources are terminated, seconds elapsed: #{second_count}" }
        end
      end
      return false
    end

    def self.wait_for_condition(kind : String, resource_name : String, condition : String, namespace : String? = nil)
      logger = @logger.for("wait_for_condition")
      cmd = "kubectl wait #{kind}/#{resource_name} --for=#{condition}"
      cmd = "#{cmd} -n #{namespace}" if namespace

      ShellCMD.raise_exc_on_error &.ShellCMD.run(cmd, logger)
    end

    private def self.resource_ready?(kind : String, resource_name : String, namespace : String? = nil) : Bool
      logger = @logger.for("resource_ready?")
      logger.debug { "Checking if resource #{kind}/#{resource_name} is ready" }

      ready = false
      case kind.downcase
      when "pod"
        return KubectlClient::Get.pod_ready?(resource_name, namespace: namespace)
      else
        replicas = KubectlClient::Get.replica_count(kind, resource_name, namespace)
        ready = replicas[:current] == replicas[:desired]
        if replicas[:desired] == 0 && replicas[:unavailable] >= 1
          ready = false
        end
        if replicas[:current] == -1 || replicas[:desired] == -1
          ready = false
        end
      end

      ready
    end

    def self.wait_for_resource_key_value(
      kind : String,
      resource_name : String,
      dig_params : Tuple,
      value : (String?) = nil,
      wait_count : Int32 = 180,
      namespace : String = "default",
    ) : Bool
      logger = @logger.for("wait_for_resource_key_value")
      logger.info { "Waiting for resource #{kind}/#{resource_name} to have #{dig_params.join(".")} = #{value.as_s}" }

      # Check if resource is installed / ready to use
      case kind.downcase
      when "pod", "replicaset", "deployment", "statefulset", "daemonset"
        is_ready = resource_wait_for_install(kind, resource_name, wait_count, namespace)
      else
        is_ready = resource_desired_is_available?(kind, resource_name, namespace)
      end

      # Check if key-value condition is met
      resource = KubectlClient::Get.resource(kind, resource_name, namespace)
      is_key_ready = false
      if is_ready
        is_key_ready = wait_for_key_value(resource, dig_params, value, wait_count)
      else
        is_key_ready = false
      end

      is_key_ready
    end

    def self.resource_wait_for_install(
      kind : String,
      resource_name : String,
      wait_count : Int32 = 180,
      namespace : String = "default",
    ) : Bool
      logger = @logger.for("resource_wait_for_install")
      logger.info { "Waiting for resource #{kind}/#{resource_name} to install" }

      second_count = 0
      is_ready = resource_ready?(kind, namespace, resource_name)
      until is_ready || second_count > wait_count
        if second_count % RESOURCE_WAIT_LOG_INTERVAL == 0
          logger.info { "seconds elapsed while waiting: #{second_count}" }
        end

        sleep 1
        is_ready = resource_ready?(kind, namespace, resource_name)
        second_count += 1
      end

      if is_ready
        logger.info { "#{kind}/#{resource_name} is ready" }
      else
        logger.warn { "#{kind}/#{resource_name} is not ready and #{wait_count}s elapsed" }
      end

      is_ready
    end

    # TODO add parameter and functionality that checks for individual pods to be successfully terminated
    def self.resource_wait_for_uninstall(
      kind : String,
      resource_name : String,
      wait_count : Int32 = 180,
      namespace : String? = "default"
    ) : Bool
      logger = @logger.for("resource_wait_for_uninstall")
      logger.info { "Waiting for resource #{kind}/#{resource_name} to uninstall" }

      second_count = 0
      resource_uninstalled = KubectlClient::Get.resource(kind, resource_name, namespace)
      until resource_uninstalled != EMPTY_JSON || second_count > wait_count
        if second_count % RESOURCE_WAIT_LOG_INTERVAL == 0
          logger.info { "seconds elapsed while waiting: #{second_count}" }
        end

        sleep 1
        resource_uninstalled = KubectlClient::Get.resource(kind, resource_name, namespace)
        second_count += 1
      end

      if resource_uninstalled == EMPTY_JSON
        logger.info { "#{kind}/#{resource_name} was uninstalled" }
        return true
      else
        logger.warn { "#{kind}/#{resource_name} is still present" }
        return false
      end
    end

    # RLTODO: im here
    def self.wait_for_key_value(resource,
                                dig_params : Tuple,
                                value : (String?) = nil,
                                wait_count : Int32 = 15)
      Log.info { "wait_for_key_value: params, value: #{dig_params}, #{value}" }
      second_count = 0
      key_created = false
      value_matched = false
      until (key_created && value_matched) || second_count > wait_count.to_i
        sleep 3
        namespace = resource.dig?("metadata", "namespace")
        if namespace
          resource = KubectlClient::Get.resource(resource["kind"].as_s, resource.dig("metadata", "name").as_s)
        else
          resource = KubectlClient::Get.resource(resource["kind"].as_s, resource.dig("metadata", "name").as_s, namespace)
        end

        Log.info { "resource.dig?(*dig_params): #{value}, #{resource.dig?(*dig_params)}" }
        if resource.dig?(*dig_params)
          key_created = true
          Log.info { "value == {resource.dig(*dig_params)}: #{value}, #{resource.dig(*dig_params)}" }
          if value == nil
            value_matched = true
          elsif value == "#{resource.dig(*dig_params)}"
            Log.info { "Value matched: true" }
            value_matched = true
          end
        end
        Log.info { "second count: #{second_count}" }
        Log.debug { "resource: params: #{resource}, #{dig_params}" }
        second_count = second_count + 1
      end
      key_created && value_matched
    end

    # TODO make dockercluser reference generic
    def self.wait_for_install_by_apply(manifest_file, wait_count = 180)
      Log.info { "wait_for_install_by_apply" }
      second_count = 0
      apply_result = KubectlClient::Apply.file(manifest_file)
      apply_resp = apply_result[:output]

      until (apply_resp =~ /cluster.cluster.x-k8s.io\/capi-quickstart unchanged/) != nil && (apply_resp =~ /dockercluster.infrastructure.cluster.x-k8s.io\/capi-quickstart unchanged/) != nil && (apply_resp =~ /kubeadmcontrolplane.controlplane.cluster.x-k8s.io\/capi-quickstart-control-plane unchanged/) != nil && (apply_resp =~ /dockermachinetemplate.infrastructure.cluster.x-k8s.io\/capi-quickstart-control-plane unchanged/) != nil && (apply_resp =~ /dockermachinetemplate.infrastructure.cluster.x-k8s.io\/capi-quickstart-md-0 unchanged/) != nil && (apply_resp =~ /kubeadmconfigtemplate.bootstrap.cluster.x-k8s.io\/capi-quickstart-md-0 unchanged/) != nil && (apply_resp =~ /machinedeployment.cluster.x-k8s.io\/capi-quickstart-md-0 unchanged/) != nil || second_count > wait_count.to_i
        Log.info { "second_count = #{second_count}" }
        sleep 1
        apply_result = KubectlClient::Apply.file(manifest_file)
        apply_resp = apply_result[:output]
        second_count = second_count + 1
      end
    end

    def self.wait_for_resource_availability(kind : String,
                                            resource_name,
                                            namespace = "default",
                                            wait_count : Int32 = 180)
      Log.info { "wait_for_resource_availability kind, name: #{kind} #{resource_name}" }
      second_count = 0
      resource_created = false
      until (resource_created) || second_count > wait_count.to_i
        sleep 3
        resource_created = resource_desired_is_available?(kind, resource_name, namespace)
        second_count = second_count + 1
      end
      resource_created
    end

    def self.resource_desired_is_available?(kind : String, resource_name, namespace = "default")
      cmd = "kubectl get #{kind} #{resource_name} -o=yaml"
      if namespace
        cmd = "#{cmd} -n #{namespace}"
      end
      result = ShellCMD.run(cmd, "resource_desired_is_available?")
      resp = result[:output]

      replicas_applicable = false
      case kind.downcase
      when "deployment", "statefulset", "replicaset"
        # Check if the desired replicas is equal to the ready replicas.
        # Return true if yes.
        describe = Totem.from_yaml(resp)
        Log.info { "desired_is_available describe: #{describe.inspect}" }
        desired_replicas = describe.get("status").as_h["replicas"].as_i
        Log.info { "desired_is_available desired_replicas: #{desired_replicas}" }
        ready_replicas = describe.get("status").as_h["readyReplicas"]?
        unless ready_replicas.nil?
          ready_replicas = ready_replicas.as_i
        else
          ready_replicas = 0
        end
        Log.info { "desired_is_available ready_replicas: #{ready_replicas}" }
        return desired_replicas == ready_replicas
      when "pod"
        # Check if the pod status is ready.
        # Return true if yes.
        pod_info = Totem.from_yaml(resp)
        pod_status_conditions = pod_info["status"]["conditions"]
        ready_condition = pod_status_conditions.as_a.find do |condition_info|
          condition_info["type"].as_s? == "Ready" && condition_info["status"].as_s? == "True"
        end

        if ready_condition
          return true
        end
        return false
      else
        # If not any of the above resources,
        # then assume resource is available.
        return true
      end
    end
  end
end
