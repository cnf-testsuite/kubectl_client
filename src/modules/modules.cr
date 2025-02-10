module KubectlClient
  module Rollout
    @logger : ::Log = Log.for("rollout")

    def self.status(kind : String, resource_name : String, namespace : String? = nil, timeout : String = "30s")
      logger = @logger.for("status")
      cmd = "kubectl rollout status #{kind}/#{resource_name} --timeout=#{timeout}"
      cmd = "#{cmd} -n #{namespace}" if namespace

      ShellCMD.raise_exc_on_error &.ShellCMD.run(cmd, logger)
    end

    def self.undo(kind : String, resource_name : String, namespace : String? = nil)
      logger = @logger.for("undo")
      cmd = "kubectl rollout undo #{kind}/#{resource_name}"
      cmd = "#{cmd} -n #{namespace}" if namespace

      ShellCMD.raise_exc_on_error &.ShellCMD.run(cmd, logger)
    end
  end

  module Apply
    @logger : ::Log = Log.for("apply")

    def self.resource(kind : String, resource_name : String, namespace : String? = nil, values : String? = nil)
      logger = @logger.for("resource")
      cmd = "kubectl create #{kind}/#{resource_name}"
      cmd = "#{cmd} -n #{namespace}" if namespace
      cmd = "#{cmd} #{values}" if values

      ShellCMD.raise_exc_on_error &.ShellCMD.run(cmd, logger)
    end

    def self.file(file_name : String?, namespace : String? = nil)
      logger = @logger.for("file")
      cmd = "kubectl apply -f #{file_name}"
      cmd = "#{cmd} -n #{namespace}" if namespace

      ShellCMD.raise_exc_on_error &.ShellCMD.run(cmd, logger)
    end

    def self.namespace(name : String, kubeconfig : String? = nil)
      logger = @logger.for("namespace")
      cmd = "kubectl create namespace #{name}"

      ShellCMD.raise_exc_on_error &.ShellCMD.run(cmd, logger)
    end
  end

  module Delete
    @logger : ::Log = Log.for("delete")

    def self.resource(kind : String, resource_name : String, namespace : String? = nil,
                      labels : Hash(String, String)? = {} of String => String)
      logger = @logger.for("resource")
      cmd = "kubectl delete #{kind}/#{resource_name}"
      cmd = "#{cmd} -n #{namespace}" if namespace
      unless labels.empty?
        label_options = labels.map { |key, value| "-l #{key}=#{value}" }.join(" ")
        cmd = "#{cmd} #{label_options}"
      end

      ShellCMD.raise_exc_on_error &.ShellCMD.run(cmd, logger)
    end

    def self.file(file_name : String?, namespace : String? = nil, wait : Bool = false)
      logger = @logger.for("file")
      cmd = "kubectl delete -f #{file_name}"
      cmd = "#{cmd} -n #{namespace}" if namespace
      if wait
        cmd = "#{cmd} --wait=true"
        logger.info { "Waiting until requested resource is deleted" }
      end

      ShellCMD.raise_exc_on_error &.ShellCMD.run(cmd, logger)
    end
  end

  module Utils
    @logger : ::Log = Log.for("utils")

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

    def self.logs(pod_name : String, container_name : String? = nil, namespace : String? = nil, options : String? = nil)
      logger = @logger.for("logs")
      cmd = "kubectl logs"
      cmd = "#{cmd} -n #{namespace}" if namespace
      cmd = "#{cmd} -c #{container_name}" if container_name
      cmd = "#{cmd} #{options}" if options

      ShellCMD.raise_exc_on_error &.ShellCMD.run(cmd, logger)
    end

    def self.exec(pod_name : String, command : String, container_name : String? = nil, namespace : String? = nil)
      logger = @logger.for("exec")
      cmd = "kubectl exec #{pod_name}"
      cmd = "#{cmd} -n #{namespace}" if namespace
      cmd = "#{cmd} -c #{container_name}" if container_name
      cmd = "-- #{command}"

      ShellCMD.raise_exc_on_error &.ShellCMD.run(cmd, logger)
    end

    def self.exec_bg(pod_name : String, command : String, container_name : String? = nil, namespace : String? = nil)
      logger = @logger.for("exec_bg")
      cmd = "kubectl exec #{pod_name}"
      cmd = "#{cmd} -n #{namespace}" if namespace
      cmd = "#{cmd} -c #{container_name}" if container_name
      cmd = "-- #{command}"

      ShellCMD.raise_exc_on_error &.ShellCMD.new(cmd, logger)
    end

    def self.copy_to_pod(pod_name : String, source : String, destination : String,
                         container_name : String? = nil, namespace : String? = nil)
      logger = @logger.for("copy_to_pod")
      cmd = "kubectl cp"
      cmd = "#{cmd} -n #{namespace}" if namespace
      cmd = "#{cmd} #{source} #{pod_name}:#{destination}"
      cmd = "#{cmd} -c #{container_name}" if container_name

      ShellCMD.raise_exc_on_error &.ShellCMD.run(cmd, logger)
    end

    def self.copy_from_pod(pod_name : String, source : String, destination : String,
                           container_name : String? = nil, namespace : String? = nil)
      logger = @logger.for("copy_from_pod")
      cmd = "kubectl cp"
      cmd = "#{cmd} -n #{namespace}" if namespace
      cmd = "#{cmd} #{pod_name}:#{source} #{destination}"
      cmd = "#{cmd} -c #{container_name}" if container_name

      ShellCMD.raise_exc_on_error &.ShellCMD.run(cmd, logger)
    end

    def self.scale(kind : String, resource_name : String, replicas : Int32, namespace : String? = nil)
      logger = @logger.for("scale")
      cmd = "kubectl scale #{kind}/#{resource_name} --replicas=#{replicas}"
      cmd = "#{cmd} -n #{namespace}" if namespace

      ShellCMD.raise_exc_on_error &.ShellCMD.run(cmd, logger)
    end

    def self.replace_raw(path : String, file_path : String, extra_flags : String? = nil)
      logger = @logger.for("replace_raw")
      cmd = "kubectl replace --raw '#{path}' -f #{file_path}"
      cmd = "#{cmd} #{extra_flags}" if extra_flags

      ShellCMD.raise_exc_on_error &.ShellCMD.run(cmd, logger)
    end

    def self.annotate(kind : String, resource_name : String, annotatation_str : String, namespace : String? = nil)
      logger = @logger.for("annotate")
      cmd = "kubectl annotate #{kind}/#{resource_name} --overwrite #{annotatation_str}"
      cmd = "#{cmd} -n #{namespace}" if namespace

      ShellCMD.raise_exc_on_error &.ShellCMD.run(cmd, logger)
    end

    def self.label(kind : String, resource_name : String, labels : Array(String), namespace : String? = nil)
      logger = @logger.for("label")
      cmd = "kubectl label --overwrite #{kind}/#{resource_name}"
      cmd = "#{cmd} -n #{namespace}" if namespace

      labels.each do |label|
        cmd = "#{cmd} #{label}"
      end

      ShellCMD.raise_exc_on_error &.ShellCMD.run(cmd, logger)
    end

    def self.cordon(node_name : String)
      logger = @logger.for("cordon")
      cmd = "kubectl cordon #{node_name}"

      ShellCMD.raise_exc_on_error &.ShellCMD.run(cmd, logger)
    end

    def self.uncordon(node_name : String)
      logger = @logger.for("uncordon")
      cmd = "kubectl uncordon #{node_name}"

      ShellCMD.raise_exc_on_error &.ShellCMD.run(cmd, logger)
    end

    def self.set_image(
      resource_kind : String,
      resource_name : String,
      container_name : String,
      image_name : String,
      version_tag : String? = nil,
      namespace : String? = nil
    )
      logger = @logger.for("set_image")

      cmd = version_tag ?
        "kubectl set image #{resource_kind}/#{resource_name}#{container_name}=#{image_name}:#{version_tag} --record" :
        "kubectl set image #{resource_kind}/#{resource_name} #{container_name}=#{image_name} --record"
      cmd = "#{cmd} -n #{namespace}" if namespace

      ShellCMD.raise_exc_on_error &.ShellCMD.run(cmd, logger)
    end
  end
end
