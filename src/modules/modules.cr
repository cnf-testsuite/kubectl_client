module KubectlClient
  module Utils
    def self.wait(cmd)
      status = Process.run("kubectl wait #{cmd}",
        shell: true,
        output: output = IO::Memory.new,
        error: stderr = IO::Memory.new)
      Log.info { "KubectlClient.wait output: #{output.to_s}" }
      Log.info { "KubectlClient.wait stderr: #{stderr.to_s}" }
      {status: status, output: output, error: stderr}
    end

    def self.logs(pod_name : String, namespace : String | Nil = nil, options : String | Nil = nil)
      full_cmd = ["kubectl", "logs"]
      full_cmd.push("-n #{namespace}") if namespace
      full_cmd.push(pod_name)
      full_cmd.push(options) if options
      full_cmd = full_cmd.join(" ")
      status = Process.run(full_cmd,
        shell: true,
        output: output = IO::Memory.new,
        error: stderr = IO::Memory.new)
      Log.debug { "KubectlClient.logs output: #{output.to_s}" }
      Log.info { "KubectlClient.logs stderr: #{stderr.to_s}" }
      {status: status, output: output, error: stderr}
    end

    def self.describe(kind, resource_name, namespace : String | Nil = nil, force_output : Bool = false)
      # kubectl describe requiretags block-latest-tag
      cmd = "kubectl describe #{kind} #{resource_name}"
      if namespace
        cmd = "#{cmd} -n #{namespace}"
      end
      ShellCmd.run(cmd, "KubectlClient.describe", force_output: force_output)
    end

    def self.exec(command, namespace : String | Nil = nil, force_output : Bool = false)
      full_cmd = construct_exec_cmd(command, namespace)
      ShellCmd.run(full_cmd, "KubectlClient.exec", force_output)
    end

    def self.exec_bg(command, namespace : String | Nil = nil, force_output : Bool = false)
      full_cmd = construct_exec_cmd(command, namespace)
      ShellCmd.new(full_cmd, "KubectlClient.exec_bg", force_output)
    end

    # Returns a command as a string to be used in exec or exec_bg
    def self.construct_exec_cmd(command, namespace : String | Nil = nil) : String
      full_cmd = ["kubectl", "exec"]
      if namespace
        full_cmd << "-n #{namespace}"
      end
      full_cmd << command
      full_cmd = full_cmd.join(" ")
      return full_cmd
    end

    def self.cp(command)
      cmd = "kubectl cp #{command}"
      ShellCmd.run(cmd, "KubectlClient.cp")
    end
  end

  module Rollout
    # DEPRECATED: Added only for smooth transition from bug/1726 to main branch
    def self.status(resource_name : String, namespace : String | Nil = nil, timeout : String = "30s") : Bool
      Log.info { "Decrecated method. Pass kind in the args KubectlClient::Rollout.status(kind, resource_name, namespace, timeout)" }
      status(kind: "deployment", resource_name: resource_name, namespace: namespace, timeout: timeout)
    end

    # DEPRECATED: Added only for smooth transition from bug/1726 to main branch
    def self.undo(resource_name : String, namespace : String | Nil = nil) : Bool
      Log.info { "Decrecated method. Pass kind in the args KubectlClient::Rollout.undo(kind, resource_name, namespace)" }
      undo(kind: "deployment", resource_name: resource_name, namespace: namespace)
    end

    # DEPRECATED: Added only for smooth transition from bug/1726 to main branch
    def self.resource_status(kind : String, resource_name : String, namespace : String | Nil = nil, timeout : String = "30s") : Bool
      status(kind: kind, resource_name: resource_name, namespace: namespace, timeout: timeout)
    end

    def self.status(kind : String, resource_name : String, namespace : String | Nil = nil, timeout : String = "30s") : Bool
      cmd = "kubectl rollout status #{kind}/#{resource_name} --timeout=#{timeout}"
      if namespace
        cmd = "#{cmd} -n #{namespace}"
      end
      result = ShellCmd.run(cmd, "KubectlClient::Rollout.status")
      Log.debug { "rollout status: #{result[:status].success?}" }
      result[:status].success?
    end

    def self.undo(kind : String, resource_name : String, namespace : String | Nil = nil) : Bool
      cmd = "kubectl rollout undo #{kind}/#{resource_name}"
      if namespace
        cmd = "#{cmd} -n #{namespace}"
      end
      result = ShellCmd.run(cmd, "KubectlClient::Rollout.undo")
      Log.debug { "rollback status: #{result[:status].success?}" }
      result[:status].success?
    end
  end

  module Annotate
    def self.run(cli)
      cmd = "kubectl annotate #{cli}"
      ShellCmd.run(cmd, "KubectlClient::Annotate.run")
    end
  end

  module Create
    class AlreadyExistsError < Exception
    end

    def self.command(cli : String)
      cmd = "kubectl create #{cli}"
      result = ShellCmd.run(cmd, "KubectlClient::Create.command")
      result[:status].success?
    end

    def self.namespace(name : String, kubeconfig : String | Nil = nil)
      cmd = "kubectl create namespace #{name}"
      if kubeconfig
        cmd = "#{cmd} --kubeconfig #{kubeconfig}"
      end
      result = ShellCmd.run(cmd, "KubectlClient::Create.namespace")
      return true if result[:status].success?
      raise AlreadyExistsError.new if result[:error].includes?("AlreadyExists")
      return false
    end
  end

  module Apply
    def self.file(file_name, kubeconfig : String | Nil = nil, namespace : String | Nil = nil)
      cmd = ["kubectl apply"]
      cmd << "--kubeconfig #{kubeconfig}" if kubeconfig
      cmd << "-n #{namespace}" if namespace
      cmd << "-f #{file_name}"
      cmd = cmd.join(" ")
      ShellCmd.run(cmd, "KubectlClient::Apply.file")
    end

    def self.validate(file_name) : Bool
      # this hits the server btw (so you need a valid K8s cluster)
      cmd = "kubectl apply --validate=true --dry-run=client -f #{file_name}"
      result = ShellCmd.run(cmd, "KubectlClient::Apply.validate")
      result[:status].success?
    end

    def self.namespace(name : String, kubeconfig : String | Nil = nil)
      cmd = "kubectl create namespace #{name} --dry-run=client -o yaml | kubectl apply -f -"
      if kubeconfig
        cmd = "kubectl create namespace #{name} --kubeconfig #{kubeconfig} --dry-run=client -o yaml | kubectl apply --kubeconfig #{kubeconfig} -f -"
        # cmd = "#{cmd} --kubeconfig #{kubeconfig}"
      end
      result = ShellCmd.run(cmd, "KubectlClient::Apply.namespace")
      result[:status].success?
    end
  end

  module Patch
    def self.spec(kind : String, resource : String, spec_input : String, namespace : String? = nil)
      namespace_opt = ""
      if namespace != nil
        namespace_opt = "-n #{namespace}"
      end
      cmd = "kubectl patch #{kind} #{resource} #{namespace_opt} -p '#{spec_input}'"
      ShellCmd.run(cmd, "KubectlClient::Patch.spec")
    end
  end

  module Scale
    def self.command(cli)
      cmd = "kubectl scale #{cli}"
      ShellCmd.run(cmd, "KubectlClient::Scale.command")
    end
  end

  module Delete
    def self.command(command, labels : Hash(String, String) | Nil = {} of String => String)
      cmd = "kubectl delete #{command}"
      if !labels.empty?
        label_options = labels.map { |key, value| "-l #{key}=#{value}" }.join(" ")
        cmd = "#{cmd} #{label_options}"
      end
      ShellCmd.run(cmd, "KubectlClient::Delete.command")
    end

    def self.file(file_name, namespace : String | Nil = nil, wait : Bool = false)
      cmd = "kubectl delete -f #{file_name}"
      if namespace
        cmd = "#{cmd} -n #{namespace}"
      end
      if wait == true
        cmd = "#{cmd} --wait=true"
      end
      ShellCmd.run(cmd, "KubectlClient::Delete.file")
    end
  end

  module Replace
    def self.command(cli : String)
      cmd = "kubectl replace #{cli}"
      ShellCmd.run(cmd, "KubectlClient::Replace.command")
    end
  end

  module Utils
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
    def self.wait_for_terminations(namespace : String | Nil = nil, wait_count : Int32 = 30)
      cmd = "kubectl get all"
      if namespace != nil
        cmd = "#{cmd} -n #{namespace}"
      else
        cmd = "#{cmd} -A" # Check all namespaces by default
      end

      # By default assume there is a resource still terminating.
      found_terminating = true
      second_count = 0
      while (found_terminating == true && second_count < wait_count)
        result = ShellCmd.run(cmd, "kubectl_get_resources", force_output: true)
        if result[:output].match(/([\s+]Terminating)/)
          found_terminating = true
          second_count = second_count + 1
          sleep(1)
        else
          found_terminating = false
        end
        Log.info { "found_terminating = #{found_terminating}; second_count = #{second_count}" }
      end
    end
  end

  module Cordon
    def self.command(command)
      cmd = "kubectl cordon #{command}"
      ShellCmd.run(cmd, "KubectlClient::Cordon.command")
    end
  end

  module Uncordon
    def self.command(command)
      cmd = "kubectl uncordon #{command}"
      ShellCmd.run(cmd, "KubectlClient::Uncordon.command")
    end
  end

  module Set
    def self.image(
      resource_kind : String,
      resource_name : String,
      container_name : String,
      image_name : String,
      version_tag : String | Nil = nil,
      namespace : String | Nil = nil
    ) : Bool
      # use --record when setting image to have history
      # TODO check if image exists in repo? DockerClient::Get.image and image_by_tags
      cmd = ""
      if version_tag
        cmd = "kubectl set image #{resource_kind}/#{resource_name} #{container_name}=#{image_name}:#{version_tag} --record"
      else
        cmd = "kubectl set image #{resource_kind}/#{resource_name} #{container_name}=#{image_name} --record"
      end
      if namespace
        cmd = "#{cmd} -n #{namespace}"
      end
      result = ShellCmd.run(cmd, "KubectlClient::Set.image")
      result[:status].success?
    end

    # DEPRECATED: Added only for smooth transition from bug/1726 to main branch
    def self.image(
      resource_name : String,
      container_name : JSON::Any,
      image_name : String,
      version_tag : String | Nil = nil,
      namespace : String | Nil = nil
    ) : Bool
      return image(
        resource_kind: "deployment",
        resource_name: resource_name,
        container_name: container_name.as_s,
        image_name: image_name,
        version_tag: version_tag,
        namespace: namespace
      )
    end
  end
end
