require "totem"
require "colorize"
require "docker_client"
require "./src/modules/get.cr"
require "./src/modules/modules.cr"
require "./src/utils/utils.cr"
require "./src/utils/system_information.cr"

module KubectlClient
  alias K8sManifest = JSON::Any
  alias K8sManifestList = Array(JSON::Any)

  WORKLOAD_RESOURCES = {deployment:      "Deployment",
                        service:         "Service",
                        pod:             "Pod",
                        replicaset:      "ReplicaSet",
                        statefulset:     "StatefulSet",
                        daemonset:       "DaemonSet",
                        service_account: "ServiceAccount"}

  module ShellCmd
    def self.run(cmd, log_prefix, force_output = false)
      Log.info { "#{log_prefix} command: #{cmd}" }
      status = Process.run(
        cmd,
        shell: true,
        output: output = IO::Memory.new,
        error: stderr = IO::Memory.new
      )
      if force_output == false
        Log.debug { "#{log_prefix} output: #{output.to_s}" }
      else
        Log.info { "#{log_prefix} output: #{output.to_s}" }
      end

      # Don't have to output log line if stderr is empty
      if stderr.to_s.size > 1
        Log.info { "#{log_prefix} stderr: #{stderr.to_s}" }
      end
      {status: status, output: output.to_s, error: stderr.to_s}
    end

    def self.new(cmd, log_prefix, force_output = false)
      Log.info { "#{log_prefix} command: #{cmd}" }
      process = Process.new(
        cmd,
        shell: true,
        output: output = IO::Memory.new,
        error: stderr = IO::Memory.new
      )
      if force_output == false
        Log.debug { "#{log_prefix} output: #{output.to_s}" }
      else
        Log.info { "#{log_prefix} output: #{output.to_s}" }
      end

      # Don't have to output log line if stderr is empty
      if stderr.to_s.size > 1
        Log.info { "#{log_prefix} stderr: #{stderr.to_s}" }
      end
      {process: process, output: output.to_s, error: stderr.to_s}
    end
  end

  def self.installation_found?(verbose = false, offline_mode = false)
    kubectl_installation(verbose = false, offline_mode = false).includes?("kubectl found")
  end

  def self.server_version
    Log.debug { "KubectlClient.server_version" }
    result = ShellCmd.run("kubectl version --output json", "KubectlClient.server_version", true)
    version = JSON.parse(result[:output])["serverVersion"]["gitVersion"].as_s
    version = version.gsub("v", "")
    Log.info { "KubectlClient.server_version: #{version}" }
    version
  end
end
