require "totem"
require "colorize"
require "docker_client"
require "./src/modules/*"
require "./src/utils/utils.cr"
require "./src/utils/system_information.cr"

module KubectlClient
  Log = ::Log.for("k8s-client")

  alias K8sManifest = JSON::Any
  alias K8sManifestList = Array(JSON::Any)
  alias CMDResult = NamedTuple(status: Process::Status, stdout: String, stderr: String)
  alias BackgroundCMDResult = NamedTuple(process: Process, stdout: String, stderr: String)

  WORKLOAD_RESOURCES = {deployment:      "Deployment",
                        service:         "Service",
                        pod:             "Pod",
                        replicaset:      "ReplicaSet",
                        statefulset:     "StatefulSet",
                        daemonset:       "DaemonSet",
                        service_account: "ServiceAccount"}

  module ShellCMD
    # logger should have method name (any other scopes, if necessary) that is calling attached using .for() method.
    def self.run(cmd, logger : ::Log = Log, force_output = false) : CMDResult
      logger = logger.for("cmd")
      logger.debug { "command: #{cmd}" }
      status = Process.run(
        cmd,
        shell: true,
        output: output = IO::Memory.new,
        error: stderr = IO::Memory.new
      )
      if force_output == false
        logger.debug { "output: #{output.to_s}" }
      else
        logger.info { "output: #{output.to_s}" }
      end

      # Don't have to output log line if stderr is empty
      if stderr.to_s.size > 1
        logger.warn { "stderr: #{stderr.to_s}" }
      end

      CMDResult.new(status: status, output: output.to_s, error: stderr.to_s)
    end

    def self.new(cmd, logger : ::Log = Log, force_output = false) : CMDResult
      logger = logger.for("cmd-background")
      logger.debug { "command: #{cmd}" }
      process = Process.new(
        cmd,
        shell: true,
        output: output = IO::Memory.new,
        error: stderr = IO::Memory.new
      )
      if force_output == false
        logger.debug { "output: #{output.to_s}" }
      else
        logger.info { "output: #{output.to_s}" }
      end

      # Don't have to output log line if stderr is empty
      if stderr.to_s.size > 1
        logger.warn { "stderr: #{stderr.to_s}" }
      end

      BackgroundCMDResult.new(process: process, output: output.to_s, error: stderr.to_s)
    end
  end

  def self.installation_found?(verbose = false, offline_mode = false) : Bool
    kubectl_installation(verbose = false, offline_mode = false).includes?("kubectl found")
  end

  def self.server_version : String
    logger = Log.for("server_version")

    result = ShellCMD.run("kubectl version --output json", logger)
    version = JSON.parse(result[:output])["serverVersion"]["gitVersion"].as_s
    version = version.gsub("v", "")

    logger.info { "K8s server version is: #{version}" }
    version
  end
end
