require "totem"
require "colorize"
require "docker_client"
require "./src/modules/*"
require "./src/utils/utils.cr"
require "./src/utils/system_information.cr"

module KubectlClient
  Log = ::Log.for("k8s-client")

  alias CMDResult = NamedTuple(status: Process::Status, output: String, error: String)
  alias BackgroundCMDResult = NamedTuple(process: Process, output: String, error: String)

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
      if !force_output
        logger.trace { "output: #{output}" }
      else
        logger.info { "output: #{output}" }
      end

      # Don't have to output log line if stderr is empty
      if stderr.to_s.size > 1
        logger.warn { "stderr: #{stderr}" }
      end

      {status: status, output: output.to_s, error: stderr.to_s}
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
      if !force_output
        logger.trace { "output: #{output}" }
      else
        logger.info { "output: #{output}" }
      end

      # Don't have to output log line if stderr is empty
      if stderr.to_s.size > 1
        logger.warn { "stderr: #{stderr}" }
      end

      {process: process, output: output.to_s, error: stderr.to_s}
    end

    def self.raise_exc_on_error(&)
      result = yield
      # TODO: raise different kind of exceptions based on type of error (network issue, resource does not exits etc.)
      unless result[:status].success?
        raise K8sClientCMDException.new(result[:error])
      end
      result
    end

    def self.parse_get_result(result : CMDResult)
      if result[:status].success? && !result[:output].empty?
        JSON.parse(result[:output])
      else
        EMPTY_JSON
      end
    end

    class K8sClientCMDException < Exception
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
