require "./spec_helper"
require "../kubectl_client.cr"
require "../src/utils/utils.cr"
require "../src/utils/system_information.cr"
require "file_utils"

describe "KubectlClient" do
  # after_all do
  # end

  it "'installation_found?' should show a kubectl client was located",  do
    (KubectlClient.installation_found?(false, true)).should be_true
  end

  it "'#KubectlClient.pods_by_node' should return all pods on a specific node", tags: ["kubectl-nodes"]  do
    pods = KubectlClient::Get.pods_by_nodes(KubectlClient::Get.schedulable_nodes_list)
    (pods).should_not be_nil
    if pods && pods[0] != Nil
      (pods.size).should be > 0
      first_node = pods[0]
      if first_node
        (first_node.dig("kind")).should eq "Pod"
      else 
        true.should be_false
      end
    else
      true.should be_false
    end
  end

  it "'#KubectlClient.pods_by_label' should return all pods on a specific node", tags: ["kubectl-nodes"]  do
    # AirGap.bootstrap_cluster()
    (KubectlClient::Apply.file("./spec/fixtures/manifest.yml")).should be_truthy
    pods = KubectlClient::Get.pods_by_nodes(KubectlClient::Get.schedulable_nodes_list)
    (pods).should_not be_nil
    pods = KubectlClient::Get.pods_by_label(pods, "name", "dockerd")
    (pods).should_not be_nil
    if pods && pods[0]? != Nil
      (pods.size).should be > 0
      first_node = pods[0]
      if first_node
        (first_node.dig("kind")).should eq "Pod"
      else 
        true.should be_false
      end
    else
      true.should be_false
    end
  end

  it "'#KubectlClient.wait_for_resource_key_value' should wait for a resource and key/value combination", tags: ["kubectl-nodes"]  do
    (KubectlClient::Apply.file("./spec/fixtures/coredns_manifest.yml")).should be_truthy
    is_ready = KubectlClient::Get.wait_for_resource_key_value("Deployment", "coredns-coredns", {"spec", "replicas"}, "1")
    # is_ready = KubectlClient::Get.wait_for_resource_key_value("Deployment", "coredns-coredns", {"spec", "replicas"})
    (is_ready).should be_true
  ensure
    `kubectl delete -f ./spec/fixtures/coredns_manifest.yml`
  end


  it "'#KubectlClient.schedulable_nodes_list' should return all schedulable worker nodes", tags: ["kubectl-nodes"]  do
    retry_limit = 50
    retries = 1
    empty_json_any = JSON.parse(%({}))
    nodes = [empty_json_any]
    until (nodes != [empty_json_any]) || retries > retry_limit
      Log.info {"schedulable_node retry: #{retries}"}
      sleep 1.0
      nodes = KubectlClient::Get.schedulable_nodes_list
      retries = retries + 1
    end
    Log.info {"schedulable_node node: #{nodes}"}
    (nodes).should_not be_nil
    if nodes && nodes[0] != Nil
      (nodes.size).should be > 0
      first_node =  nodes[0]
      if first_node
        (first_node.dig("kind")).should eq "Node"
      else 
        true.should be_false
      end
    else
      true.should be_false
    end
  end

  it "'#KubectlClient.resource_map' should a subset of manifest resource json", tags: ["kubectl-nodes"]  do
    retry_limit = 50
    retries = 1
    empty_json_any = JSON.parse(%({}))
    filtered_nodes = [empty_json_any]
    until (filtered_nodes != [empty_json_any]) || retries > retry_limit
      Log.info {"resource_map retry: #{retries}"}
      sleep 1.0
      filtered_nodes = KubectlClient::Get.resource_map(KubectlClient::Get.nodes) do |item, metadata|
        taints = item.dig?("spec", "taints")
        Log.debug {"taints: #{taints}"}
        if (taints && taints.dig?("effect") == "NoSchedule")
          nil
        else
          {:node => item, :name => item.dig?("metadata", "name")}
        end
      end
      retries = retries + 1
    end
    Log.info {"spec: resource_map node: #{filtered_nodes}"}
    (filtered_nodes).should_not be_nil
    if filtered_nodes
      (filtered_nodes.size).should be > 0
    else
      true.should be_false
    end
  end

  it "'Kubectl::Get.wait_for_install' should wait for a cnf to be installed", tags: ["kubectl-install"]  do
    (KubectlClient::Apply.file("./spec/fixtures/coredns_manifest.yml")).should be_truthy

    KubectlClient::Get.wait_for_install("coredns-coredns")
    current_replicas = `kubectl get deployments coredns-coredns -o=jsonpath='{.status.readyReplicas}'`
    (current_replicas.to_i > 0).should be_true
  end

  it "'Kubectl::Get.resource_wait_for_uninstall' should wait for a cnf to be installed", tags: ["kubectl-install"]  do
    (KubectlClient::Apply.file("./spec/fixtures/wordpress_manifest.yml")).should be_truthy

    KubectlClient::Delete.file("./spec/fixtures/wordpress_manifest.yml")
    resp = KubectlClient::Get.resource_wait_for_uninstall("deployment", "wordpress")
    (resp).should be_true
  end

  it "'#KubectlClient.get_nodes' should return the information about a node in json", tags: ["kubectl-nodes"]  do
    json = KubectlClient::Get.nodes
    (json["items"].size).should be > 0
  end

  it "'#KubectlClient.container_runtime' should return the information about the container runtime", tags: ["kubectl-runtime"]  do
    resp = KubectlClient::Get.container_runtime
    (resp.match(KubectlClient::OCI_RUNTIME_REGEX)).should_not be_nil
  end

  it "'#KubectlClient.container_runtimes' should return all container runtimes", tags: ["kubectl-runtime"]  do
    resp = KubectlClient::Get.container_runtimes
    (resp[0].match(KubectlClient::OCI_RUNTIME_REGEX)).should_not be_nil
  end

  it "'#KubectlClient.schedulable_nodes' should return all schedulable worker nodes", tags: ["kubectl-nodes"]  do
    retry_limit = 50
    retries = 1
    nodes = nil
    until (nodes && nodes.size > 0 && !nodes[0].empty?) || retries > retry_limit
      Log.info {"schedulable_node retry: #{retries}"}
      sleep 1.0
      nodes = KubectlClient::Get.schedulable_nodes
      retries = retries + 1
    end
    Log.info {"schedulable_node node: #{nodes}"}
    # resp = KubectlClient::Get.schedulable_nodes
    (nodes).should_not be_nil
    if nodes 
      (nodes.size).should be > 0
      (nodes[0]).should_not be_nil
      (nodes[0]).should_not be_empty
    end
  end

  it "'#KubectlClient.containers' should return all containers defined in a deployment", tags: ["kubectl-deployment"]  do
    # Log.debug {`./cnf-testsuite cnf_setup cnf-config=./sample-cnfs/k8s-sidecar-container-pattern/cnf-testsuite.yml wait_count=0`}
    KubectlClient::Apply.file("./spec/fixtures/sidecar_manifest.yml")
    resp = KubectlClient::Get.deployment_containers("nginx-webapp")
    (resp.size).should be > 0
  ensure
    # Log.debug  {`./cnf-testsuite cnf_cleanup cnf-config=./sample-cnfs/k8s-sidecar-container-pattern/cnf-testsuite.yml deploy_with_chart=false` }
    KubectlClient::Delete.file("./spec/fixtures/sidecar_manifest.yml")
  end

  it "'#KubectlClient.pod_exists?' should true if a pod exists", tags: ["kubectl-status"]  do
    # Log.debug {`./cnf-testsuite cnf_setup cnf-config=./sample-cnfs/sample-generic-cnf/cnf-testsuite.yml` }
    KubectlClient::Apply.file("./spec/fixtures/coredns_manifest.yml")
    resp = KubectlClient::Get.pod_exists?("coredns")
    (resp).should be_true 
  ensure
    # Log.debug {`./cnf-testsuite cnf_cleanup cnf-config=./sample-cnfs/sample-generic-cnf/cnf-testsuite.yml` }
    KubectlClient::Delete.file("./spec/fixtures/coredns_manifest.yml")
  end
 
  it "'#KubectlClient.pod_status' should return a status of false if the pod is not installed (failed to install) and other pods exist", tags: ["kubectl-status"]  do
    # cnf="./sample-cnfs/sample-coredns-cnf"
    # Log.info {`./cnf-testsuite cnf_setup cnf-path=#{cnf}`}
    # Log.info {`./cnf-testsuite uninstall_dockerd`}
    KubectlClient::Apply.file("./spec/fixtures/coredns_manifest.yml")
    KubectlClient::Apply.file("./spec/fixtures/manifest.yml")
    KubectlClient::Delete.file("./spec/fixtures/manifest.yml")

    resp = KubectlClient::Get.pod_status(pod_name_prefix: "dockerd").split(",")[2] # true/false
    Log.info { resp }
    (resp && !resp.empty? && resp == "true").should be_false
  ensure
    # Log.info {`./cnf-testsuite cnf_cleanup cnf-path=#{cnf}`}
    KubectlClient::Delete.file("./spec/fixtures/coredns_manifest.yml")
  end

  it "'#KubectlClient.pod_status' should return a status of true if the pod is installed and other pods exist", tags: ["kubectl-status"]  do
    cnf="./sample-cnfs/sample-coredns-cnf"
    KubectlClient::Apply.file("./spec/fixtures/coredns_manifest.yml")
    KubectlClient::Apply.file("./spec/fixtures/dockerd_manifest.yml")
    KubectlClient::Get.resource_wait_for_install("Pod", "dockerd")

    resp = KubectlClient::Get.pod_status(pod_name_prefix: "dockerd").split(",")[2] # true/false
    Log.info { resp }
    (resp && !resp.empty? && resp == "true").should be_true
  ensure
    KubectlClient::Delete.file("./spec/fixtures/coredns_manifest.yml")
    KubectlClient::Delete.file("./spec/fixtures/dockerd_manifest.yml")
  end
end

