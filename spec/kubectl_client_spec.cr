require "./spec_helper"
require "../kubectl_client.cr"
require "../src/utils/utils.cr"
require "../src/utils/system_information.cr"
require "../src/modules/get.cr"
require "../src/modules/modules.cr"
require "file_utils"

describe "KubectlClient" do
  # after_all do
  # end

  it "'installation_found?' should show a kubectl client was located" do
    (KubectlClient.installation_found?(false, true)).should be_true
  end

  it "'#KubectlClient::Get.resource(\"nodes\")' should return the information about a node in json", tags: ["kubectl-nodes"] do
    json = KubectlClient::Get.resource("nodes")
    (json["items"].size).should be > 0
  end

  it "'#KubectlClient::Get.pods_by_nodes' should return all pods on a specific node", tags: ["kubectl-nodes"] do
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

  it "'#KubectlClient::Get.pods_by_labels' should return only one pod from manifest.yml", tags: ["kubectl-nodes"] do
    (KubectlClient::Apply.file("./spec/fixtures/manifest.yml")).should be_truthy
    pods = KubectlClient::Get.pods_by_nodes(KubectlClient::Get.schedulable_nodes_list)
    (pods).should_not be_nil
    pods = KubectlClient::Get.pods_by_labels(pods, {"name" => "dockerd-test-label"})
    (pods).should_not be_nil
    if pods && pods[0]? != Nil
      (pods.size).should eq 1
      first_node = pods[0]
      if first_node
        (first_node.dig("kind")).should eq "Pod"
      else
        true.should be_false
      end
    else
      true.should be_false
    end
  ensure
    KubectlClient::Delete.file("./spec/fixtures/manifest.yml")
  end

  it "'#KubectlClient::Wait.wait_for_resource_key_value' should wait for a resource and key/value combination", tags: ["kubectl-nodes"] do
    (KubectlClient::Apply.file("./spec/fixtures/coredns_manifest.yml")).should be_truthy
    is_ready = KubectlClient::Wait.wait_for_resource_key_value("deployment", "coredns-coredns", {"spec", "replicas"}, "1")
    (is_ready).should be_true
  ensure
    KubectlClient::Delete.file("./spec/fixtures/coredns_manifest.yml")
  end

  it "'#KubectlClient::Get.schedulable_nodes_list' should return all schedulable worker nodes", tags: ["kubectl-nodes"] do
    retry_limit = 50
    retries = 1
    empty_json_any = KubectlClient::EMPTY_JSON
    nodes = [empty_json_any]
    until (nodes != [empty_json_any]) || retries > retry_limit
      sleep 1.seconds
      nodes = KubectlClient::Get.schedulable_nodes_list
      retries = retries + 1
    end
    (nodes).should_not be_nil
    if nodes && nodes[0] != Nil
      (nodes.size).should be > 0
      first_node = nodes[0]
      if first_node
        (first_node.dig("kind")).should eq "Node"
      else
        true.should be_false
      end
    else
      true.should be_false
    end
  end

  it "'#KubectlClient::Get.resource_map' should extract a subset of manifest resource json", tags: ["kubectl-nodes"] do
    retry_limit = 50
    retries = 1
    empty_json_any = KubectlClient::EMPTY_JSON
    filtered_nodes = [empty_json_any]
    until (filtered_nodes != [empty_json_any]) || retries > retry_limit
      sleep 1.seconds
      filtered_nodes = KubectlClient::Get.resource_map(KubectlClient::Get.resource("nodes")) do |item, metadata|
        taints = item.dig?("spec", "taints")
        if (taints && taints.dig?("effect") == "NoSchedule")
          nil
        else
          item.dig("metadata", "name").as_s
        end
      end
      retries = retries + 1
    end
    (filtered_nodes).should_not be_nil
    if filtered_nodes
      (filtered_nodes.size).should be > 0
      (filtered_nodes[0]).should be_a String
    else
      true.should be_false
    end
  end

  it "'Kubectl::Wait.resource_wait_for_install' should wait for a cnf to be installed", tags: ["kubectl-install"] do
    (KubectlClient::Apply.file("./spec/fixtures/coredns_manifest.yml")).should be_truthy

    KubectlClient::Wait.resource_wait_for_install("deployment", "coredns-coredns")
    current_replicas = `kubectl get deployments coredns-coredns -o=jsonpath='{.status.readyReplicas}'`
    (current_replicas.to_i > 0).should be_true
  end

  it "'Kubectl::Wait.resource_wait_for_uninstall' should wait for a cnf to be installed", tags: ["kubectl-install"] do
    (KubectlClient::Apply.file("./spec/fixtures/wordpress_manifest.yml")).should be_truthy

    KubectlClient::Delete.file("./spec/fixtures/wordpress_manifest.yml")
    resp = KubectlClient::Wait.resource_wait_for_uninstall("deployment", "wordpress")
    (resp).should be_true
  end

  it "'#KubectlClient.container_runtimes' should return all container runtimes", tags: ["kubectl-runtime"] do
    resp = KubectlClient::Get.container_runtimes
    (resp[0].match(KubectlClient::OCI_RUNTIME_REGEX)).should_not be_nil
  end

  it "'#KubectlClient::Get.resource_containers' should return all containers defined in a deployment", tags: ["kubectl-deployment"] do
    KubectlClient::Apply.file("./spec/fixtures/sidecar_manifest.yml")
    resp = KubectlClient::Get.resource_containers("deployment", "nginx-webapp")
    (resp.size).should be > 0
  ensure
    KubectlClient::Delete.file("./spec/fixtures/sidecar_manifest.yml")
  end

  it "'#KubectlClient::Get.pod_ready?' should return 'true' if the pod has all containers ready", tags: ["kubectl-status"] do
    KubectlClient::Apply.file("./spec/fixtures/multi_container_pod_manifest.yml")
    KubectlClient::Wait.resource_wait_for_install("pod", "multi-container-pod")

    # Also test if pod prefix matching works as intended
    resp = KubectlClient::Get.pod_ready?(pod_name_prefix: "multi-cont")
    resp.should be_true
  ensure
    KubectlClient::Delete.file("./spec/fixtures/multi_container_pod_manifest.yml")
  end

  it "'#KubectlClient::Get.pod_ready?' should return 'false' if pod has containers that are not ready", tags: ["kubectl-status"] do
    KubectlClient::Apply.file("./spec/fixtures/failed_multi_container_pod_manifest.yml")

    sleep 3.seconds
    # Also test if pod prefix matching works as intended
    resp = KubectlClient::Get.pod_ready?(pod_name_prefix: "test")
    resp.should be_false
  ensure
    KubectlClient::Delete.file("./spec/fixtures/failed_multi_container_pod_manifest.yml")
  end

  it "'#KubectlClient::Get.pods_by_resource_labels' should return pods for a deployment ", tags: ["kubectl-status"] do
    KubectlClient::Apply.file("./spec/fixtures/coredns_manifest.yml")
    KubectlClient::Wait.resource_wait_for_install("pod", "coredns")

    resource = KubectlClient::Get.resource("deployment", "coredns-coredns")
    resp = KubectlClient::Get.pods_by_resource_labels(resource)
    (resp && !resp.empty?).should be_true
  ensure
    KubectlClient::Delete.file("./spec/fixtures/coredns_manifest.yml")
  end
end
