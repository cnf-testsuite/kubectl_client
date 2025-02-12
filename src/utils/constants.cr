module KubectlClient
  DEFAULT_LOCAL_BINARY_PATH  = "tools/git/linux-amd64/docker"
  BASE_CONFIG                = "./config.yml"
  RESOURCE_WAIT_LOG_INTERVAL = 10
  # https://www.capitalone.com/tech/cloud/container-runtime/
  OCI_RUNTIME_REGEX = /containerd|docker|podman|runc|railcar|crun|rkt|gviso|nabla|runv|clearcontainers|kata|cri-o/i
  EMPTY_JSON        = JSON.parse(%({}))
end
