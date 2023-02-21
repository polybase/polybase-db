# Polybase Helm chart

## Introduction

This chart bootstraps a Polybase DB deployment on a Kubernetes cluster using the Helm package manager.

## Prerequisites

- Kubernetes 1.19+
- Helm 3.2.0+
- PV provisioner support in the underlying infrastructure
- Tendermit files ready (genesis, node_key and priv_key_validator)

The Tenermint files need to be placed in Vault and are picked up from there automatically. 
The INDEX is determined by the number of replicas (nodes). If you want to spin 3 replicas you need to have 9 files in Vault. Index always starts from 0. 

## Values file

The `values.yaml` file contains the configuration for the Helm chart which is replicated across all environments. 
The environment-specific configuration files (`prenet-vaules.yaml`, `testnet-values.yaml`, `mainnet-values.yaml`) are specifying only the things that are specific to that environment and they override the default `values.yaml` file.
Important things to note:

- image section
    Contains information about the image that will be deployed, such as registry, tag and pull policy.
- peerList
    Contains the list of peers that will be connected. The list needs to be in the following format: 
    `[node_key_id]@[service_dns]:[port]`. Service DNS is determined in the following way:
    `[pod_name].[service_name].[namespace].svc.cluster.local`. The chart automatically deploys a headless service which is used for service discovery.
- resources
    Contains the set requests and limits for the deployment. Might need to be adjusted according to usage.
- persistance
    Contains the information required for data persistance. Important to note here are the `storageClass` (SSD or HDD) and `size` (in GB) options

## Installing the Chart

Connect to the k8s cluster that you wish to install the chart on.

To install the chart with the release name `polybase`:
```
cd polybase/helm
helm install polybase . -f [env_values_file] -n [k8s_namespace]
```

These commands deploy three nodes of Polybase on the Kubernetes cluster in the default configuration.

## Uninstalling the Chart

`helm delete polybase`

The command removes all the Kubernetes components associated with the chart and deletes the release.
