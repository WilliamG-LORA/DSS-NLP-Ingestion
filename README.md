# NLP Data Ingestion
Contains the ingestion part of the NLP data pipeline (Part of Grapevine)

## Overview
This ingestion system consist of 5 main parts:
- **1. Redis Server**
    - The redis server servered as our message broker that allows different pods to communicate with each other. We will hold the workqueue for the scrappering job and the duplicated records in the redis server.
- **2. Workqueue Initializer**
    - After the redis server is initialized, the workqueue initializer will initialize the the workqueue.
- **3. Worker (Lurker)**
    - The worker will start pull the job item from the workqueue after the workqueue initializer is completed. Each job item contains the lurker type and the ticker. The lurker will then mutate into the lurker type and do the scrappering. After the job is finished, the worker will start pulling another job until the workqueus is empty.
- **4. Workqueue Garbage Collector**
    - There might be a chance that the lurker is crashed due to some reason, and the job item will stay at the processing queue even after the job lease is expired. A workqueue garbage collector will move the ununattended job item back to the main queue every minute.
- **5. Duplication Checking**
    - There might be a possibility that a article is searched by multiple workers and we need to keep track of the scrape history within a time span. The duplication checking list will be held inside the redis server. The worker will check the duplication using the url of the scrapers. 

## Current available lurker
### English
- Reddit (US Stock)
- News Filter (US Stock)
    - Analyst Ratings
    - Bloomberg
    - Reuters
    - CNBC
    - WSJ
    - Barrons
    - PR Newswire
    - Globe Newswire
    - BusinessWire
    - AccessWire
    - SeekingAlpha
    - Bezinga
    - S&P Global
    - Earnings Call Transcripts
    - ClinicalTrials.gov
    - SAM.gov
    - SEC Filings
    - SEC Press Releases
    - FCC Filings
    - Patent Database
    - Patent Trial & Appeal Board
    - Department of Defense
    - FDA Drug Approvals
    - Economic Indicators

### Chinese (Traditional)
- AAStock (HK Stock)
- Etnet (HK Stock)

### Chinese (Simplified)
- Eastmoney (US Stock/HK Stock/A Stock)

## Usage

**Requirements**
- [**Docker**](https://www.docker.com/get-started): Used to build images and push to cloud container registries.
- [**Minikube**](https://minikube.sigs.k8s.io/docs/start/): Used to run a local Kubernetes cluster. It requires Kubectl to interact with it. This is only for deving.
- [**Kubectl**](https://kubernetes.io/releases/download/): Used to interact with a Kubernetes cluster. It is a standalone CLI tool.
- [**Skaffold**](https://skaffold.dev/docs/install/): Used to develop and deploy Kubernetes resources. This uses Docker and Kubectl.

**Set-up**
1. Install requirements

2. Start minikube (local k8s cluster).
```
$ minikube start
```

3. Point docker daemon to minikube's cache. This let's you deploy locally without pushing images to cloud first.
```
$ eval `minikube docker-env`
```

4. Prepare directory for persistent volumes for redis pod
```
$ minikube ssh
```

5. Create /mnt/data/ directory in minikube ssh
```
docker@minikube:~$ sudo mkdir /mnt/data/
```

**Dev loop**  
Just do:
```
$ skaffold dev
```
NOTE: This command needs to be run from the root directory (i.e. DSS-NLP-Ingestion/).

**Deploy** 
Preparation: 
Make sure you have added the cluster configuration in the kubeconfig(~/.kube/config)

1. Change the context
```
$ kubectl config set current-context <your-context>
```

2. Deploy the cluster by:
```
$ skaffold run
```

**Settings**
The config files are located inside the "deploy" folder, which consist of multiple config files

```
deploy
├── configmaps
│   └── configs.yaml            # config for pods
├── jobs
│   ├── garbage_collector.yaml  # kubeconfig 
│   ├── init-workqueue.yaml     # kubeconfig 
│   └── worker.yaml             # kubeconfig 
├── microservices
│   ├── redis-local.yaml        # Redis Local Config
│   ├── redis-vpc.yaml          # Redis Deployment Config
│   └── redis.yaml              # Redis Base Config
└── secrets
    └── db-creds.yaml           # db-creds in base64 format
```
