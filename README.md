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
- **5. (WIP) Duplication Checking**
    - There might be a possibility that a article is searched by multiple workers and we need to keep track of the scrape history within a time span. The duplication checking list will be held inside the redis server. The worker will check the duplication using the url of the scrapers. 

## Current available lurker
### English
- Reddit (US Stock)
- News Filters (US Stock)

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

**Dev loop**  
Just do:
```
$ skaffold dev
```
NOTE: This command needs to be run from the root directory (i.e. DSS-NLP-Ingestion/).

**Settings**
