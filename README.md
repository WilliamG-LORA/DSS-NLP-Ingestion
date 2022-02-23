# NLP Data Ingestion
Contains the ingestion part of the NLP data pipeline (Part of Grapevine)

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