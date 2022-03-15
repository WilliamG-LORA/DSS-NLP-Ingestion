#! /bin/sh
echo "Start minikube (local k8s cluster)" && minikube start &&  echo "Point docker daemon to minikube's cache" && eval `minikube docker-env`