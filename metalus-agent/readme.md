

## Local Kubernetes Setup
### Docker
Local builds and testing uses docker to generate the container. It is recommended that
[Docker Desktop](https://www.docker.com/products/docker-desktop/) is used.
### Minikube
Minikube is used for deploying the service to kubernetes locally.
* Install [Minikube](https://minikube.sigs.k8s.io/docs/start/)
* Start Minikube: `minikube -p minikube start --driver=docker`
* Setup the Namespace for the service TODO: Where is the common yaml stuff going to be stored?
* Create the _metalus_ namespace once: `kubectl create namespace metalus`
* Set the default Namespace: `kubectl config set-context --current --namespace=metalus`

## Build and Publish locally
* Make sure that containers are published to docker: `eval $(minikube -p minikube docker-env)`
* The build will attempt to pull _metalus-utils_ from the local directory. Next it will attempt to
pull from the _metalus-utils/target_ directory. It is recommended that it be built locally.
* Build and publish locally: `sbt clean docker:publishLocal`

## Deploy locally
* Run the build steps
* Deploy the container image to minikube: `kubectl apply -f k8s/deploy-local.yml`
* Setup Minikube Load Balancer (must be in a different terminal window than the minkube terminal): `minikube tunnel`
* Setup the Load Balancer: `kubectl expose deployment process- --type=LoadBalancer --name=process-preview-services-lb --port=9000`

## Undeploy locally
Delete the local deployment: `kubectl delete -f k8s/deploy-local.yml`

## Useful Kubernetes Commands
* **Show Pods** - `kubectl get pods`
* **Show Logs** - `kubectl logs <container-name>`
* **Pod Shell Access** - `kubectl exec --stdin --tty <container-name> -- /bin/bash`
