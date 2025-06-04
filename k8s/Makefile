# Makefile-k8s

# Default namespace for Kubernetes manifests
K8S_DIR := k8s

.PHONY: all build deploy migrate restart clean

# Build local Docker images inside Minikube environment
build:
	@echo "🔨 Building Docker images in Minikube context..."
	@eval $$(minikube -p minikube docker-env) && \
	docker build -t api:latest -f ../Dockerfile .. && \
	docker build -t dagster:latest -f ../Dockerfile.dagster .. && \
	docker build -t migrate:latest -f ../Dockerfile ..

# Apply all Kubernetes manifests
deploy:
	@echo "🚀 Applying Kubernetes manifests..."
	kubectl apply -f .

# Restart all deployments
restart:
	@echo "🔄 Restarting all deployments..."
	kubectl rollout restart deployment dagster dagster-daemon api postgres

# Delete everything
clean:
	@echo "🧹 Deleting all Kubernetes resources..."
	kubectl delete -f . --recursive

# Re-run just the migration
migrate:
	kubectl delete job migrate || true
	kubectl apply -f /jobs/migrate.yaml

open-dagster:
	@minikube service dagster-service

open-swagger:
	@echo "🌐 Opening Swagger UI..."
	xdg-open "$$(minikube service api-service --url)/docs" || open "$$(minikube service api-service --url)/docs"

