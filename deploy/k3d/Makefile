# cache-kit local Valkey-cluster infra on k3d.
# Usage: make up   (create everything)   |   make down   (tear down)
CLUSTER := cache-kit
NS       := cache-kit
KUBECTL  := kubectl --context k3d-$(CLUSTER)
SEED     := valkey-cluster-0.valkey-cluster-headless.$(NS).svc.cluster.local
APP_IMG  := cachekit-testapp:dev
REPO_ROOT := ../..

.PHONY: up down create apply bootstrap verify status logs psh \
        testapp testapp-build testapp-logs testapp-demo

up: create apply bootstrap verify ## Full stack: k3d cluster + Valkey cluster + bootstrap + verify

create: ## Create the k3d cluster
	k3d cluster create --config k3d.yaml

apply: ## Apply namespace/config/service/statefulset and wait for pods
	$(KUBECTL) apply -f valkey-cluster.yaml
	$(KUBECTL) -n $(NS) rollout status statefulset/valkey-cluster --timeout=180s

bootstrap: ## Form the cluster (idempotent)
	$(KUBECTL) -n $(NS) delete job valkey-cluster-bootstrap --ignore-not-found
	$(KUBECTL) apply -f bootstrap-job.yaml
	$(KUBECTL) -n $(NS) wait --for=condition=complete job/valkey-cluster-bootstrap --timeout=180s
	$(KUBECTL) -n $(NS) logs job/valkey-cluster-bootstrap

verify: ## Prove slots are covered and cross-slot routing works
	$(KUBECTL) -n $(NS) exec valkey-cluster-0 -- valkey-cli cluster info | tr -d '\r'
	$(KUBECTL) -n $(NS) exec valkey-cluster-0 -- valkey-cli cluster shards | tr -d '\r' | head -40
	@echo "--- routed SET/GET across slots ---"
	$(KUBECTL) -n $(NS) exec valkey-cluster-0 -- sh -c 'for k in a b c foo bar user:1; do valkey-cli -c set "$$k" "v-$$k" >/dev/null && valkey-cli -c get "$$k"; done'

status: ## Show pods
	$(KUBECTL) -n $(NS) get pods -o wide

logs: ## Tail a node's logs (make logs N=2)
	$(KUBECTL) -n $(NS) logs -f valkey-cluster-$(or $(N),0)

psh: ## Open a cluster-mode valkey-cli shell
	$(KUBECTL) -n $(NS) exec -it valkey-cluster-0 -- valkey-cli -c

testapp: testapp-build ## Build, import and deploy the cache-kit test app
	$(KUBECTL) apply -f testapp.yaml
	$(KUBECTL) -n $(NS) rollout status deploy/cachekit-testapp --timeout=120s
	$(KUBECTL) -n $(NS) logs deploy/cachekit-testapp

testapp-build: ## Build the test app image and load it into k3d
	docker build -f testapp/Dockerfile -t $(APP_IMG) $(REPO_ROOT)
	k3d image import $(APP_IMG) -c $(CLUSTER)

testapp-logs: ## Tail the test app logs
	$(KUBECTL) -n $(NS) logs -f deploy/cachekit-testapp

testapp-demo: ## Run the cross-slot cache-aside demo over HTTP
	$(KUBECTL) -n $(NS) exec deploy/cachekit-testapp -- wget -qO- http://localhost:8080/demo

down: ## Delete the whole k3d cluster
	k3d cluster delete $(CLUSTER)
