.DEFAULT_GOAL := help
PY      ?= .venv/bin/python
BIN     := .venv/bin
KUSTOMIZE := kubectl kustomize --load-restrictor LoadRestrictionsNone
CRDS    := https://raw.githubusercontent.com/datreeio/CRDs-catalog/main/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json

.PHONY: help
help: ## Show targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n",$$1,$$2}'

.venv:
	python3 -m venv .venv && $(BIN)/pip install -q --upgrade pip

.PHONY: install
install: .venv ## Install package + dev tools
	$(BIN)/pip install -q -e '.[dev]'

.PHONY: lint
lint: ## Ruff lint + format check
	$(BIN)/ruff check src tests
	$(BIN)/ruff format --check src tests

.PHONY: typecheck
typecheck: ## mypy --strict
	$(BIN)/mypy src

.PHONY: test
test: ## Unit + integration tests with coverage gate
	$(BIN)/pytest --cov --cov-report=term

.PHONY: check
check: lint typecheck test k8s-validate ## Everything CI runs

.PHONY: up
up: ## Start the full local stack (docker compose)
	docker compose up -d --build --wait

.PHONY: down
down: ## Stop the stack and delete volumes
	docker compose down -v

.PHONY: demo
demo: ## Fly 3 aircraft; motor 7 on N301FL overheats and pages (watch :8090/incidents, :3000)
	$(BIN)/flightline simulate --aircraft 3 --duration 120 --scenario motor-overtemp

.PHONY: e2e
e2e: ## End-to-end test against the running stack (make up first)
	$(BIN)/pytest -m e2e -v

.PHONY: bench
bench: ## Load-test ingest from inside the compose network
	docker compose run --rm --no-deps -T ingest bench --url http://ingest:8080 --seconds 20 --concurrency 8

.PHONY: k8s-render
k8s-render: ## Render both overlays to build/
	@mkdir -p build
	$(KUSTOMIZE) deploy/k8s/overlays/local > build/k8s-local.yaml
	$(KUSTOMIZE) deploy/k8s/overlays/aws   > build/k8s-aws.yaml

.PHONY: k8s-validate
k8s-validate: k8s-render ## Schema-validate rendered manifests (incl. KEDA/ESO CRDs)
	docker run --rm -v "$(CURDIR)/build:/m" ghcr.io/yannh/kubeconform:v0.6.7 -strict -summary \
	  -kubernetes-version 1.31.0 -schema-location default -schema-location '$(CRDS)' \
	  /m/k8s-local.yaml /m/k8s-aws.yaml

.PHONY: k8s-local
k8s-local: ## Deploy to a local cluster (k3s/kind/Docker Desktop) with the image preloaded
	$(KUSTOMIZE) deploy/k8s/overlays/local | kubectl apply -f -
	kubectl -n flightline rollout status deploy/ingest --timeout=180s

.PHONY: tf-validate
tf-validate: ## terraform fmt + validate (no AWS credentials needed)
	docker run --rm -v "$(CURDIR)/deploy/terraform:/tf" -w /tf hashicorp/terraform:1.9 fmt -check
	docker run --rm -v "$(CURDIR)/deploy/terraform:/tf" -w /tf hashicorp/terraform:1.9 init -backend=false -input=false >/dev/null
	docker run --rm -v "$(CURDIR)/deploy/terraform:/tf" -w /tf hashicorp/terraform:1.9 validate
