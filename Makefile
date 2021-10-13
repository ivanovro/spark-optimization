.DEFAULT_GOAL := help

# From https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: get_data
get-data: ## Download datasets
	./collect_data.sh

.PHONY: build_local
build_local: ## Build docker images for local setup
	cd docker-local
	./build.sh

.PHONY: build_instruqt
build_instruqt: ## Build docker images for Instruqt setup
	cd docker
	./build.sh

.PHONY: push
push: ## Push recently built docker images to docker registry
	./push.sh

.PHONY: up
up: ## Bring docker application up
	./start.sh

.PHONY: down
down: ## Bring docker application down
	docker-compose down -v

.PHONY: cleanup
cleanup: ## Clean docker environment
	./cleanup.sh
