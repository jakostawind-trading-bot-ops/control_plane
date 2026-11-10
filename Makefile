.DEFAULT_GOAL := help

COMPOSE := docker compose -f docker-compose.yaml
COMPOSE_DEV := $(COMPOSE) -f docker-compose.dev.yaml
SERVICE ?= control_plane_server
GO_TEST_FLAGS := -race -vet=all -cover -count=1 -shuffle=on

.PHONY: help test test-integration test-all \
        dev-build dev-up dev-down dev-restart dev-logs dev-ps dev-shell dev-config

help: ## Показать доступные команды
	@awk 'BEGIN { FS = ":.*## " } /^[a-z][a-z-]*:.*## / { printf "  %-14s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

test: ## Запустить Go-тесты без Docker
	cd server && CGO_ENABLED=1 go test $(GO_TEST_FLAGS) ./...

test-integration: ## Запустить интеграционные Go-тесты с Docker
	cd server && CGO_ENABLED=1 go test $(GO_TEST_FLAGS) \
		-tags=integration -coverpkg=./internal/... -timeout=3m ./tests/integration/...

test-all: test test-integration ## Запустить все Go-тесты, включая интеграционные

dev-build: ## Собрать dev-образ
	$(COMPOSE_DEV) build

dev-up: ## Собрать и запустить dev в фоне с reload
	$(COMPOSE_DEV) up -d --build

dev-down: ## Остановить и удалить контейнеры dev
	$(COMPOSE_DEV) down

dev-restart: ## Перезапустить контейнеры dev
	$(COMPOSE_DEV) restart

dev-logs: ## Смотреть логи сервиса в dev
	$(COMPOSE_DEV) logs --follow --tail=100 $(SERVICE)

dev-ps: ## Показать состояние контейнеров dev
	$(COMPOSE_DEV) ps

dev-shell: ## Открыть shell в сервисе dev
	$(COMPOSE_DEV) exec $(SERVICE) sh

dev-config: ## Проверить конфигурацию dev
	$(COMPOSE_DEV) config --quiet
