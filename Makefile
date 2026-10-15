.DEFAULT_GOAL := help

COMPOSE := docker compose -f docker-compose.yaml
COMPOSE_DEV := $(COMPOSE) -f docker-compose.dev.yaml
SERVICE ?= control_plane_server

.PHONY: help \
        dev-build dev-up dev-down dev-restart dev-logs dev-ps dev-shell dev-config

help: ## Показать доступные команды
	@awk 'BEGIN { FS = ":.*## " } /^[a-z][a-z-]*:.*## / { printf "  %-14s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

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
