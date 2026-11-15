# Control Plane Client

Пустой проект SvelteKit с TypeScript и Node-адаптером. Для локальной разработки используется Node.js 24 и npm.

## Локальный запуск

Из `client/`:

```sh
npm ci
npm run dev
```

Приложение доступно на http://localhost:5173.

```sh
npm run check
npm run build
npm start
```

Собранное приложение запускается на порту 3000.

## Docker Compose

Команды выполняются из корня репозитория:

```sh
# Обычная сборка: стадия runtime, node build
docker compose -f docker-compose.yaml up -d --build

# Разработка: стадия dev, Vite и hot reload
make dev-up
```

Caddy принимает запросы на http://localhost:8080: `/api` и `/api/*` направляются в `cp_server:8000`, остальные пути — в `cp_client:3000`. WebSocket для hot reload проходит через тот же адрес.

Dev-конфигурация backend требует `server/.env.dev` и внешние сети `dev_valkey` и `dev_nats`. В обычном окружении настройки backend передаются отдельно.

В dev исходники `client/` подключены в контейнер; `node_modules` и `.svelte-kit` хранятся в Docker volumes. Зависимости обновляются через `npm ci` при запуске контейнера. После изменения `package.json` и `package-lock.json` перезапустите `cp_client`.
