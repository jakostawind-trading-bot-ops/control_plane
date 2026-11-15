# syntax=docker/dockerfile:1

FROM node:24-alpine AS base

WORKDIR /app
COPY package.json package-lock.json .npmrc ./
RUN npm ci --ignore-scripts
COPY . .

FROM base AS dev

EXPOSE 3000
CMD ["sh", "-c", "npm ci --ignore-scripts && exec npm run dev -- --host 0.0.0.0 --port 3000 --strictPort"]

FROM base AS build

RUN npm run build

FROM node:24-alpine AS runtime

WORKDIR /app
ENV NODE_ENV=production \
    HOST=0.0.0.0 \
    PORT=3000

COPY package.json package-lock.json .npmrc ./
RUN npm ci --omit=dev --ignore-scripts && npm cache clean --force
COPY --from=build --chown=node:node /app/build ./build

USER node
EXPOSE 3000
CMD ["node", "build"]
