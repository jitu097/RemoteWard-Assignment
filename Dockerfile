# Worker image (the same image can run the API with `node src/api/server.js`).
FROM node:22-alpine
ENV NODE_ENV=production
WORKDIR /app

# Dependencies first so this layer is cached until the lockfile changes.
COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force

COPY src ./src

USER node
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s \
  CMD node -e "fetch('http://127.0.0.1:'+(process.env.WORKER_HEALTH_PORT||8080)+'/healthz').then(r=>process.exit(r.ok?0:1),()=>process.exit(1))"

# Exec form, no npm wrapper: node is PID 1 and receives SIGTERM directly for graceful shutdown.
CMD ["node", "src/worker/main.js"]
