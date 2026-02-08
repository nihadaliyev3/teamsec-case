# frontend/Dockerfile
FROM node:22-alpine

WORKDIR /app

COPY package.json package-lock.json ./
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

COPY . .

EXPOSE 5173

# Run npm install at runtime so node_modules use Linux binaries (volume-mounted)
ENTRYPOINT ["/entrypoint.sh"]