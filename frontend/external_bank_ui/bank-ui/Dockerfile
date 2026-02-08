# frontend/Dockerfile
FROM node:22-alpine

WORKDIR /app

# Install dependencies first (better caching)
COPY package.json package-lock.json ./
RUN npm install

# Copy source code
COPY . .

# Expose Vite's default port
EXPOSE 5173

# Start development server
# --host 0.0.0.0 is CRITICAL for Docker to expose the port
CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0"]