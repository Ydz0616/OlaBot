FROM node:20-alpine

WORKDIR /app

# Copy package files and install
COPY package.json package-lock.json* ./
RUN npm ci --no-audit --no-fund 2>/dev/null || npm install

# Copy source and build
COPY . .
RUN npm run build

# Create auth_info directory
RUN mkdir -p auth_info

EXPOSE 3001 3002

CMD ["node", "dist/index.js"]
