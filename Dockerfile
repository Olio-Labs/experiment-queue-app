# Stage 1: Build React frontend
FROM node:22-alpine AS frontend-build
WORKDIR /app/ui
COPY ui/package.json ui/pnpm-lock.yaml* ./
RUN corepack enable && pnpm install --frozen-lockfile || npm install
COPY ui/ .
RUN npm run build

# Stage 2: Python backend + serve static frontend
FROM python:3.12-slim AS runtime
WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install Python dependencies
COPY api/pyproject.toml api/
RUN cd api && uv sync --no-dev

# Copy backend code
COPY api/ api/

# Copy built frontend
COPY --from=frontend-build /app/ui/dist ui/dist

# Expose port
EXPOSE 8080

# Run the app
CMD ["uv", "run", "--directory", "api", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
