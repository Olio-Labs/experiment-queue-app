# Experiment Queue App

Experiment queue management system with a FastAPI REST backend and React/Vite/TypeScript frontend.

Migrated from the original Flask/Jinja2 monolith per [OLI-1556](https://linear.app/olio-labs/issue/OLI-1556).

## Architecture

```
experiment-queue-app/
  api/          ← FastAPI backend (Python)
  ui/           ← React + Vite + TypeScript frontend
  Dockerfile    ← Multi-stage build
```

## Development

### Backend

```bash
cd api
uv sync
cp .env.example .env  # fill in your credentials
uv run uvicorn app.main:app --reload --port 8000
```

### Frontend

```bash
cd ui
npm install
npm run dev
```

The Vite dev server proxies `/api` requests to `localhost:8000`.

### Tests

```bash
cd api
uv run pytest
```

## Docker

```bash
# Production build
docker compose up

# Development with hot reload
docker compose --profile dev up
```

## API Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/api/experiments` | GET | List active experiments |
| `/api/experiments` | POST | Create experiment |
| `/api/experiments/{id}` | GET/PUT/DELETE | CRUD operations |
| `/api/experiments/form-options` | GET | Form dropdown options |
| `/api/scheduling/preview` | GET | Plan preview |
| `/api/scheduling/push` | POST | Push plan to Airtable |
| `/api/scheduling/clear` | POST | Clear scheduled plan |
| `/api/scheduling/recalculate` | POST | Recalculate times |
| `/api/calendar/weekly` | GET | Calendar embed URL |
| `/api/cages` | GET/POST | List/create cages |
| `/api/cages/form-options` | GET | Cage form options |
| `/api/cages/preview` | POST | Preview cage creation |
| `/api/box-room` | GET | Box room layout data |
| `/api/box-room/video` | GET | Box video presigned URL |
| `/api/box-room/flagged-issues/{n}` | GET | Box flagged issues |
| `/api/box-room/cart-videos` | GET | Cart event videos |
| `/api/box-room/cart-clip` | GET | Stream trimmed clip |
| `/api/health` | GET | Health check |
