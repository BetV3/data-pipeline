# Data Pipeline Platform

A high-throughput data processing system designed to handle millions of events daily with real-time analytics capabilities and fault-tolerant architecture.

## Overview
This project is built iteratively in tiers:

- **T0 Sandbox:** local stack + tracer-bullet end-to-end flow
- **T1 MVP:** DLQ, schema evolution, backpressure handling, basic observability
- **T2 Pilot:** multi-source ingestion, backfills, hot/cold storage, orchestration
- **T3 Hardening:** reliability drills, SLOs, scale tests, capacity model

## Local Development

### Prerequisites
- Docker + Docker Compose
- Python 3.11+ (for producer/consumer apps)

### Start the platform
From the repo root:
```bash
cd platform/compose
docker compose up -d
