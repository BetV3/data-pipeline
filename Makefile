SHELL := /bin/bash

.PHONY: up down reset ps logs smoke \
        venv-producer venv-consumer producer consumer \
        db-count db-shell kafka-topics kafka-consume

up:
	cd platform/compose && docker compose up -d

down:
	cd platform/compose && docker compose down

reset:
	cd platform/compose && docker compose down -v

ps:
	cd platform/compose && docker compose ps

logs:
	cd platform/compose && docker compose logs -f --tail=200

smoke:
	bash scripts/smoke_test.sh

venv-producer:
	cd apps/producer-python && \
	  [ -d .venv ] || python -m venv .venv; \
	  source .venv/bin/activate; \
	  pip install -r requirements.txt

venv-consumer:
	cd apps/consumer-python && \
	  [ -d .venv ] || python -m venv .venv; \
	  source .venv/bin/activate; \
	  pip install -r requirements.txt

producer: venv-producer
	cd apps/producer-python && \
	  source .venv/bin/activate && \
	  python src/producer.py --seconds $${SECONDS:-10} --rate $${RATE:-25}

consumer: venv-consumer
	cd apps/consumer-python && \
	  source .venv/bin/activate && \
	  python src/consumer.py

db-count:
	docker exec -i dpp-postgres psql -U platform -d platform -c "SELECT COUNT(*) FROM raw_events;"

db-shell:
	docker exec -it dpp-postgres psql -U platform -d platform

kafka-topics:
	docker exec -it dpp-kafka kafka-topics --bootstrap-server kafka:29092 --list

kafka-consume:
	docker exec -it dpp-kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic events --from-beginning --max-messages 5
