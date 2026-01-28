# Consumer (Python)

Consumes JSON events from Kafka and writes them into Postgres table `raw_events`.

## Setup
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
