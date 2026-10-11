PY=.venv/bin/python
# Docker path: Postgres on 5434 and RabbitMQ on 5672 from docker-compose.yml.
DOCKER_CHAOS=OPP_PG_RESTART="docker compose restart postgres" OPP_MQ_STOP="docker compose stop rabbitmq" OPP_MQ_START="docker compose start rabbitmq"

setup:
	python3 -m venv .venv && .venv/bin/pip install -q -r requirements-dev.txt

services:
	docker compose up -d --wait

run:
	$(PY) -c "from opp import db; db.init_schema()"
	$(PY) -m opp.supervise & .venv/bin/uvicorn opp.api:app --port 8000

test:
	.venv/bin/pytest -q

chaos:
	$(PY) scripts/chaos.py

chaos-docker:
	$(DOCKER_CHAOS) $(PY) scripts/chaos.py

.PHONY: setup services run test chaos chaos-docker
