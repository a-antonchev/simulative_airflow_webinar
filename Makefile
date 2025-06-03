SHELL := /bin/bash
PY = .venv/bin/python

.PHONY: services-up services-down af-up af-down lint

lint:
	pre-commit run --all-files
	pre-commit run --all-files

services-up:
	docker compose -f docker-compose-services.yaml up -d --build

services-down:
	docker compose -f docker-compose-services.yaml down -v

af-up:
	docker compose -f docker-compose-af.yaml up -d --build

af-down:
	docker compose -f docker-compose-af.yaml down -v 
