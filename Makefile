.PHONY: up down logs clean

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f sensor-job generator

clean:
	docker compose down -v
