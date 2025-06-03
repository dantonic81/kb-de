# Makefile

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

clean:
	docker compose down --volumes --rmi all --remove-orphans

logs:
	docker compose logs -f

rebuild:
	docker compose up -d --build

ps:
	docker compose ps
