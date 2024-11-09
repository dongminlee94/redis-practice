setup:
	pip install -r requirements.txt

setup-dev:
	make setup
	pip install -r requirements-dev.txt

format:
	black . --line-length 110
	isort . --profile black

lint:
	flake8 . --max-line-length 110 --extend-ignore E203

check:
	make format
	make lint

up:
	docker compose up -d

down:
	docker compose down -v
