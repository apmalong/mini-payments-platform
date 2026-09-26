COMPOSE = docker compose --env-file .env

up-core:      ; $(COMPOSE) --profile core up -d
up-streaming: ; $(COMPOSE) --profile core --profile streaming up -d
up-ml:        ; $(COMPOSE) --profile core --profile ml up -d
up-gateway:   ; $(COMPOSE) --profile gateway up -d
up-obs:       ; $(COMPOSE) --profile obs up -d
up:           ; $(COMPOSE) --profile core --profile streaming --profile ml --profile gateway --profile obs up -d
down:         ; $(COMPOSE) --profile "*" down

generate:     ; python generator/generate.py
ingest:       ; python ingestion/sync.py
dbt-build:    ; cd warehouse/dbt && dbt build --profiles-dir .
pii-check:    ; python governance/check_pii.py
lint:         ; ruff check . && sqlfluff lint warehouse/dbt/models

.PHONY: up-core up-streaming up-ml up-gateway up-obs up down generate ingest dbt-build pii-check lint
