.PHONY: up down clean logs ps shell count scan metrics gaps mock

up:       ## Build and start everything
	docker compose up -d --build

down:     ## Stop containers, keep data
	docker compose down

clean:    ## Stop containers AND delete all data volumes (fresh start)
	docker compose down -v

ps:       ## Show container status
	docker compose ps -a

logs:     ## Follow Flume's log
	docker compose logs -f flume

shell:    ## Open an interactive HBase shell
	docker compose exec hbase hbase shell

count:    ## Count rows in upgrad
	@echo "count 'upgrad'" | docker compose exec -T hbase hbase shell -n 2>/dev/null | grep 'row(s)'

scan:     ## Show the first 3 rows by row key
	@echo "scan 'upgrad', {LIMIT => 3}" | docker compose exec -T hbase hbase shell -n 2>/dev/null | grep -E 'column=|row\(s\)'

metrics:  ## Flume source/channel/sink counters
	@./scripts/metrics.sh

gaps:     ## Check for lost events in HBase
	@./scripts/check-gaps.sh

mock:     ## Send one hand-made event (optional: make mock KEY=MOCK-hello)
	@./scripts/send-mock-event.sh $(KEY)
