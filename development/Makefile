up/%:
	docker compose -f docker-compose-common.yml -f docker-compose-$*.yml up --build

upd/%:
	docker compose -f docker-compose-common.yml -f docker-compose-$*.yml up -d --build

down/%:
	docker compose -f docker-compose-common.yml -f docker-compose-$*.yml down --volumes
	rm -rf ./log/mysql ./log/nginx

ps/%:
	docker compose -f docker-compose-common.yml -f docker-compose-$*.yml ps
