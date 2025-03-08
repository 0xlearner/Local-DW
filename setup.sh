docker compose -f docker-compose.test.yml exec -it superset superset fab create-admin \
              --username admin \
              --firstname Superset \
              --lastname Admin \
              --email admin@localhost \
              --password secret

docker compose -f docker-compose.test.yml exec -it superset superset db upgrade &&
         docker exec -it superset superset init
