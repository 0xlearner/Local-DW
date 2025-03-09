docker compose exec -it superset superset fab create-admin \
              --username admin \
              --firstname Superset \
              --lastname Admin \
              --email admin@localhost \
              --password secret

docker compose exec -it superset superset db upgrade &&
         docker exec -it superset superset init
