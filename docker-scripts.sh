docker network create retail_net
docker compose -f .\infra-compose.yml up -d
docker compose -f .\bronze-kafka-connect-compose.yml up -d --build
docker compose -f .\spark-compose.yml up -d
# to do wywalenia będzie, bo wszystko będzie puszczanie z airflow'a
docker run --network=retail_net -v .\silver_data_transforms\:/app bitnami/spark:3.5.1
docker run -d -p 8085:8080 --env "_AIRFLOW_DB_MIGRATE=true" --env "_AIRFLOW_WWW_USER_CREATE=true" --env "_AIRFLOW_WWW_USER_PASSWORD=admin" --env "AIRFLOW__WEBSERVER__BASE_URL=http://localhost:8085" apache/airflow:slim-2.10.2-python3.12 standalone


# cleaning
docker compose -f .\bronze-kafka-connect-compose.yml -v down
docker compose -f .\infra-compose.yml -v down
docker compose -f .\spark-compose.yml -v down 