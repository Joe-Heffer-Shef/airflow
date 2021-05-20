# airflow
[Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html)

## Testing

Test a specific task

```bash
# docker-compose exec airflow-worker airflow task test <dag> <task> <date>
docker-compose exec --user airflow airflow-worker airflow tasks test airbods raw_04a7d0f3-855e-481e-8bf2-8f4c4bb7049c 2021-05-20
```



Run unit tests

```bash
docker-compose exec airflow-worker python -m unittest --failfast
```

