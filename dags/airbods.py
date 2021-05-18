import datetime
import textwrap

import airflow
from airflow.providers.http.operators.http import SimpleHttpOperator


# TODO https://airflow.apache.org/docs/apache-airflow/stable/howto/custom-operator.html
class GraphQLHttpOperator(SimpleHttpOperator):
    pass


with airflow.DAG(
        dag_id='airbods',
        start_date=datetime.datetime(2021, 5, 17, tzinfo=datetime.timezone.utc)
) as dag:
    list_devices = SimpleHttpOperator(
        task_id='list_devices',
        http_conn_id='datacake_airbods',
        method='GET',
        data=dict(
            query=textwrap.dedent("""
        query {
          allDevices(inWorkspace:"0bdfb2eb-6531-4afb-a842-ce6b51d3c980") {
            id
          }
        }
        """)
        ),
        response_filter=lambda response: response.json()['data']['allDevices']
    )
