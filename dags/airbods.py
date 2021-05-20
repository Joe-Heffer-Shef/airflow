import datetime
import textwrap
import json

import airflow
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.hooks.http import HttpHook


class GraphQLHttpOperator(SimpleHttpOperator):
    def __init__(self, query: str, **kwargs):
        super().__init__(**kwargs)
        self.method = 'GET'
        self.query = query

    def execute(self, context: dict):
        # Build template context
        _context = self.params.copy()
        _context.update(context)
        rendered_query = self.render_template(content=self.query,
                                              context=_context)
        self.log.debug(rendered_query)
        self.data = dict(query=rendered_query)
        return super().execute(context)


def save_file(task_instance, params, **kwargs):
    previous_task_id = 'raw_{}'.format(params['id'])
    data = task_instance.xcom_pull(task_ids=previous_task_id)

    filename = '{}.json'.format(params['id'])
    with open(filename, 'w') as file:
        file.write(data)
        return file.name


with airflow.DAG(
        dag_id='airbods',
        start_date=datetime.datetime(2021, 5, 17, tzinfo=datetime.timezone.utc)
) as dag:
    # Datacake HTTP
    hook = HttpHook(http_conn_id='datacake_airbods')

    # List devices
    response = hook.run(endpoint=None, json=dict(query=textwrap.dedent("""
query {
  allDevices(inWorkspace:"0bdfb2eb-6531-4afb-a842-ce6b51d3c980") {
    id
    serialNumber
    verboseName
  }
}
""")))

    # Iterate over devices
    for device in response.json()['data']['allDevices']:
        # Download raw data for each device
        task_id = 'raw_{}'.format(device['id'])
        get_raw_data = GraphQLHttpOperator(
            http_conn_id='datacake_airbods',
            task_id=task_id,
            doc=device['verboseName'],
            params=device,
            # Jinja escape characters for GraphQL syntax
            query="""
query {{ '{' }}
  device(deviceId: "{{ id }}") {{ '{' }}
    history(
      fields: ["CO2","TEMPERATURE","LORAWAN_SNR","LORAWAN_DATARATE","LORAWAN_RSSI","AIR_QUALITY","HUMIDITY"]
      timerangestart: "{{ ts }}"
      timerangeend: "{{ next_execution_date }}"
      resolution: "raw"
    ) 
  {{ '}' }}
{{ '}' }}
"""
        )

        # Saw raw JSON data
        save_raw_data = PythonOperator(
            task_id='save_{}'.format(device['id']),
            python_callable=save_file,
            params=device,
        )

        get_raw_data >> save_raw_data
