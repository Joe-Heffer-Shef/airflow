import datetime
import textwrap
import json

import airflow
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.hooks.http import HttpHook


class GraphQLHttpOperator(SimpleHttpOperator):
    def __init__(self, query: str, **kwargs):
        super().__init__(**kwargs)
        self.method = 'GET'
        self.query = query

    def execute(self, context: dict):
        self.data = dict(query=self.render_template(content=self.query,
                                                    context=context))
        text = super().execute(context)
        return json.loads(text)['data']


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
  }
}
""")))

    # Iterate over devices
    for device in response.json()['data']['allDevices']:
        # Download raw data for each device
        task = GraphQLHttpOperator(
            task_id='raw_{}'.format(device['id']),
            params=dict(
                device_id=device['id'],
            ),
            # Jinja escape characters for GraphQL syntax
            query="""
query {{ '{{' }}
  device(deviceId: "{{ device_id }}") {{ '{{' }}
    history(
      fields: [CO2,TEMPERATURE,LORAWAN_SNR,LORAWAN_DATARATE,LORAWAN_RSSI,AIR_QUALITY,HUMIDITY]
      timerangestart: "{{ ts }}"
      timerangeend: "{{ next_execution_date }}"
      resolution: "raw"
    ) 
  {{ '}}' }}
{{ '}}' }}
"""
        )
