import airflow.utils.dates
from airflow.providers.ssh.operators.ssh import SSHOperator

with airflow.DAG(
        'hpc_example',
        description='HPC interface test workflow',
        tags=['example'],
        start_date=airflow.utils.dates.days_ago(1),
) as dag:
    task1 = SSHOperator(
        task_id='hello_world',
        ssh_conn_id='bessemer',
        command="sacct"
    )
