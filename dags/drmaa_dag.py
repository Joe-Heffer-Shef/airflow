import textwrap

import airflow.operators.python
import airflow.utils.dates

default_args = dict()


def get_drmaa_session_info() -> dict:
    import drmaa

    with drmaa.Session() as session:
        print('A session was started successfully')
        return session.__dict__


with airflow.DAG(
        'dramaa_test',
        default_args=default_args,
        description='HPC interface test workflow',
        tags=['example'],
        start_date=airflow.utils.dates.days_ago(2),
        doc_md=textwrap.dedent("""
Distributed Resource Management Application API (DRMAA)
https://docs.hpc.shef.ac.uk/en/latest/hpc/scheduler/drmaa.html

DRMAA Python documentation
https://drmaa-python.readthedocs.io/en/latest/
"""),
) as dag:
    task1 = airflow.operators.python.PythonVirtualenvOperator(
        task_id='session_info',
        python_callable=get_drmaa_session_info,
        requirements=['drmaa==0.7.*']
    )
