import textwrap

import airflow.utils.dates

# def hello_world() -> bytes:
#     import socket
#     import ssh2.session
#
#     with socket.socket() as socket:
#         socket.connect(('bessemer.shef.ac.uk', 22))
#
#         with ssh2.session.Session() as session:
#             session.handshake(socket)
#             session.userauth_publickey_fromfile(
#                 username='cs1jsth',
#                 privatekey='/run/secrets/private_key',
#                 passphrase=
#             )
#             channel = session.open_session()
#             channel.execute('echo OK')
#             channel.wait_eof()
#             channel.close()
#             channel.wait_closed()
#
#             print("Exit status: %s" % channel.get_exit_status())
#
#             data = bytes()
#             while True:
#                 size, new_data = channel.read()
#                 if size:
#                     data += new_data
#                 else:
#                     break
#
#             return data
#

with airflow.DAG(
        'hpc_example',
        description='HPC interface test workflow',
        tags=['example'],
        start_date=airflow.utils.dates.days_ago(1),
) as dag:
    # task1 = airflow.operators.python.PythonVirtualenvOperator(
    #     task_id='session_info',
    #     python_callable=hello_world,
    #     requirements=['ssh2-python==0.26.*']
    # )

    task1 = airflow.operators.bash.BashOperator(
        task_id='hello_world',
        bash_command=textwrap.dedent("""
           
        """)
    )
