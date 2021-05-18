import unittest
from typing import List

import airflow.models


class AirbodsTestCase(unittest.TestCase):

    def setUp(self):
        self.dag = self.dagbag.get_dag(dag_id='airbods')  # type: airflow.DAG

    @classmethod
    def setUpClass(cls):
        cls.dagbag = airflow.models.DagBag()

    def test_dag_loaded(self):
        assert self.dagbag.import_errors == dict()
        assert self.dag is not None
        assert len(self.dag.tasks) == 1

    def test_list_devices(self):
        task = self.dag.get_task('list_devices')
        devices = task.execute(context=dict())

        # Check data structure
        self.assertIsInstance(devices, list)
        for device in devices:
            self.assertIsInstance(device, dict)
            self.assertIsInstance(device['id'], str)
