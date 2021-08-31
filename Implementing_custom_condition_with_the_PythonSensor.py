from pathlib import Path
from airflow.sensors.python import PythonSensor
import airflow.utils.dates
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow import DAG

dag = DAG(task_id='supermarket_file',
                  start_date = airflow.utils.dates.days_ago(3),
                  schedule_interval = "0 16 * * *",
                  description = "This is a dummy DAG to test the sensor",
                  default_args = {"depends_on_past": True}, )



def _wait_for_supermarket(supermarket_id):
    supermarket_path = Path("/data/", supermarket_id)
    data_files = supermarket_path.glob("data-*.csv")
    success_file = supermarket_path / "_SUCCESS"
    return data_files and success_file.exists()


for supermarket_id in range(1, 5):
    wait = PythonSensor(
    task_id="wait_for_supermarket_{supermarket_id}",
    python_callable=_wait_for_supermarket,
    op_kwargs={"supermarket_id": f"supermarket{supermarket_id}"},
    timeout = 600,
    dag=dag,)

    copy = DummyOperator(task_id=f"copy_supermarket_{supermarket_id}", dag=dag)
    processs = DummyOperator(task_id=f"process_supermarket_{supermarket_id}", dag=dag)
    create_metics = DummyOperator(task_id=f"create_metics_{supermarket_id}", dag=dag)

    wait >> copy >> processs >> create_metics


