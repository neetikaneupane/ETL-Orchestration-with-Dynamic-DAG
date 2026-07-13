import os
import sys

# Prevent Airflow from initializing during test collection
os.environ["_AIRFLOW__AS_LIBRARY"] = "1"
os.environ["AIRFLOW_HOME"] = "/tmp/airflow"
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "plugins"))
