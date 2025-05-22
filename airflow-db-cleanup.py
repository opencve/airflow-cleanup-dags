import logging
import os
from datetime import datetime, timedelta

import airflow
import dateutil.parser
from airflow import settings
from airflow.configuration import conf
from airflow.decorators import dag, task
from airflow.models import (DAG, DagModel, DagRun, DagTag, ImportError, Log,
                            SlaMiss, TaskFail, TaskInstance, TaskReschedule,
                            Variable, XCom)
from airflow.utils import timezone
from sqlalchemy import and_, func
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import load_only

try:
    from airflow.jobs.job import Job as BaseJob
except Exception as e:
    try:
        from airflow.jobs.base_job import BaseJob
    except Exception as e:
        from airflow.jobs import BaseJob

now = timezone.utcnow

# airflow-db-cleanup
DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
START_DATE = airflow.utils.dates.days_ago(1)
# How often to Run. @daily - Once a day at Midnight (UTC)
SCHEDULE_INTERVAL = "@daily"
# Length to retain the log files if not already provided in the conf. If this
# is set to 30, the job will remove files that are 30 days old or older.
DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS = int(
    Variable.get("airflow_db_cleanup__max_db_entry_age_in_days", 30)
)
# Whether the job should delete the db entries or not. Included if you want to
# temporarily avoid deleting the db entries.
ENABLE_DELETE_STR = Variable.get("airflow_db_cleanup__enable_delete", default_var="true")
ENABLE_DELETE = ENABLE_DELETE_STR.lower() == "true"

# Determine the correct age check column name for DagModel
# This adapts to different Airflow versions.
dag_model_actual_age_check_column_name = ""
try:
    _ = DagModel.last_scheduler_run  # Attempt to access the attribute
    dag_model_actual_age_check_column_name = "last_scheduler_run"
except AttributeError:
    dag_model_actual_age_check_column_name = "last_parsed_time"
except Exception as e:
    # Fallback or handle unexpected error if DagModel itself is problematic
    logging.error(f"Could not determine DagModel age check column: {e}. Defaulting to 'last_parsed_time'.")
    dag_model_actual_age_check_column_name = "last_parsed_time"

# List of all the objects that will be deleted. Comment out the DB objects you
# want to skip.
DATABASE_OBJECTS = [
    {
        "airflow_db_model": BaseJob,
        "age_check_column_name": "latest_heartbeat",
        "keep_last": False,
        "keep_last_filters_config": None,
        "keep_last_group_by_column_name": None
    },
    {
        "airflow_db_model": DagRun,
        "age_check_column_name": "execution_date",
        "keep_last": True,
        "keep_last_filters_config": [
            {"column_name": "external_trigger", "operator": "is_", "value": False}
        ],
        "keep_last_group_by_column_name": "dag_id"
    },
    {
        "airflow_db_model": TaskInstance,
        "age_check_column_name": "end_date",
        "keep_last": False,
        "keep_last_filters_config": None,
        "keep_last_group_by_column_name": None
    },
    {
        "airflow_db_model": Log,
        "age_check_column_name": "dttm",
        "keep_last": False,
        "keep_last_filters_config": None,
        "keep_last_group_by_column_name": None
    },
    {
        "airflow_db_model": XCom,
        "age_check_column_name": "timestamp",
        "keep_last": False,
        "keep_last_filters_config": None,
        "keep_last_group_by_column_name": None
    },
    {
        "airflow_db_model": SlaMiss,
        "age_check_column_name": "execution_date",
        "keep_last": False,
        "keep_last_filters_config": None,
        "keep_last_group_by_column_name": None
    },
    {
        "airflow_db_model": DagModel,
        "age_check_column_name": dag_model_actual_age_check_column_name,
        "keep_last": False,
        "keep_last_filters_config": None,
        "keep_last_group_by_column_name": None
    }
]

# Check for TaskReschedule model
try:
    from airflow.models import TaskReschedule
    DATABASE_OBJECTS.append({
        "airflow_db_model": TaskReschedule,
        "age_check_column_name": "reschedule_date",
        "keep_last": False,
        "keep_last_filters_config": None,
        "keep_last_group_by_column_name": None
    })
except Exception as e:
    logging.error(f"Could not import TaskReschedule: {e}")

# Check for TaskFail model
try:
    from airflow.models import TaskFail
    DATABASE_OBJECTS.append({
        "airflow_db_model": TaskFail,
        "age_check_column_name": "end_date",
        "keep_last": False,
        "keep_last_filters_config": None,
        "keep_last_group_by_column_name": None
    })
except Exception as e:
    logging.error(f"Could not import TaskFail: {e}")

# Check for ImportError model
try:
    from airflow.models import ImportError
    DATABASE_OBJECTS.append({
        "airflow_db_model": ImportError,
        "age_check_column_name": "timestamp",
        "keep_last": False,
        "keep_last_filters_config": None,
        "keep_last_group_by_column_name": None
        })
except Exception as e:
    logging.error(f"Could not import ImportError: {e}")

# Check for celery executor
airflow_executor = str(conf.get("core", "executor"))
logging.info("Airflow Executor: " + str(airflow_executor))
if airflow_executor == "CeleryExecutor":
    logging.info("Including Celery Modules")
    try:
        from celery.backends.database.models import Task, TaskSet
        DATABASE_OBJECTS.extend((
            {
                "airflow_db_model": Task,
                "age_check_column_name": "date_done",
                "keep_last": False,
                "keep_last_filters_config": None,
                "keep_last_group_by_column_name": None
            },
            {
                "airflow_db_model": TaskSet,
                "age_check_column_name": "date_done",
                "keep_last": False,
                "keep_last_filters_config": None,
                "keep_last_group_by_column_name": None
            }))
    except Exception as e:
        logging.error(f"Could not import Celery Task/TaskSet: {e}")

session = settings.Session()

default_args = {
    'depends_on_past': False,
    'start_date': START_DATE,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

doc_md_DAG = """
## Airflow MetaStore Cleanup DAG

This DAG performs periodic cleanup of the Airflow MetaStore to prevent excessive data accumulation and maintain database performance.

### Overview

The `airflow-db-cleanup` DAG removes old records from various metadata tables, including:

*   `DagRun`
*   `TaskInstance`
*   `Log`
*   `XCom`
*   `BaseJob` (Job table)
*   `SlaMiss`
*   `DagModel`
*   `TaskReschedule` (if available)
*   `TaskFail` (if available)
*   `RenderedTaskInstanceFields` (if available)
*   `ImportError` (if available)
*   Celery `Task` and `TaskSet` tables (if CeleryExecutor is used)

### Configuration

The primary configuration parameter is `maxDBEntryAgeInDays`, which determines the maximum age (in days) of records to keep. Older records will be deleted.

**Setting `maxDBEntryAgeInDays`:**

1.  **Via DAG Run Configuration (Manual Trigger):**
    You can specify `maxDBEntryAgeInDays` when triggering the DAG manually:
    ```bash
    airflow trigger_dag airflow-db-cleanup --conf '{"maxDBEntryAgeInDays": 30}'
    ```
    Replace `30` with your desired retention period.

2.  **Via Airflow Variable (Default):**
    If not specified in the DAG run configuration, the DAG will use the value from the Airflow Variable `airflow_db_cleanup__max_db_entry_age_in_days`.
    The default value for this variable (if the Airflow Variable itself is not set) is `30` days, as defined in the DAG script.

### Important Script Variables

These control the DAG's behavior and are set within the Python script:

*   `ENABLE_DELETE` (default: `False`): **This is a critical safety switch.** Set this to `True` to enable actual deletion of database records. If `False`, the DAG performs a "dry run", logging what would be deleted without making any changes.

### Scheduling

By default, this DAG is scheduled to run daily (`@daily`). Adjust the `SCHEDULE_INTERVAL` variable in the script if a different frequency is needed.
"""

@task
def validate_db_object_columns():
    """
    Validates that the configured 'age_check_column' for each database object
    exists in the respective model's database table.
    Raises ValueError if any column is invalid.
    """
    logging.info("Validating age_check_column_name for all DATABASE_OBJECTS...")
    invalid_configs = []
    for db_object_config in DATABASE_OBJECTS:
        model = db_object_config["airflow_db_model"]
        age_check_column_name_str = db_object_config["age_check_column_name"]

        try:
            age_check_column_attr = getattr(model, age_check_column_name_str)
            logging.info(f"Validating model: {model.__name__}, age_check_column attribute key: {age_check_column_attr.key}")
        except AttributeError:
            error_msg = (
                f"Validation Error: For model {model.__name__}, the "
                f"age_check_column_name '{age_check_column_name_str}' does not correspond to a valid attribute."
            )
            logging.error(error_msg)
            invalid_configs.append(error_msg)
            continue
        except Exception as e_log:
            logging.info(f"Could not log details for model {model.__name__}.{age_check_column_name_str}. Error: {e_log}")
            invalid_configs.append(f"Error accessing attribute for {model.__name__}.{age_check_column_name_str}: {e_log}")
            continue

        if not hasattr(age_check_column_attr, 'name'):
            error_msg = (
                f"Validation Error: For model {model.__name__}, the "
                f"resolved attribute for '{age_check_column_name_str}' ({age_check_column_attr}) does not have a 'name' property. "
                f"It might not be a valid SQLAlchemy Column object."
            )
            logging.error(error_msg)
            invalid_configs.append(error_msg)
            continue

        resolved_column_name_in_table = age_check_column_attr.name

        if not hasattr(model, '__table__') or not hasattr(model.__table__, 'columns'):
            error_msg = (
                f"Validation Error: Model {model.__name__} does not have a '__table__.columns' attribute. "
                f"Cannot validate columns."
            )
            logging.error(error_msg)
            invalid_configs.append(error_msg)
            continue
            
        if resolved_column_name_in_table not in model.__table__.columns:
            error_msg = (
                f"Validation Error: For model {model.__name__}, the specified "
                f"age_check_column_name '{age_check_column_name_str}' (resolves to '{resolved_column_name_in_table}') does not exist in the table. "
                f"Available columns: {list(model.__table__.columns.keys())}"
            )
            logging.error(error_msg)
            invalid_configs.append(error_msg)
        else:
            logging.info(f"Validated: {model.__name__}.{resolved_column_name_in_table} is a valid column.")
            
    if invalid_configs:
        error_summary = "\\n".join(invalid_configs)
        raise ValueError(f"DAG configuration error(s) found with age_check_column_name:\\n{error_summary}")
    logging.info("All age_check_column_name configurations are valid.")


@task
def print_configuration_function(dag_run: DagRun = None):
    """
    Logs the DAG run configuration and calculates the maximum date for record retention.
    Returns the calculated maximum date as an ISO formatted string.
    """
    logging.info("Loading configurations...")
    dag_run_conf = dag_run.conf if dag_run and hasattr(dag_run, 'conf') else {}
    logging.info("dag_run.conf: " + str(dag_run_conf))
    max_db_entry_age_in_days = None
    if dag_run_conf:
        max_db_entry_age_in_days = dag_run_conf.get(
            "maxDBEntryAgeInDays", None
        )
    logging.info("maxDBEntryAgeInDays from dag_run.conf: " + str(max_db_entry_age_in_days))
    if (max_db_entry_age_in_days is None or max_db_entry_age_in_days < 1):
        logging.info(
            "maxDBEntryAgeInDays conf variable isn't included or Variable " +
            "value is less than 1. Using Default '" +
            str(DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS) + "'"
        )
        max_db_entry_age_in_days = DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS
    max_date = now() + timedelta(-max_db_entry_age_in_days)
    logging.info("Finished loading configurations")
    logging.info("")

    logging.info("Configurations:")
    logging.info("max_db_entry_age_in_days: " + str(max_db_entry_age_in_days))
    logging.info("max_date:                 " + str(max_date))
    logging.info("enable_delete:            " + str(ENABLE_DELETE))
    logging.info("")

    logging.info("Returning max_date for downstream processes")
    return max_date.isoformat()


@task
def cleanup_function(max_date_iso: str, db_object: dict):
    """
    Performs the cleanup of old records for a specific database table (Airflow model).
    Deletes records older than the provided max_date.
    """
    max_date = dateutil.parser.parse(max_date_iso)
    airflow_db_model = db_object.get("airflow_db_model")
    age_check_column_name_str = db_object.get("age_check_column_name")
    keep_last = db_object.get("keep_last")
    keep_last_filters_config = db_object.get("keep_last_filters_config")
    keep_last_group_by_column_name = db_object.get("keep_last_group_by_column_name")

    try:
        age_check_column = getattr(airflow_db_model, age_check_column_name_str)
    except AttributeError:
        logging.error(f"AttributeError: Could not find column '{age_check_column_name_str}' in model {airflow_db_model.__name__}. Skipping cleanup for this model.")
        return

    logging.info(f"Starting cleanup for {airflow_db_model.__name__} using column {age_check_column.key}")
    logging.info("Configurations for this task:")
    logging.info(f"airflow_db_model: {airflow_db_model.__name__}")
    logging.info(f"age_check_column (resolved): {age_check_column.key}")
    logging.info(f"keep_last: {keep_last}")
    logging.info(f"max_date: {max_date}")

    logging.info("Running cleanup process...")

    try:
        query = session.query(airflow_db_model).options(
            load_only(age_check_column)
        )

        if keep_last:
            if airflow_db_model == DagRun:
                subquery_max_column_attr = DagRun.execution_date
            else:
                subquery_max_column_attr = age_check_column
                logging.warning(
                    f"keep_last=True for {airflow_db_model.__name__}, using its age_check_column "
                    f"'{age_check_column.key}' for subquery's func.max(). Verify this is correct."
                )

            subquery = session.query(func.max(subquery_max_column_attr))

            if keep_last_filters_config is not None:
                for filter_config in keep_last_filters_config:
                    column_attr = getattr(airflow_db_model, filter_config["column_name"])
                    operator = filter_config["operator"]
                    value = filter_config["value"]
                    if operator == "is_":
                        subquery = subquery.filter(column_attr.is_(value))
                    else:
                        logging.error(f"Unsupported operator '{operator}' in keep_last_filters_config for model {airflow_db_model.__name__}")

            if keep_last_group_by_column_name is not None:
                group_by_column_attr = getattr(airflow_db_model, keep_last_group_by_column_name)
                subquery = subquery.group_by(group_by_column_attr)
            
            subquery = subquery.from_self()

            query = query.filter(
                and_(age_check_column.notin_(subquery)),
                and_(age_check_column <= max_date)
            )
        else:
            query = query.filter(age_check_column <= max_date,)

        count_to_delete = query.count()
        logging.info(
            f"Found {count_to_delete} {airflow_db_model.__name__} record(s) to delete"
        )

        if count_to_delete > 0:
            if ENABLE_DELETE:
                logging.info('Performing delete...')
                if airflow_db_model.__name__ == 'DagModel':
                    logging.info('Deleting tags for DagModel entries...')
                    ids_query = query.from_self().with_entities(DagModel.dag_id)
                    tags_query = session.query(DagTag).filter(DagTag.dag_id.in_(ids_query))
                    tags_query.delete(synchronize_session=False)
                
                deleted_rows = query.delete(synchronize_session=False)
                session.commit()
                logging.info(f'Finished performing delete. {deleted_rows} rows deleted for {airflow_db_model.__name__}')
            else:
                logging.warning(
                    "Deletion is disabled. Set ENABLE_DELETE to True to delete entries.")
        else:
            logging.info(f"No {airflow_db_model.__name__} entries to delete based on the current criteria.")

        logging.info(f"Finished running cleanup process for {airflow_db_model.__name__}")

    except ProgrammingError as e:
        logging.error(e)
        logging.error(f"{airflow_db_model.__name__} is not present in the metadata. Skipping...")
    except Exception as e:
        logging.error(f"An unexpected error occurred during cleanup of {airflow_db_model.__name__}: {e}")
        raise # Re-raise the exception to fail the task

@dag(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=START_DATE,
    catchup=False,
    doc_md=doc_md_DAG
)
def airflow_db_cleanup_dag():
    """
    Airflow DB Cleanup DAG
    Periodically cleans out metadata database entries.
    """
    validation_status = validate_db_object_columns()

    # Restored direct call, relying on Airflow to pass context
    max_date_iso = print_configuration_function()

    validation_status >> max_date_iso

    for db_object_config in DATABASE_OBJECTS:
        # Dynamically override task_id for each cleanup task based on the model name
        cleanup_task = cleanup_function.override(task_id=f'cleanup_{db_object_config["airflow_db_model"].__name__}')(
            max_date_iso=max_date_iso,
            db_object=db_object_config
        )
        # Dependency is implicitly set by passing max_date_iso to cleanup_function

# Instantiate the DAG
airflow_db_cleanup_dag_instance = airflow_db_cleanup_dag()
