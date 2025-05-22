import logging
import os
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.configuration import conf
from airflow.models import Variable
from airflow.utils import dates

# airflow-log-cleanup
DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
START_DATE = dates.days_ago(1)
BASE_LOG_FOLDER = conf.get("logging", "BASE_LOG_FOLDER").rstrip("/")
# How often to Run. @daily - Once a day at Midnight
SCHEDULE_INTERVAL = "@daily"
# Length to retain the log files if not already provided in the conf. If this
# is set to 30, the job will remove files that are 30 days old or older
DEFAULT_MAX_LOG_AGE_IN_DAYS = Variable.get(
    "airflow_log_cleanup__max_log_age_in_days", 30
)
# Whether the job should delete the logs or not. Included if you want to
# temporarily avoid deleting the logs
ENABLE_DELETE_STR = Variable.get("airflow_log_cleanup__enable_delete", default_var="true")
ENABLE_DELETE = ENABLE_DELETE_STR.lower() == "true"
# The number of worker nodes you have in Airflow. Will attempt to run this
# process for the number of workers specified, so that each worker node involved can clear its
# logs cleared.
NUMBER_OF_WORKERS = 1
DIRECTORIES_TO_DELETE = [BASE_LOG_FOLDER]
ENABLE_DELETE_CHILD_LOG = Variable.get(
    "airflow_log_cleanup__enable_delete_child_log", "False"
)
LOG_CLEANUP_PROCESS_LOCK_FILE = "/tmp/airflow_log_cleanup_worker.lock"
logging.info(f"ENABLE_DELETE_CHILD_LOG: {ENABLE_DELETE_CHILD_LOG}")

if not BASE_LOG_FOLDER or BASE_LOG_FOLDER.strip() == "":
    raise ValueError(
        "BASE_LOG_FOLDER variable is empty in airflow.cfg. It can be found "
        "under [logging] (>=2.0.0) in the cfg file. "
        "Kindly provide an appropriate directory path."
    )

if ENABLE_DELETE_CHILD_LOG.lower() == "true":
    try:
        CHILD_PROCESS_LOG_DIRECTORY = conf.get(
            "scheduler", "CHILD_PROCESS_LOG_DIRECTORY"
        )
        if CHILD_PROCESS_LOG_DIRECTORY != ' ':
            DIRECTORIES_TO_DELETE.append(CHILD_PROCESS_LOG_DIRECTORY)
    except Exception as e:
        logging.exception(
            f"Could not obtain CHILD_PROCESS_LOG_DIRECTORY from Airflow configurations: {e}"
        )

default_args_dict = {
    'depends_on_past': False,
    'start_date': START_DATE,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

doc_md_DAG = """
## Airflow Log Cleanup DAG: `airflow-log-cleanup`

**Purpose:** Periodically cleans Airflow task and child process logs to manage disk space.

**How it Works:**
Deletes log files older than `MAX_LOG_AGE_IN_DAYS` and then removes empty log directories. Uses a lock file (`{LOG_CLEANUP_PROCESS_LOCK_FILE}`) to prevent concurrent runs on the same worker for the same directory.

**Triggering:**
- **Via DAG Run Configuration (Manual Trigger):**
    You can specify `maxLogAgeInDays` when triggering the DAG manually:
    ```bash
    airflow trigger_dag airflow-log-cleanup --conf '{"maxLogAgeInDays": 15}'  
    ```
    Replace `15` with your desired retention period.

- **Via Airflow Variable (Default):**
    If not specified in the DAG run configuration, the DAG will use the value from the Airflow Variable `airflow_log_cleanup__max_log_age_in_days`.
    The default value for this variable (if the Airflow Variable itself is not set) is `30` days, as defined in the DAG script.

- **Scheduled:** Runs daily (`SCHEDULE_INTERVAL`).

**Key Configuration Parameters:**

*   **`maxLogAgeInDays` (Manual Conf):** Overrides default log retention period.
*   **`airflow_log_cleanup__max_log_age_in_days` (Airflow Variable):** Default log retention (days). Default: `30`.
*   **`airflow_log_cleanup__enable_delete` (Airflow Variable):** `True` to delete, `False` for dry-run. Default: `True`.
*   **`NUMBER_OF_WORKERS` (DAG File Global):** Number of worker nodes. Default: `1`.
*   **`ENABLE_DELETE_CHILD_LOG` (Airflow Variable):** `"True"` to clean child process logs. Default: `"False"`.
*   **`BASE_LOG_FOLDER` (Airflow Cfg):** Main task log directory (auto-detected).
*   **`CHILD_PROCESS_LOG_DIRECTORY` (Airflow Cfg):** Child process log directory (auto-detected, used if `ENABLE_DELETE_CHILD_LOG` is true).

**Paths Managed:**
Log directories listed in `DIRECTORIES_TO_DELETE` (initially `BASE_LOG_FOLDER`, potentially `CHILD_PROCESS_LOG_DIRECTORY`).

**Important Considerations:**
*   **Permissions:** Airflow user needs r/w/d access to log dirs and lock file path.
*   **Lock File:** If a run fails, `{LOG_CLEANUP_PROCESS_LOCK_FILE}` might need manual deletion.
*   **`find -mtime +N`:** Targets files modified over N*24 hours ago.

Essential for maintaining a healthy Airflow environment.
"""

@task
def log_cleanup_task(directory: str, sleep_time: int, max_log_age_in_days_override: int = None):
    """
    Executes a shell script to find and delete old log files and empty directories.
    Uses a lock file to prevent concurrent execution on the same worker for the same directory.
    """
    import subprocess

    # Retrieve dag_run conf if available, otherwise use default
    # This part needs careful handling as dag_run.conf is not directly accessible in @task like in BashOperator
    # We'll pass max_log_age_in_days_override which can be set from dag_run.conf in the @dag function
    
    max_log_age = max_log_age_in_days_override if max_log_age_in_days_override is not None else DEFAULT_MAX_LOG_AGE_IN_DAYS
    enable_delete_str = "true" if ENABLE_DELETE else "false"

    log_cleanup_script = f"""
    echo "Getting configurations..."
    BASE_LOG_FOLDER="{directory}"
    WORKER_SLEEP_TIME="{sleep_time}"

    sleep ${{WORKER_SLEEP_TIME}}s

    MAX_LOG_AGE_IN_DAYS="{max_log_age}"
    # If maxLogAgeInDays was passed via conf, it's already in MAX_LOG_AGE_IN_DAYS
    # The original script's logic for checking dag_run.conf is handled by passing max_log_age_in_days_override

    ENABLE_DELETE="{enable_delete_str}"
    echo "Finished getting configurations"
    echo ""

    echo "Configurations:"
    echo "BASE_LOG_FOLDER:      '${{BASE_LOG_FOLDER}}'"
    echo "MAX_LOG_AGE_IN_DAYS:  '${{MAX_LOG_AGE_IN_DAYS}}'"
    echo "ENABLE_DELETE:        '${{ENABLE_DELETE}}'"

    cleanup() {{
        echo "Executing find statement: $1"
        FILES_MARKED_FOR_DELETE=$(eval "$1") # Use $() for command substitution
        echo "Process will be deleting the following file(s)/directory(s):"
        echo "${{FILES_MARKED_FOR_DELETE}}"
        echo "Process will be deleting $(echo "${{FILES_MARKED_FOR_DELETE}}" | grep -v '^$' | wc -l) file(s)/directory(s)"
        echo ""
        if [ "${{ENABLE_DELETE}}" = "true" ];
        then
            if [ "${{FILES_MARKED_FOR_DELETE}}" != "" ];
            then
                echo "Executing delete statement: $2"
                eval "$2"
                DELETE_STMT_EXIT_CODE=$?
                if [ "${{DELETE_STMT_EXIT_CODE}}" != "0" ]; then
                    echo "Delete process failed with exit code '${{DELETE_STMT_EXIT_CODE}}'"
                    echo "Removing lock file..."
                    rm -f "{LOG_CLEANUP_PROCESS_LOCK_FILE}"
                    REMOVE_LOCK_FILE_EXIT_CODE=$? # Corrected variable name
                    if [ "${{REMOVE_LOCK_FILE_EXIT_CODE}}" != "0" ]; then # Corrected variable name
                        echo "Error removing the lock file. Check file permissions. To re-run the DAG, ensure that the lock file has been deleted ({LOG_CLEANUP_PROCESS_LOCK_FILE})."
                        exit ${{REMOVE_LOCK_FILE_EXIT_CODE}} # Corrected variable name
                    fi
                    exit ${{DELETE_STMT_EXIT_CODE}}
                fi
            else
                echo "WARN: No file(s)/directory(s) to delete"
            fi
        else
            echo "WARN: Deletion is disabled (ENABLE_DELETE is not 'true'). Skipping delete operation."
        fi
    }}

    if [ ! -f "{LOG_CLEANUP_PROCESS_LOCK_FILE}" ]; then
        echo "Lock file not found on this node! Creating it to prevent collisions..."
        touch "{LOG_CLEANUP_PROCESS_LOCK_FILE}"
        CREATE_LOCK_FILE_EXIT_CODE=$?
        if [ "${{CREATE_LOCK_FILE_EXIT_CODE}}" != "0" ]; then
            echo "Error creating the lock file. Check if the airflow user can create files under tmp directory. Exiting..."
            exit ${{CREATE_LOCK_FILE_EXIT_CODE}}
        fi

        echo ""
        echo "Running cleanup process..."

        # Cleanup files (e.g., BASE_LOG_FOLDER/dag_id/task_id/file.log - depth 3)
        # Quote '${{BASE_LOG_FOLDER}}' to handle potential spaces/special chars in path
        # Use -mindepth / -maxdepth to control traversal depth
        FIND_STATEMENT_FILES="find '${{BASE_LOG_FOLDER}}' -mindepth 3 -type f -mtime +${{MAX_LOG_AGE_IN_DAYS}}"
        DELETE_STMT_FILES="${{FIND_STATEMENT_FILES}} -exec rm -f {{}} \\;"
        cleanup "${{FIND_STATEMENT_FILES}}" "${{DELETE_STMT_FILES}}"

        # Cleanup empty directories at depth 2 (e.g., BASE_LOG_FOLDER/dag_id/task_id/ where task_id is empty)
        FIND_STATEMENT_EMPTY_DIRS_D2="find '${{BASE_LOG_FOLDER}}' -mindepth 2 -maxdepth 2 -type d -empty -mtime +${{MAX_LOG_AGE_IN_DAYS}}"
        DELETE_STMT_EMPTY_DIRS_D2="${{FIND_STATEMENT_EMPTY_DIRS_D2}} -prune -exec rm -rf {{}} \\;"
        cleanup "${{FIND_STATEMENT_EMPTY_DIRS_D2}}" "${{DELETE_STMT_EMPTY_DIRS_D2}}"

        # Cleanup empty directories at depth 1 (e.g., BASE_LOG_FOLDER/dag_id/ where dag_id is empty)
        FIND_STATEMENT_EMPTY_DIRS_D1="find '${{BASE_LOG_FOLDER}}' -mindepth 1 -maxdepth 1 -type d -empty -mtime +${{MAX_LOG_AGE_IN_DAYS}}"
        DELETE_STMT_EMPTY_DIRS_D1="${{FIND_STATEMENT_EMPTY_DIRS_D1}} -prune -exec rm -rf {{}} \\;"
        cleanup "${{FIND_STATEMENT_EMPTY_DIRS_D1}}" "${{DELETE_STMT_EMPTY_DIRS_D1}}"

        echo "Finished running cleanup process"

        echo "Deleting lock file..."
        rm -f "{LOG_CLEANUP_PROCESS_LOCK_FILE}"
        REMOVE_LOCK_FILE_EXIT_CODE=$?
        if [ "${{REMOVE_LOCK_FILE_EXIT_CODE}}" != "0" ]; then
            echo "Error removing the lock file. Check file permissions. To re-run the DAG, ensure that the lock file has been deleted ({LOG_CLEANUP_PROCESS_LOCK_FILE})."
            exit ${{REMOVE_LOCK_FILE_EXIT_CODE}}
        fi
    else
        echo "Lock file found. Another cleanup task may be running on this worker. Skipping execution."
        echo "If you believe this is an error, please check for and remove the lock file: {LOG_CLEANUP_PROCESS_LOCK_FILE}"
        exit 0
    fi
    """
    # Execute the script
    process = subprocess.Popen(log_cleanup_script, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    stdout, stderr = process.communicate()

    if process.returncode != 0:
        logging.error(f"Log cleanup script failed for directory {directory}")
        logging.error("Stdout: " + stdout)
        logging.error("Stderr: " + stderr)
        raise Exception(f"Log cleanup script failed with return code {process.returncode}. Error: {stderr}")
    else:
        logging.info(f"Log cleanup script succeeded for directory {directory}")
        logging.info("Stdout: " + stdout)
        if stderr:
            logging.warning("Stderr: " + stderr)


@dag(
    dag_id=DAG_ID,
    default_args=default_args_dict,
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=START_DATE,
    catchup=False,
    doc_md=doc_md_DAG
)
def airflow_log_cleanup_dag(**kwargs): # kwargs will contain dag_run and conf
    """
    Airflow Log Cleanup DAG.
    Periodically cleans Airflow task and child process log files.
    """
    
    # Get maxLogAgeInDays from dag_run.conf if provided
    dag_run = kwargs.get('dag_run')
    max_log_age_override = None
    if dag_run and dag_run.conf and 'maxLogAgeInDays' in dag_run.conf:
        try:
            max_log_age_override = int(dag_run.conf['maxLogAgeInDays'])
            logging.info(f"Using maxLogAgeInDays from dag_run.conf: {max_log_age_override}")
        except ValueError:
            logging.warning(f"Invalid value for maxLogAgeInDays in conf: {dag_run.conf['maxLogAgeInDays']}. Using default.")

    for i in range(1, NUMBER_OF_WORKERS + 1):
        for dir_idx, directory_to_clean in enumerate(DIRECTORIES_TO_DELETE):
            # Task ID will be generated automatically, or can be set with .override(task_id=...)
            # Pass the override value to the task
            log_cleanup_task.override(task_id=f'log_cleanup_worker_num_{i}_dir_{dir_idx}')(
                directory=directory_to_clean,
                sleep_time=i * 3,
                max_log_age_in_days_override=max_log_age_override
            )

# Instantiate the DAG
airflow_log_cleanup_dag_instance = airflow_log_cleanup_dag()
