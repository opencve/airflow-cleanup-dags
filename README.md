# Airflow Cleanup DAGs

This repository contains a collection of Airflow DAGs designed to perform essential cleanup tasks for your Airflow environment. These DAGs help manage disk space and database size by removing old logs and metadata, ensuring your Airflow instance remains performant and stable.

These DAGs are actively used within the OpenCVE project to manage its Airflow scheduler's cleanup tasks.

## DAGs Included

1.  [Airflow Database Cleanup (`airflow-db-cleanup`)](#airflow-database-cleanup-airflow-db-cleanup)
2.  [Airflow Log Cleanup (`airflow-log-cleanup`)](#airflow-log-cleanup-airflow-log-cleanup)

## Prerequisites

*   **Airflow:** These DAGs are designed for Apache Airflow. They have been tested on Airflow 2.8 and 2.10. While they leverage common Airflow models and configurations and may work with other 2.x versions, compatibility outside of the tested versions is not guaranteed.
*   **Python:** Python 3.x.
*   **Database Backend:** A supported Airflow metadata database (e.g., PostgreSQL, MySQL).
*   **Celery Executor (Optional):** If you use the CeleryExecutor, the `airflow-db-cleanup` DAG can also clean Celery-specific tables (`task` and `taskset`).
*   **Airflow Variables:** These DAGs rely on Airflow Variables for configuration. Ensure you can set these in your Airflow UI or via the CLI.

## Setup and Installation

1.  **Clone the repository or download the DAG files**
2.  **Place DAG files in your Airflow DAGs folder:**
    Copy `airflow-db-cleanup.py` and `airflow-log-cleanup.py` into the DAGs folder specified in your `airflow.cfg` (typically `dags_folder`). For OpenCVE, the default path is `scheduler/dags/`.
3.  **Configure Airflow Variables:**
    Set the required and optional Airflow Variables as described in the sections for each DAG below. You can do this via the Airflow UI (Admin -> Variables) or using the Airflow CLI.

    **Important Defaults & Recommendation:**
    *   By default, both DAGs have **deletion enabled** if the specific `airflow_...__enable_delete` variable is not explicitly set to `"false"`.
    *   The default retention period (`max_db_entry_age_in_days` for DB cleanup and `max_log_age_in_days` for log cleanup) is **30 days** if not otherwise configured via DAG run parameters or specific Airflow Variables.
    *   **It is advised to set the `airflow_db_cleanup__enable_delete` and `airflow_log_cleanup__enable_delete` variables to `"false"` *before* unpausing the DAGs for the first time.** This will allow you to perform an initial dry run and verify the cleanup actions.

    Example for setting variables (ensure you set `enable_delete` to `"false"` initially):
    ```bash
    airflow variables set airflow_db_cleanup__max_db_entry_age_in_days 30
    airflow variables set airflow_db_cleanup__enable_delete "false" 
    airflow variables set airflow_log_cleanup__max_log_age_in_days 30
    airflow variables set airflow_log_cleanup__enable_delete "false"
    ```
4.  **Enable the DAGs in the Airflow UI:**
    Once Airflow picks up the new DAGs (they will be paused by default), review their configuration and the Airflow Variables you have set. Once you are comfortable with the settings (especially after an initial dry run with `enable_delete` set to `"false"`), you can unpause them in the UI.

## Airflow Database Cleanup (`airflow-db-cleanup`)

**Purpose:**
Performs periodic cleanup of the Airflow MetaStore to prevent excessive data accumulation and maintain database performance. It removes old records from various metadata tables.

**Tables Cleaned:**
*   `DagRun`
*   `TaskInstance`
*   `Log`
*   `XCom`
*   `BaseJob` (Job table, e.g., `job` or `base_job` depending on Airflow version)
*   `SlaMiss`
*   `DagModel` (and associated `DagTag` entries)
*   `TaskReschedule` (if available in your Airflow version)
*   `TaskFail` (if available in your Airflow version)
*   `ImportError` (if available in your Airflow version)
*   Celery `Task` and `TaskSet` tables (if CeleryExecutor is configured and these tables are present)

**Key Features:**
*   **Configurable Retention Period:** Define how long to keep records.
*   **Safety Switch:** `ENABLE_DELETE` flag allows for dry runs.
*   **Version Adaptability:** Attempts to dynamically determine the correct column for age checks in `DagModel` across different Airflow versions.
*   **Selective Keep Last:** For `DagRun` records, it keeps the most recent run for each DAG (specifically for non-externally triggered runs) while cleaning up older ones.
*   **Column Validation:** Includes a preliminary task to validate that the configured `age_check_column_name` exists for each database object, preventing errors during cleanup.

**Configuration:**

*   **Via DAG Run Configuration (Manual Trigger):**
    You can specify `maxDBEntryAgeInDays` when triggering the DAG manually:
    ```json
    {
      "maxDBEntryAgeInDays": 30
    }
    ```
    Example CLI command:
    ```bash
    airflow dags trigger airflow-db-cleanup --conf '{"maxDBEntryAgeInDays": 30}'
    ```

*   **Via Airflow Variables (Default Behavior):**
    *   `airflow_db_cleanup__max_db_entry_age_in_days`: (Integer) The maximum age (in days) of records to keep. Records older than this will be targeted for deletion.
        *   **Default (if variable not set):** `30`
    *   `airflow_db_cleanup__enable_delete`: (String: `"true"` or `"false"`) **CRITICAL SAFETY SWITCH.**
        *   Set to `"true"` to enable actual deletion of database records.
        *   Set to `"false"` to perform a "dry run" (logs what would be deleted without making changes).
        *   **Default (if variable not set):** `"true"` (Note: the DAG script has a default of `False` in its global `ENABLE_DELETE`, but this variable, if present, overrides it. The internal script default `ENABLE_DELETE_STR` variable points to the `airflow_db_cleanup__enable_delete` Airflow Variable, defaulting that to `"true"` if the Airflow Var is missing. Please verify your settings carefully).

**Scheduling:**
*   **Default:** Runs daily (`@daily`). This can be adjusted by modifying the `SCHEDULE_INTERVAL` variable within the `airflow-db-cleanup.py` script.

**Important Notes:**
*   **Review `ENABLE_DELETE`:** Always double-check the `airflow_db_cleanup__enable_delete` variable before enabling actual deletion, especially in production environments. Start with a dry run (`"false"`) to verify the records targeted for deletion.
*   **Database Performance:** Running this DAG might put a temporary load on your database. Schedule it during off-peak hours if possible.
*   **Backup:** Always ensure you have a recent backup of your Airflow metadata database before running any cleanup operations, especially for the first time or after significant configuration changes.

## Airflow Log Cleanup (`airflow-log-cleanup`)

**Purpose:**
Periodically cleans Airflow task and child process logs from the filesystem to manage disk space.

**How it Works:**
The DAG deletes log files older than a specified number of days (`maxLogAgeInDays`). After deleting files, it attempts to remove empty log directories. It uses a lock file (`/tmp/airflow_log_cleanup_worker.lock`) for each directory being cleaned to prevent concurrent cleanup operations on the same worker node for that specific directory, which is particularly useful if `NUMBER_OF_WORKERS` is greater than 1 or if the DAG runs frequently.

**Key Features:**
*   **Configurable Log Retention:** Specify how long to keep log files.
*   **Safety Switch:** `ENABLE_DELETE` flag allows for dry runs.
*   **Child Process Log Cleaning:** Optionally clean child process logs generated by the scheduler.
*   **Multi-Worker Support:** Can be configured to simulate cleanup across multiple worker nodes by staggering tasks.
*   **Empty Directory Removal:** Cleans up directories that become empty after log file deletion.

**Configuration:**

*   **Via DAG Run Configuration (Manual Trigger):**
    You can specify `maxLogAgeInDays` when triggering the DAG manually:
    ```json
    {
      "maxLogAgeInDays": 15
    }
    ```
    Example CLI command:
    ```bash
    airflow dags trigger airflow-log-cleanup --conf '{"maxLogAgeInDays": 15}'
    ```

*   **Via Airflow Variables (Default Behavior):**
    *   `airflow_log_cleanup__max_log_age_in_days`: (Integer) The maximum age (in days) of log files to keep. Files older than this will be targeted for deletion.
        *   **Default (if variable not set):** `30`
    *   `airflow_log_cleanup__enable_delete`: (String: `"true"` or `"false"`) Controls whether log files are actually deleted or if the DAG performs a dry run.
        *   **Default (if variable not set):** `"true"`
    *   `airflow_log_cleanup__enable_delete_child_log`: (String: `"true"` or `"false"`) Set to `"true"` to also clean logs from the `CHILD_PROCESS_LOG_DIRECTORY` (configured in `airflow.cfg`).
        *   **Default (if variable not set):** `"False"`

*   **DAG Script Variables:**
    *   `NUMBER_OF_WORKERS`: (Integer) The number of worker nodes you have in Airflow. The DAG will create tasks staggered by a small sleep time for each worker "simulation" to help distribute the cleanup if logs are on shared storage accessible by multiple workers, or if you want to ensure each worker node gets a chance to clear its local logs (though the script itself runs on one worker, the lock file mechanism is per-directory). **Default:** `1`.
    *   `LOG_CLEANUP_PROCESS_LOCK_FILE`: (String) Path to the lock file. **Default:** `"/tmp/airflow_log_cleanup_worker.lock"`.

*   **Airflow Configuration (`airflow.cfg`):**
    *   `BASE_LOG_FOLDER`: (Under `[logging]` section) The primary directory where task logs are stored.
    *   `CHILD_PROCESS_LOG_DIRECTORY`: (Under `[scheduler]` section) The directory for child process logs. Used if `airflow_log_cleanup__enable_delete_child_log` is true.

**Paths Managed:**
The DAG cleans logs from directories listed in its internal `DIRECTORIES_TO_DELETE` list. This list is populated based on:
1.  `BASE_LOG_FOLDER` (always included).
2.  `CHILD_PROCESS_LOG_DIRECTORY` (if `airflow_log_cleanup__enable_delete_child_log` is true and the directory is configured).

**Scheduling:**
*   **Default:** Runs daily (`@daily`). This can be adjusted by modifying the `SCHEDULE_INTERVAL` variable within the `airflow-log-cleanup.py` script.

**Important Notes:**
*   **Permissions:** The user running the Airflow worker/scheduler processes needs read, write, and delete permissions for the log directories and the path where the lock file is created (`/tmp/` by default).
*   **Lock File:** If a DAG run fails while the lock file exists, you might need to manually delete `{LOG_CLEANUP_PROCESS_LOCK_FILE}` (e.g., `/tmp/airflow_log_cleanup_worker.lock`) before the next run can proceed for that specific directory. The DAG attempts to remove the lock file upon completion or error, but critical failures might prevent this.
*   **`find -mtime +N` Behavior:** The cleanup script uses `find ... -mtime +N`, which targets files modified more than `N * 24` hours ago.
*   **Dry Runs:** It's highly recommended to run with `airflow_log_cleanup__enable_delete` set to `"false"` initially to verify which files and directories will be targeted.

## Contributing

Contributions are welcome! If you have suggestions for improvements, new features, or bug fixes, please:

1.  Fork the repository.
2.  Create a new branch for your feature or fix.
3.  Make your changes.
4.  Test your changes thoroughly.
5.  Submit a pull request with a clear description of your changes.

## Acknowledgements

These DAGs were originally based on and modified from the work done in the [teamclairvoyant/airflow-maintenance-dags](https://github.com/teamclairvoyant/airflow-maintenance-dags) repository. We extend our thanks to the original authors for their valuable contributions to the Airflow community.
