# Databricks notebook source
"""
Job Generator for Lightweight Validation Framework

This notebook:
1. Reads configuration from 03_configure_jobs.py  
2. Shows preview of jobs to be created
3. Checks for existing jobs
4. Creates jobs using Databricks SDK

CRITICAL: Job naming convention is {JOB_NAME_PREFIX}_{table_group}
         Configure JOB_NAME_PREFIX in deltarecon.config.constants

PREREQUISITE: Edit 03_configure_jobs.py first to configure your jobs!
"""

# COMMAND ----------

# MAGIC %run ./03_configure_jobs

# COMMAND ----------

# DBTITLE 1,Setup
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

# Initialize Workspace Client
w = WorkspaceClient()

print("JOB GENERATOR - Validation Framework")

# COMMAND ----------

# DBTITLE 1,Get Workspace Path for main.py
# Get the current notebook path and construct path to main.py
# Current path is: .../files/notebooks/setup/04_generate_jobs
# Main path should be: .../files/notebooks/main
current_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
workspace_root = "/".join(current_path.split("/")[:-2])  # Go up from /notebooks/setup to /notebooks
main_notebook_path = f"{workspace_root}/main"

print(f"Main notebook path: {main_notebook_path}")

# COMMAND ----------

# DBTITLE 1,Preview Jobs
print("PREVIEW: Jobs to be created")

for table_group, config in TABLE_GROUP_CONFIGS.items():
    job_name = get_job_name(table_group)
    cluster_profile = config["cluster_profile"]
    schedule = config.get("schedule", "Manual")
    
    print(f"  {job_name}: {table_group} | {cluster_profile} | {schedule}")

print(f"\nTotal jobs to create: {len(TABLE_GROUP_CONFIGS)}")

# COMMAND ----------

# DBTITLE 1,Check for Existing Jobs
print("CHECKING FOR EXISTING JOBS")

# Get all jobs and check for framework tag
all_jobs = w.jobs.list()
existing_jobs = {}

for job in all_jobs:
    # Get full job details to check tags
    try:
        job_details = w.jobs.get(job.job_id)
        tags = job_details.settings.tags if job_details.settings.tags else {}
        
        # Check if this is a framework-managed job (using managed_by tag)
        if tags.get("managed_by") == "deltarecon":
            job_name = job_details.settings.name
            existing_jobs[job_name] = job.job_id
            table_group = tags.get("table_group", "N/A")
            print(f"  Found existing: {job_name} (ID: {job.job_id}, Group: {table_group})")
    except Exception as e:
        # Skip jobs we can't access
        continue

if existing_jobs:
    print(f"\nERROR: {len(existing_jobs)} existing framework job(s) found!")
    print("Please use 05_manage_jobs.py to delete existing jobs before regenerating.")
    dbutils.notebook.exit("ERROR: Existing jobs found. Please delete them first.")
else:
    print("No existing framework jobs found. Safe to proceed.")

# COMMAND ----------

# DBTITLE 1,Confirmation
print(f"CONFIRMATION REQUIRED: You are about to create {len(TABLE_GROUP_CONFIGS)} validation jobs.")
print("To proceed, uncomment the line in the next cell and run it.")

# COMMAND ----------

# Uncomment the line below to confirm job creation
# PROCEED_WITH_CREATION = True

# COMMAND ----------

# DBTITLE 1,Helper Function to Build Job Configuration
def build_job_config(table_group: str, config: dict):
    """
    Build job configuration for Databricks SDK
    
    Args:
        table_group: Table group name
        config: Configuration from TABLE_GROUP_CONFIGS
    
    Returns:
        Job settings for SDK
    """
    from databricks.sdk.service.compute import ClusterSpec, Library
    
    job_name = get_job_name(table_group)
    cluster_profile = CLUSTER_PROFILES[config["cluster_profile"]]
    
    # Build new cluster spec with wheel library
    new_cluster = ClusterSpec(
        spark_version=cluster_profile["spark_version"],
        node_type_id=cluster_profile["node_type_id"],
        num_workers=cluster_profile.get("num_workers", 2),
        spark_conf=cluster_profile.get("spark_conf", {}),
        custom_tags=cluster_profile.get("custom_tags", {})
    )
    
    # No libraries needed - deltarecon package is synced directly via DAB
    libraries = []
    
    # Build task
    task = jobs.Task(
        task_key="validation_task",
        notebook_task=jobs.NotebookTask(
            notebook_path=main_notebook_path,
            base_parameters={
                "table_group": table_group,
                "iteration_suffix": config["iteration_suffix"]
            }
        ),
        new_cluster=new_cluster,
        libraries=libraries,
        timeout_seconds=config.get("timeout_seconds", GLOBAL_JOB_SETTINGS["timeout_seconds"]),
        max_retries=config.get("max_retries", GLOBAL_JOB_SETTINGS["max_retries"]),
        min_retry_interval_millis=GLOBAL_JOB_SETTINGS["min_retry_interval_millis"],
        retry_on_timeout=GLOBAL_JOB_SETTINGS["retry_on_timeout"]
    )
    
    # Build schedule if configured
    schedule = None
    if config.get("schedule"):
        schedule = jobs.CronSchedule(
            quartz_cron_expression=config["schedule"],
            timezone_id="UTC",
            pause_status=jobs.PauseStatus.UNPAUSED
        )
    
    # Build job tags for management
    job_tags = {
        "managed_by": "deltarecon",
        "table_group": table_group
    }
    
    # Build job settings
    job_settings = jobs.JobSettings(
        name=job_name,
        tasks=[task],
        max_concurrent_runs=GLOBAL_JOB_SETTINGS["max_concurrent_runs"],
        format=jobs.Format.MULTI_TASK,
        tags=job_tags
    )
    
    if schedule:
        job_settings.schedule = schedule
    
    # Add email notifications if configured
    if GLOBAL_JOB_SETTINGS["email_notifications"]["on_failure"]:
        job_settings.email_notifications = jobs.JobEmailNotifications(
            on_failure=GLOBAL_JOB_SETTINGS["email_notifications"]["on_failure"],
            on_success=GLOBAL_JOB_SETTINGS["email_notifications"]["on_success"]
        )
    
    return job_settings

# COMMAND ----------

# DBTITLE 1,Create Jobs
if 'PROCEED_WITH_CREATION' not in locals() or not PROCEED_WITH_CREATION:
    print("Job creation not confirmed. Exiting.")
    print("Please uncomment PROCEED_WITH_CREATION = True in the previous cell.")
    dbutils.notebook.exit("Job creation not confirmed")

print("CREATING JOBS")

created_jobs = []
failed_jobs = []

for table_group, config in TABLE_GROUP_CONFIGS.items():
    job_name = get_job_name(table_group)
    
    try:
        print(f"Creating job: {job_name}...")
        
        # Build job settings
        job_settings = build_job_config(table_group, config)
        
        # Create job - pass parameters directly
        created_job = w.jobs.create(
            name=job_settings.name,
            tasks=job_settings.tasks,
            schedule=job_settings.schedule,
            max_concurrent_runs=job_settings.max_concurrent_runs,
            email_notifications=job_settings.email_notifications,
            format=job_settings.format,
            tags=job_settings.tags
        )
        
        created_jobs.append({
            "job_name": job_name,
            "job_id": created_job.job_id,
            "table_group": table_group
        })
        
        print(f"Created: {job_name} (ID: {created_job.job_id})")
        
    except Exception as e:
        failed_jobs.append({
            "job_name": job_name,
            "error": str(e)
        })
        print(f"Failed to create {job_name}: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Summary
print("\nJOB CREATION SUMMARY")

print(f"Total jobs requested: {len(TABLE_GROUP_CONFIGS)}")
print(f"Successfully created: {len(created_jobs)}")
print(f"Failed: {len(failed_jobs)}")

if created_jobs:
    print("\nSuccessfully created jobs:")
    for job in created_jobs:
        print(f"  - {job['job_name']:30s} (ID: {job['job_id']})")

if failed_jobs:
    print("\nFailed jobs:")
    for job in failed_jobs:
        print(f"  - {job['job_name']:30s} Error: {job['error']}")
    print("\nSome jobs failed to create. Please review errors above.")
else:
    print("\nAll jobs created successfully!")
    print("Next step: Use 05_manage_jobs.py to manage these jobs")

# COMMAND ----------

# DBTITLE 1,Store Job IDs for Management
# Store created job IDs in a temporary table for easy management
if created_jobs:
    import pandas as pd
    from datetime import datetime
    
    df = pd.DataFrame(created_jobs)
    df['created_at'] = datetime.now()
    
    spark_df = spark.createDataFrame(df)
    
    # Store in temp view
    spark_df.createOrReplaceTempView("validation_jobs_created")
    
    print("\nJob IDs stored in temporary view: validation_jobs_created")
    print("You can query this view to see all created jobs.")
    
    display(spark_df)
