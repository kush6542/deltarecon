# Databricks notebook source
"""
Job Management for Lightweight Validation Framework

Utilities to:
- List all validation jobs
- Update job schedules
- Pause/Resume jobs
- Delete jobs

Use this notebook to manage jobs created by 04_generate_jobs.py
"""

# COMMAND ----------

# DBTITLE 1,Setup
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
import pandas as pd

# Initialize Workspace Client
w = WorkspaceClient()

print("JOB MANAGEMENT - Lightweight Validation Framework")

# COMMAND ----------

# DBTITLE 1,List All Validation Jobs
def list_validation_jobs():
    """
    List all validation framework jobs (identified by tags)
    
    Looks for jobs with tag:
    - managed_by = "deltarecon"
    """
    all_jobs = w.jobs.list()
    validation_jobs = []
    
    for job in all_jobs:
        try:
            # Get full job details to check tags
            job_details = w.jobs.get(job.job_id)
            tags = job_details.settings.tags if job_details.settings.tags else {}
            
            # Check if this is a framework-managed job (using managed_by tag)
            if tags.get("managed_by") == "deltarecon":
                job_name = job_details.settings.name
                
                schedule_info = "Manual"
                if job_details.settings.schedule:
                    schedule_info = job_details.settings.schedule.quartz_cron_expression
                    if job_details.settings.schedule.pause_status == jobs.PauseStatus.PAUSED:
                        schedule_info += " (PAUSED)"
                
                # Extract tags
                table_group = tags.get("table_group", "N/A")
                
                validation_jobs.append({
                    "job_id": job.job_id,
                    "job_name": job_name,
                    "table_group": table_group,
                    "schedule": schedule_info,
                    "max_concurrent_runs": job_details.settings.max_concurrent_runs,
                    "created_time": job.created_time
                })
        except Exception as e:
            # Skip jobs we can't access or have issues
            continue
    
    return validation_jobs

validation_jobs = list_validation_jobs()

if not validation_jobs:
    print("No validation jobs found.")
    print("\nNote: Jobs are identified by tag: managed_by = 'deltarecon'")
else:
    print(f"\nFound {len(validation_jobs)} validation job(s) (identified by managed_by tag):\n")
    df = pd.DataFrame(validation_jobs)
    display(df)

# COMMAND ----------

# DBTITLE 1,Get Job Run History
# Select a job to view run history
dbutils.widgets.dropdown("job_name_for_history", "", [""] + [job["job_name"] for job in validation_jobs])

job_name_selected = dbutils.widgets.get("job_name_for_history")

if job_name_selected:
    # Find job ID
    job_id = next((job["job_id"] for job in validation_jobs if job["job_name"] == job_name_selected), None)
    
    if job_id:
        print(f"Run history for: {job_name_selected} (ID: {job_id})\n")
        
        # Get runs
        runs = w.jobs.list_runs(job_id=job_id, limit=10)
        
        run_history = []
        for run in runs.runs:
            run_history.append({
                "run_id": run.run_id,
                "start_time": run.start_time,
                "end_time": run.end_time,
                "state": run.state.life_cycle_state,
                "result_state": run.state.result_state if run.state.result_state else "N/A"
            })
        
        if run_history:
            df_runs = pd.DataFrame(run_history)
            display(df_runs)
        else:
            print("No run history found.")

# COMMAND ----------

# DBTITLE 1,Update Job Schedule
def update_job_schedule(job_name: str, new_schedule: str):
    """
    Update job schedule
    
    Args:
        job_name: Job name (e.g., validation_sales)
        new_schedule: New cron expression (e.g., "0 0 3 * * ?")
    """
    # Find job
    job_id = next((job["job_id"] for job in validation_jobs if job["job_name"] == job_name), None)
    
    if not job_id:
        print(f"Job not found: {job_name}")
        return
    
    # Get current settings
    job_details = w.jobs.get(job_id)
    
    # Update schedule
    w.jobs.update(
        job_id=job_id,
        new_settings=jobs.JobSettings(
            name=job_name,
            tasks=job_details.settings.tasks,
            schedule=jobs.CronSchedule(
                quartz_cron_expression=new_schedule,
                timezone_id="UTC",
                pause_status=jobs.PauseStatus.UNPAUSED
            ),
            max_concurrent_runs=job_details.settings.max_concurrent_runs,
            format=jobs.Format.MULTI_TASK
        )
    )
    
    print(f"Updated schedule for {job_name} to: {new_schedule}")

# Example usage:
# update_job_schedule("validation_sales", "0 0 4 * * ?")  # Daily at 4 AM

# COMMAND ----------

# DBTITLE 1,Pause Job
def pause_job(job_name: str):
    """
    Pause a job
    
    Args:
        job_name: Job name
    """
    job_id = next((job["job_id"] for job in validation_jobs if job["job_name"] == job_name), None)
    
    if not job_id:
        print(f"Job not found: {job_name}")
        return
    
    # Get current settings
    job_details = w.jobs.get(job_id)
    
    if job_details.settings.schedule:
        # Update to paused
        w.jobs.update(
            job_id=job_id,
            new_settings=jobs.JobSettings(
                name=job_name,
                tasks=job_details.settings.tasks,
                schedule=jobs.CronSchedule(
                    quartz_cron_expression=job_details.settings.schedule.quartz_cron_expression,
                    timezone_id="UTC",
                    pause_status=jobs.PauseStatus.PAUSED
                ),
                max_concurrent_runs=job_details.settings.max_concurrent_runs,
                format=jobs.Format.MULTI_TASK
            )
        )
        print(f"Paused job: {job_name}")
    else:
        print(f"Job {job_name} has no schedule (manual trigger only)")

# Example usage:
# pause_job("validation_sales")

# COMMAND ----------

# DBTITLE 1,Resume Job
def resume_job(job_name: str):
    """
    Resume a paused job
    
    Args:
        job_name: Job name
    """
    job_id = next((job["job_id"] for job in validation_jobs if job["job_name"] == job_name), None)
    
    if not job_id:
        print(f"Job not found: {job_name}")
        return
    
    # Get current settings
    job_details = w.jobs.get(job_id)
    
    if job_details.settings.schedule:
        # Update to unpaused
        w.jobs.update(
            job_id=job_id,
            new_settings=jobs.JobSettings(
                name=job_name,
                tasks=job_details.settings.tasks,
                schedule=jobs.CronSchedule(
                    quartz_cron_expression=job_details.settings.schedule.quartz_cron_expression,
                    timezone_id="UTC",
                    pause_status=jobs.PauseStatus.UNPAUSED
                ),
                max_concurrent_runs=job_details.settings.max_concurrent_runs,
                format=jobs.Format.MULTI_TASK
            )
        )
        print(f"Resumed job: {job_name}")
    else:
        print(f"Job {job_name} has no schedule (manual trigger only)")

# Example usage:
# resume_job("validation_sales")

# COMMAND ----------

# DBTITLE 1,Delete Job
def delete_job(job_name: str):
    """
    Delete a job (CAREFUL!)
    
    Args:
        job_name: Job name
    """
    job_id = next((job["job_id"] for job in validation_jobs if job["job_name"] == job_name), None)
    
    if not job_id:
        print(f"Job not found: {job_name}")
        return
    
    w.jobs.delete(job_id)
    print(f"Deleted job: {job_name} (ID: {job_id})")

# Example usage:
# delete_job("validation_sales")

# COMMAND ----------

# DBTITLE 1,Bulk Delete All Validation Jobs
def delete_all_validation_jobs():
    """
    Delete ALL framework-managed validation jobs (VERY CAREFUL!)
    
    Identifies jobs by tag (managed_by="deltarecon").
    Use this to clean up before regenerating jobs.
    """
    validation_jobs_current = list_validation_jobs()
    
    if not validation_jobs_current:
        print("No validation jobs to delete.")
        return
    
    print(f"WARNING: About to delete {len(validation_jobs_current)} job(s):\n")
    for job in validation_jobs_current:
        print(f"  - {job['job_name']} (ID: {job['job_id']})")
    
    print("\nUncomment the confirmation line below and re-run this cell to proceed.")

# Uncomment the line below to confirm deletion
# CONFIRM_DELETE_ALL = True

if 'CONFIRM_DELETE_ALL' in locals() and CONFIRM_DELETE_ALL:
    validation_jobs_to_delete = list_validation_jobs()
    
    for job in validation_jobs_to_delete:
        try:
            w.jobs.delete(job["job_id"])
            print(f"Deleted: {job['job_name']} (ID: {job['job_id']})")
        except Exception as e:
            print(f"Failed to delete {job['job_name']}: {str(e)}")
    
    print(f"\nDeleted {len(validation_jobs_to_delete)} job(s)")

# COMMAND ----------

# DBTITLE 1,Run Job Manually
def run_job_manually(job_name: str):
    """
    Trigger a job run manually
    
    Args:
        job_name: Job name
    """
    job_id = next((job["job_id"] for job in validation_jobs if job["job_name"] == job_name), None)
    
    if not job_id:
        print(f"Job not found: {job_name}")
        return
    
    run = w.jobs.run_now(job_id)
    print(f"Triggered job: {job_name}")
    print(f"  Run ID: {run.run_id}")
    print(f"  Check status in Databricks Jobs UI")

# Example usage:
# run_job_manually("validation_sales")

# COMMAND ----------

# DBTITLE 1,Helper: Pause All Jobs
def pause_all_validation_jobs():
    """Pause all validation jobs"""
    validation_jobs_current = list_validation_jobs()
    
    for job in validation_jobs_current:
        try:
            pause_job(job["job_name"])
        except Exception as e:
            print(f"Failed to pause {job['job_name']}: {str(e)}")

# Uncomment to pause all:
# pause_all_validation_jobs()

# COMMAND ----------

# DBTITLE 1,Helper: Resume All Jobs
def resume_all_validation_jobs():
    """Resume all validation jobs"""
    validation_jobs_current = list_validation_jobs()
    
    for job in validation_jobs_current:
        try:
            resume_job(job["job_name"])
        except Exception as e:
            print(f"Failed to resume {job['job_name']}: {str(e)}")

# Uncomment to resume all:
# resume_all_validation_jobs()

# COMMAND ----------

# DBTITLE 1,Helper: Filter Jobs by Table Group
def get_jobs_by_table_group(table_group: str):
    """
    Get all validation jobs for a specific table group
    
    Args:
        table_group: Table group name (e.g., 'sales')
    
    Returns:
        List of job details for the table group
    """
    validation_jobs_current = list_validation_jobs()
    filtered = [job for job in validation_jobs_current if job.get("table_group") == table_group]
    
    if filtered:
        print(f"Jobs for table_group '{table_group}':")
        df = pd.DataFrame(filtered)
        display(df)
    else:
        print(f"No jobs found for table_group: {table_group}")
    
    return filtered

# Example usage:
# get_jobs_by_table_group("sales")

# COMMAND ----------

print("JOB MANAGEMENT UTILITIES LOADED")
print("\nAvailable functions:")
print("  - list_validation_jobs()")
print("  - get_jobs_by_table_group(table_group)")
print("  - update_job_schedule(job_name, new_schedule)")
print("  - pause_job(job_name)")
print("  - resume_job(job_name)")
print("  - delete_job(job_name)")
print("  - delete_all_validation_jobs()")
print("  - run_job_manually(job_name)")
print("  - pause_all_validation_jobs()")
print("  - resume_all_validation_jobs()")

