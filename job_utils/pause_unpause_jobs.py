# Databricks notebook source
# MAGIC %md
# MAGIC # Pause/Unpause Validation Jobs
# MAGIC Bulk management of validation jobs

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text("job_name_contains", "validation_")
dbutils.widgets.dropdown("pause_schedule", "pause", ["pause", "resume"])
dbutils.widgets.dropdown("cancel_all_runs", "N", ["Y", "N"])
dbutils.widgets.dropdown("cancel_queued_runs", "N", ["Y", "N"])
dbutils.widgets.dropdown("dry_run", "Y", ["Y", "N"])

# COMMAND ----------

# DBTITLE 1,Imports
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
from databricks.sdk.service.jobs import JobSettings
import logging
from typing import List, Optional

# COMMAND ----------

# DBTITLE 1,Helper Functions
def _str_null_blanks(inp_str):
    if not inp_str or str(inp_str).strip() == '':
        return None
    else:
        return str(inp_str).strip()

def _get_bool(inp_str, default=False):
    inp_str = _str_null_blanks(inp_str)
    if not inp_str:
        return default
    inp_str = inp_str.upper()
    if inp_str in ('Y', 'YES', 'T', 'TRUE'):
        return True
    elif inp_str in ('N', 'NO', 'F', 'FALSE'):
        return False

# COMMAND ----------

# DBTITLE 1,Get Parameters
job_name_contains = _str_null_blanks(dbutils.widgets.get("job_name_contains"))
pause_schedule = _str_null_blanks(dbutils.widgets.get("pause_schedule"))
cancel_all_runs = _get_bool(dbutils.widgets.get("cancel_all_runs"))
cancel_queued_runs = _get_bool(dbutils.widgets.get("cancel_queued_runs"))
dry_run = _get_bool(dbutils.widgets.get("dry_run"))

w = WorkspaceClient()

print("="*80)
print("VALIDATION JOB MANAGEMENT - Pause/Unpause & Cancel Runs")
print("="*80)
print(f"job_name_contains: {job_name_contains}")
print(f"pause_schedule: {pause_schedule}")
print(f"cancel_all_runs: {cancel_all_runs}")
print(f"cancel_queued_runs: {cancel_queued_runs}")
print(f"dry_run: {dry_run}")

# COMMAND ----------

# DBTITLE 1,Cancel Runs Function
def cancel_runs(job_id, cancel_all_runs: bool = True, cancel_queued_runs: bool = True, dry_run=False):
    """Cancel active or queued runs for a job"""
    l_runs = list(w.jobs.list_runs(job_id=job_id, active_only=True))
    if not l_runs:
        print(f"  {job_id}: No active runs found")
        return
    
    if cancel_all_runs:
        pass
    elif cancel_queued_runs:
        l_runs = [x for x in l_runs if x.state.life_cycle_state in (
            jobs.RunLifeCycleState.QUEUED,
            jobs.RunLifeCycleState.PENDING
        )]
        if not l_runs:
            print(f"  {job_id}: No queued runs found")
            return
    
    for x in l_runs:
        if not dry_run:
            w.jobs.cancel_run(run_id=x.run_id)
        print(f"  {job_id}: {'[DRY RUN] Would cancel' if dry_run else 'Cancelled'} run {x.run_id}, state: {x.state.life_cycle_state}")

# COMMAND ----------

# DBTITLE 1,Process Jobs Function
def process(l_jobs, pause_schedule, cancel_all_runs, cancel_queued_runs, dry_run):
    """Process jobs for pause/resume and run cancellation"""
    required_status = jobs.PauseStatus.PAUSED if pause_schedule == 'pause' else jobs.PauseStatus.UNPAUSED
    
    for x in l_jobs:
        schedule = x.settings.schedule
        if schedule:
            job_pause_status = schedule.pause_status
            if job_pause_status != required_status:
                if not dry_run:
                    x.settings.schedule.pause_status = required_status
                    res = w.jobs.update(job_id=x.job_id, new_settings=JobSettings(schedule=x.settings.schedule))
                status_msg = '[DRY RUN] Would update' if dry_run else 'Updated'
                print(f"  {status_msg}: {x.settings.name}:{x.job_id} pause status to {required_status}")
        
        if cancel_queued_runs or cancel_all_runs:
            print(f"  {x.settings.name}::{x.job_id}: Checking runs...")
            cancel_runs(x.job_id, cancel_all_runs, cancel_queued_runs, dry_run)

# COMMAND ----------

# DBTITLE 1,Get Matching Jobs
print("\n" + "="*80)
print("FINDING JOBS")
print("="*80)

l_jobs = [x for x in w.jobs.list()]
if job_name_contains:
    l_jobs = [x for x in l_jobs if job_name_contains in x.settings.name]
    print(f"Filtering jobs containing: '{job_name_contains}'")
else:
    l_jobs = [x for x in l_jobs if x.settings.name.startswith("validation_")]
    print("Filtering jobs starting with: 'validation_'")

print(f"\nFound {len(l_jobs)} matching job(s):")
for job in l_jobs:
    schedule_info = "Manual"
    if job.settings.schedule:
        schedule_info = f"{job.settings.schedule.quartz_cron_expression}"
        if job.settings.schedule.pause_status == jobs.PauseStatus.PAUSED:
            schedule_info += " (PAUSED)"
    print(f"  - {job.settings.name} (ID: {job.job_id}) | Schedule: {schedule_info}")

# COMMAND ----------

# DBTITLE 1,Process Jobs
if len(l_jobs) == 0:
    print("\nNo jobs to process. Exiting.")
else:
    print("\n" + "="*80)
    print("PROCESSING JOBS")
    print("="*80)
    if dry_run:
        print("[DRY RUN MODE - No changes will be made]\n")
    
    process(l_jobs, pause_schedule, cancel_all_runs, cancel_queued_runs, dry_run)
    
    print("\n" + "="*80)
    print("COMPLETE")
    print("="*80)
    if dry_run:
        print("This was a DRY RUN. To apply changes, set dry_run='N' and re-run.")

