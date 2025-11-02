# Databricks notebook source
# MAGIC %md
# MAGIC # Validation Job Deployment
# MAGIC Deploy validation jobs from YAML configuration

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text("config_file_path", "")
dbutils.widgets.dropdown("dry_run", "Y", ["Y", "N"])

# COMMAND ----------

# DBTITLE 1,Imports
from databricks.sdk.service.jobs import JobSettings as Job
from databricks.sdk.service import jobs as jobs_svc
from databricks.sdk.service.jobs import JobAccessControlRequest, JobPermissionLevel
from databricks.sdk.service import iam
from typing import Dict, Any
from databricks.sdk import WorkspaceClient
import yaml
from pprint import pprint
from copy import deepcopy
from pathlib import Path

# COMMAND ----------

# DBTITLE 1,Load Configuration
config_file_path = dbutils.widgets.get("config_file_path").strip()
dry_run = dbutils.widgets.get("dry_run").strip().upper() == "Y"

if config_file_path == '':
    config_file_path = './validation_job_config.yml'

if not Path(config_file_path).exists() or not Path(config_file_path).is_file():
    raise Exception(f"Config file not found at {config_file_path}")

d_config = None
with open(config_file_path) as stream:
    try:
        d_config = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        raise Exception(f"Error parsing YAML config: {exc}")

global_config = {k: v for k, v in d_config.items() if k != 'jobs'}
jobs_config = d_config.get('jobs', [])

print("="*80)
print("VALIDATION JOB DEPLOYMENT")
print("="*80)
print(f"Dry Run: {dry_run}")
print(f"Config File: {config_file_path}")
print(f"Jobs to process: {len(jobs_config)}")
print("\nGlobal Configuration:")
pprint(global_config)

w = WorkspaceClient()

# COMMAND ----------

# DBTITLE 1,Permission Management Functions
def get_job_owner(job_id):
    """Get the owner of a job"""
    response = w.jobs.get_permissions(job_id=job_id)
    l_permissions = response.access_control_list
    owner = None
    owner_type = None
    for acl in l_permissions:
        identity = acl.user_name or acl.group_name or acl.service_principal_name
        id_type = 'user' if acl.user_name else 'group' if acl.group_name else 'service_principal'
        for _perm in acl.all_permissions:
            if _perm.permission_level == JobPermissionLevel.IS_OWNER:
                owner = identity
                owner_type = id_type
                break
    return owner, owner_type

def update_permissions(job_id, d_permissions: Dict[str, str]):
    """Update job permissions"""
    l_acls = []
    owner, owner_type = get_job_owner(job_id)
    owner_key = f"{owner_type}:{owner}"
    
    for access_level, members in d_permissions.items():
        members = [x.strip() for x in members.split(',')] if members else []
        for x in members:
            id_type = 'group'
            identity = x
            _l_x = [y.strip() for y in x.split(':') if y.strip() != '']
            if len(_l_x) > 1:
                id_type = str(_l_x[0]).strip().lower()
                identity = _l_x[1]
            key = f"{id_type}:{identity}"
            if key == owner_key:
                print(f"  Cannot modify owner privileges for {owner_type} {owner}. If owner needs to be changed, contact admin.")
                continue
            
            user_name = identity if id_type == 'user' else None
            group_name = identity if id_type == 'group' else None
            service_principal_name = identity if id_type in ('service_principal', 'spn') else None
            
            required_access_level = (
                JobPermissionLevel.CAN_MANAGE if access_level == 'can_manage'
                else JobPermissionLevel.CAN_MANAGE_RUN if access_level == 'can_manage_run'
                else JobPermissionLevel.CAN_VIEW
            )
            
            acl = JobAccessControlRequest(
                user_name=user_name,
                group_name=group_name,
                service_principal_name=service_principal_name,
                permission_level=required_access_level
            )
            l_acls.append(acl)
    
    if l_acls:
        w.jobs.update_permissions(job_id=job_id, access_control_list=l_acls)
        print(f"  Permissions updated for job {job_id}")

# COMMAND ----------

# DBTITLE 1,Cluster and Task Templates
cluster_template = """
{
  "job_cluster_key": "{cluster_name}",
  "new_cluster": {
      "cluster_name": "",
      "spark_version": "16.4.x-scala2.12",
      "spark_env_vars": {
          "PYSPARK_PYTHON": "/databricks/python3/bin/python3",
      },
      "instance_pool_id": "{instance_pool_id}",
      "policy_id": "{policy_id}",
      "driver_instance_pool_id": "{driver_instance_pool_id}",
      "data_security_mode": "USER_ISOLATION",
      "runtime_engine": "STANDARD",
      "kind": "CLASSIC_PREVIEW",
      "autoscale": {
          "min_workers": {min_workers},
          "max_workers": {max_workers},
      },
  },
}
"""

task_template = """
{
  "task_key": "{job_name}",
  "notebook_task": {
      "notebook_path": "{notebook_path}",
      "source": "WORKSPACE",
  },
  "job_cluster_key": "{cluster_name}",
}
"""

# COMMAND ----------

# DBTITLE 1,Helper Functions
def get_cluster(cluster_name: str,
                instance_pool_id: str,
                policy_id: str,
                driver_instance_pool_id: str,
                min_workers: int,
                max_workers: int,
                custom_tags: Dict[str, str] = None,
                spark_conf: Dict[str, str] = None):
    """Build cluster configuration"""
    cluster = cluster_template.replace("{cluster_name}", cluster_name)
    cluster = cluster.replace("{instance_pool_id}", instance_pool_id)
    cluster = cluster.replace("{policy_id}", policy_id)
    cluster = cluster.replace("{driver_instance_pool_id}", driver_instance_pool_id)
    cluster = cluster.replace("{min_workers}", str(min_workers))
    cluster = cluster.replace("{max_workers}", str(max_workers))
    d_cluster = eval(cluster)
    if custom_tags:
        d_cluster["new_cluster"]["custom_tags"] = custom_tags
    if spark_conf:
        d_cluster["new_cluster"]["spark_conf"] = spark_conf
    return d_cluster

def get_task(job_name: str,
             cluster_name: str,
             notebook_path: str,
             job_parameters: Dict[str, Any]):
    """Build task configuration"""
    task = task_template.replace("{job_name}", job_name)
    task = task.replace("{notebook_path}", notebook_path)
    task = task.replace("{cluster_name}", cluster_name)
    d_task = eval(task)
    if job_parameters:
        d_task["notebook_task"]["base_parameters"] = job_parameters
    return d_task

def get_job_parameters(job_parameters: Dict[str, str]):
    """Convert job parameters to list format"""
    if not job_parameters:
        return []
    l_params = []
    for k, v in job_parameters.items():
        job_param = {"name": k, "default": v}
        l_params.append(job_param)
    return l_params

def get_schedule(cron_expression: str, paused_status: str = "PAUSED"):
    """Build schedule configuration"""
    return {
        "quartz_cron_expression": cron_expression,
        "timezone_id": "Asia/Kolkata",
        "pause_status": paused_status,
    }

def check_if_job_exists(job_name: str):
    """Check if a job with given name exists"""
    try:
        jobs = [j for j in w.jobs.list(name=job_name)]
        if not jobs:
            return None
        if len(jobs) > 1:
            ids = ", ".join(str(j.job_id) for j in jobs)
            raise ValueError(f"Multiple jobs found with name '{job_name}': {ids}")
        return jobs[0].job_id
    except Exception as e:
        print(f"  Error checking if job exists: {e}")
        return None

def create_job(job_name: str, job_attribs: Dict[str, Any], dry_run: bool = False):
    """Create or update a job"""
    if dry_run:
        print(f"  [DRY RUN] Would create/update job: {job_name}")
        return None
    
    job_id = check_if_job_exists(job_name)
    if job_id:
        print(f"  Job already exists with job_id:{job_id}, updating...")
        jb = Job.from_dict(job_attribs)
        w.jobs.reset(new_settings=jb, job_id=job_id)
    else:
        print(f"  Creating new job: {job_name}")
        jb = Job.from_dict(job_attribs)
        try:
            # Try as_shallow_dict() for newer SDK versions
            created_job = w.jobs.create(**jb.as_shallow_dict())
            job_id = created_job.job_id
        except AttributeError:
            # Fallback for older SDK versions
            created_job = w.jobs.create(**job_attribs)
            job_id = created_job.job_id
    return job_id

# COMMAND ----------

# DBTITLE 1,Job Creation Function
def create_job_from_config(job_config: Dict[str, Any], global_config: Dict[str, Any], dry_run: bool = False):
    """Create a validation job from configuration"""
    
    elem_config = deepcopy(global_config)
    elem_config.update(job_config)
    
    job_name = elem_config.get('job_name')
    if not job_name:
        print("  ERROR: job_name is required for each job")
        return None
    
    notebook_path = elem_config.get('notebook_path')
    job_parameters = elem_config.get('job_parameters', {})
    
    driver_instance_pool_id = elem_config.get('driver_instance_pool_id')
    instance_pool_id = elem_config.get('instance_pool_id')
    policy_id = elem_config.get('policy_id')
    min_workers = elem_config.get('min_workers', 2)
    max_workers = elem_config.get('max_workers', 5)
    spn_id = elem_config.get('spn_id')
    cluster_name = f"{job_name[:10]}_cluster"
    cluster_tags = elem_config.get('cluster_tags', {})
    spark_conf = elem_config.get('spark_conf', {})
    
    if not isinstance(cluster_tags, dict):
        print("  Warning: cluster_tags is not in Dict[str,str] format")
        cluster_tags = {}
    
    d_cluster = get_cluster(cluster_name, instance_pool_id, policy_id,
                           driver_instance_pool_id, min_workers, max_workers,
                           cluster_tags, spark_conf)
    d_task = get_task(job_name, cluster_name, notebook_path, job_parameters)
    l_job_params = get_job_parameters(job_parameters)
    
    email_notify = elem_config.get('email_notification_list', "")
    l_email_notify = [x.strip() for x in email_notify.split(",")] if email_notify else []
    
    _schedule = elem_config.get('schedule')
    schedule = None
    if _schedule and str(_schedule).strip() != '':
        pause_schedule = elem_config.get('pause_schedule', False)
        paused_status = 'PAUSED' if pause_schedule else 'UNPAUSED'
        schedule = get_schedule(_schedule, paused_status=paused_status)
    
    timeout_seconds = elem_config.get('timeout_seconds', 7200)
    warning_threshold_seconds = elem_config.get('warning_threshold_seconds', 3600)
    max_concurrent_runs = elem_config.get('max_concurrent_runs', 1)
    queue_enabled = elem_config.get('is_queue_enabled', True)
    
    # Merge global job_tags with job-specific tags
    tags = deepcopy(global_config.get('job_tags', {}))
    job_specific_tags = elem_config.get('tags', {})
    if isinstance(job_specific_tags, dict):
        tags.update(job_specific_tags)
    elif job_specific_tags:
        print("  Warning: tags is not in Dict[str,str] format")
    
    d_job = {
        "name": job_name,
        "tasks": [d_task],
        "job_clusters": [d_cluster],
        "queue": {"enabled": queue_enabled}
    }
    
    if spn_id and str(spn_id).strip() != '':
        d_job["run_as"] = {"service_principal_name": spn_id}
    
    if l_job_params:
        d_job["parameters"] = l_job_params
    
    if l_email_notify:
        d_job["email_notifications"] = {
            "on_failure": l_email_notify,
            "no_alert_for_skipped_runs": True,
        }
    
    if schedule:
        d_job["schedule"] = schedule
    
    if timeout_seconds:
        d_job["timeout_seconds"] = timeout_seconds
    
    if warning_threshold_seconds:
        d_job["health"] = {"rules": []}
        d_job["health"]["rules"].append({
            "metric": "RUN_DURATION_SECONDS",
            "op": "GREATER_THAN",
            "value": warning_threshold_seconds
        })
        if l_email_notify:
            d_job["email_notifications"]["on_duration_warning_threshold_exceeded"] = l_email_notify
    
    if max_concurrent_runs:
        d_job["max_concurrent_runs"] = max_concurrent_runs
    
    if tags:
        d_job["tags"] = tags
    
    print(f"\nProcessing: {job_name}")
    if dry_run:
        print("  [DRY RUN] Job configuration:")
        pprint(d_job)
        return None
    
    job_id = create_job(job_name, d_job, dry_run)
    
    if job_id:
        d_permissions = elem_config.get('permissions')
        if d_permissions:
            try:
                update_permissions(job_id, d_permissions)
            except Exception as e:
                print(f"  Error updating permissions: {e}")
    
    return job_id

# COMMAND ----------

# DBTITLE 1,Process All Jobs
print("\n" + "="*80)
print("PROCESSING JOBS")
print("="*80)

created_jobs = []
failed_jobs = []

for job_config in jobs_config:
    try:
        job_id = create_job_from_config(job_config, global_config, dry_run)
        if job_id:
            job_name = job_config.get('job_name')
            created_jobs.append({
                "job_name": job_name,
                "job_id": job_id
            })
    except Exception as e:
        job_name = job_config.get('job_name', 'unknown')
        failed_jobs.append({
            "job_name": job_name,
            "error": str(e)
        })
        print(f"\n  ERROR processing {job_name}: {e}")

print("\n" + "="*80)
print("SUMMARY")
print("="*80)
print(f"Total jobs in config: {len(jobs_config)}")
if not dry_run:
    print(f"Successfully created/updated: {len(created_jobs)}")
    print(f"Failed: {len(failed_jobs)}")
else:
    print("DRY RUN - No jobs were created or modified")

if created_jobs:
    print("\nSuccessfully processed jobs:")
    for job in created_jobs:
        print(f"  - {job['job_name']:40s} (ID: {job['job_id']})")

if failed_jobs:
    print("\nFailed jobs:")
    for job in failed_jobs:
        print(f"  - {job['job_name']:40s} Error: {job['error']}")

# COMMAND ----------

# DBTITLE 1,Store Results
if created_jobs and not dry_run:
    import pandas as pd
    from datetime import datetime
    
    df = pd.DataFrame(created_jobs)
    df['created_at'] = datetime.now()
    
    spark_df = spark.createDataFrame(df)
    spark_df.createOrReplaceTempView("validation_jobs_deployed")
    
    print("\nJob IDs stored in temporary view: validation_jobs_deployed")
    display(spark_df)

