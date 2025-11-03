# Databricks notebook source
# MAGIC %md
# MAGIC # Validation Job Deployment
# MAGIC Deploy validation jobs from YAML configuration

# COMMAND ----------

# DBTITLE 1,Install Required Packages
# %pip install --upgrade databricks-sdk==0.49.0
# %pip install pyyaml

# COMMAND ----------

# DBTITLE 1,Restart Python
# %restart_python

# COMMAND ----------

# DBTITLE 1,Widgets
dbutils.widgets.text("config_file_path","")
dbutils.widgets.dropdown("dry_run","Y",["Y","N"])

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
config_file_path = dbutils.widgets.get("config_file_path")
config_file_path = config_file_path.strip()
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
        raise

# Extract global config (everything except 'jobs' key)
global_config = {k: v for k, v in d_config.items() if k != 'jobs'}
print("="*80)
print(f"DRY RUN MODE: {'ENABLED' if dry_run else 'DISABLED'}")
print("="*80)
print("\nGlobal Configuration:")
pprint(global_config)

jobs = d_config.get('jobs', [])
print(f"\nNumber of jobs to deploy: {len(jobs)}")
pprint(jobs)

w = WorkspaceClient()

# COMMAND ----------

# DBTITLE 1,Permission Management Functions
def get_job_owner(job_id):
    response = w.jobs.get_permissions(job_id=job_id)
    l_permissions = response.access_control_list
    d_res = {}
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
                print(f"Cannot modify owner privileges for {owner_type} {owner}. If owner needs to be changed, contact admin.")
                continue
            user_name = identity if id_type == 'user' else None
            group_name = identity if id_type == 'group' else None
            service_principal_name = identity if id_type in ('service_principal', 'spn') else None
            required_access_level = (
                JobPermissionLevel.CAN_MANAGE if access_level == 'can_manage'
                else JobPermissionLevel.CAN_MANAGE_RUN if access_level == 'can_manage_run'
                else JobPermissionLevel.CAN_VIEW
            )
            acl = JobAccessControlRequest(user_name=user_name, group_name=group_name, service_principal_name=service_principal_name, permission_level=required_access_level)
            l_acls.append(acl)
        # end of for
    # end of for
    if l_acls:
        w.jobs.update_permissions(job_id=job_id, access_control_list=l_acls)
        print(f"Permissions updated for job {job_id}")

# COMMAND ----------

# DBTITLE 1,Cluster and Task Templates
# NOTE: instance_pool_id and driver_instance_pool_id are conditionally added below
# Only uncomment them in YAML if your policy does NOT define instance pools
# If policy defines them, leave commented to avoid validation errors
cluster_template = """
{
  "job_cluster_key": "{cluster_name}",
  "new_cluster": {
      "cluster_name": "",
      "spark_version": "16.4.x-scala2.12",
      "spark_env_vars": {
          "PYSPARK_PYTHON": "/databricks/python3/bin/python3",
      },
      "policy_id": "{policy_id}",
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
                min_workers: int, max_workers: int,
                custom_tags: Dict[str, str] = None,
                spark_conf: Dict[str, str] = None,
                node_type_id: str = None,
                driver_node_type_id: str = None
                ):
    cluster = cluster_template.replace("{cluster_name}", cluster_name)
    cluster = cluster.replace("{policy_id}", policy_id)
    cluster = cluster.replace("{min_workers}", str(min_workers))
    cluster = cluster.replace("{max_workers}", str(max_workers))
    d_cluster = eval(cluster)
    
    # Priority 1: Use node_type_id if defined (more explicit)
    # Priority 2: Use instance_pool_id if defined
    # Priority 3: Let policy define compute
    
    if node_type_id and str(node_type_id).strip():
        # Use node type (takes priority)
        d_cluster["new_cluster"]["node_type_id"] = node_type_id
        print(f"  Using node_type_id: {node_type_id}")
    elif instance_pool_id and str(instance_pool_id).strip():
        # Use instance pool as fallback
        d_cluster["new_cluster"]["instance_pool_id"] = instance_pool_id
        print(f"  Using instance_pool_id: {instance_pool_id}")
    # else: policy defines compute
    
    if driver_node_type_id and str(driver_node_type_id).strip():
        # Use driver node type (takes priority)
        d_cluster["new_cluster"]["driver_node_type_id"] = driver_node_type_id
        print(f"  Using driver_node_type_id: {driver_node_type_id}")
    elif driver_instance_pool_id and str(driver_instance_pool_id).strip():
        # Use driver instance pool as fallback
        d_cluster["new_cluster"]["driver_instance_pool_id"] = driver_instance_pool_id
        print(f"  Using driver_instance_pool_id: {driver_instance_pool_id}")
    # else: policy defines compute
    
    if custom_tags:
        d_cluster["new_cluster"]["custom_tags"] = custom_tags
    if spark_conf:
        d_cluster["new_cluster"]["spark_conf"] = spark_conf
    return d_cluster

def get_task(job_name: str,
             cluster_name: str, 
             notebook_path: str, 
             job_parameters: Dict[str, Any]
             ):
    task = task_template.replace("{job_name}", job_name)
    task = task.replace("{notebook_path}", notebook_path)
    task = task.replace("{cluster_name}", cluster_name)
    d_task = eval(task)
    if job_parameters:
        d_task["notebook_task"]["base_parameters"] = job_parameters
    return d_task

def get_job_parameters(job_parameters: Dict[str, str]):
    if not job_parameters:
        return []
    l_params = []
    for k, v in job_parameters.items():
        job_param = {
            "name": k,
            "default": v
        }
        l_params.append(job_param)
    # end of for
    return l_params

def get_schedule(cron_expression: str, paused_status: str = "PAUSED"):
    return {
        "quartz_cron_expression": cron_expression,
        "timezone_id": "Asia/Kolkata",
        "pause_status": paused_status,
    }

def check_if_job_exists(job_name: str):
    try:
        jobs = [j for j in w.jobs.list(name=job_name)]
        if not jobs:
            return None
        if len(jobs) > 1:
            ids = ", ".join(str(j.job_id) for j in jobs)
            raise ValueError(f"Multiple jobs found with name '{job_name}': {ids}")
        # end of if
        return jobs[0].job_id
    except Exception as e:
        print("Error encountered while checking if job exists")
        print(e)
        return None

def create_job(job_name: str, job_attribs: Dict[str, Any], dry_run: bool = False):
    if dry_run:
        print(f"[DRY RUN] Would create/update job: {job_name}")
        print("[DRY RUN] Job configuration:")
        pprint(job_attribs)
        return None
    
    jb = Job.from_dict(job_attribs)
    job_id = check_if_job_exists(job_name)
    if job_id:
        print(f"Job already exists with job_id:{job_id}, updating..")
        w.jobs.reset(new_settings=jb, job_id=job_id)
    else:
        print(f"Creating new job {job_name}")
        jb = w.jobs.create(**jb.as_shallow_dict())
        job_id = jb.job_id
    return job_id

# COMMAND ----------

# DBTITLE 1,Job Creation Function
def create_job_from_config(job_name, job_config, dry_run=False):
    notebook_path = elem_config.get('notebook_path')
    job_parameters = elem_config.get('job_parameters')
    driver_instance_pool_id = elem_config.get('driver_instance_pool_id')
    instance_pool_id = elem_config.get('instance_pool_id')
    node_type_id = elem_config.get('node_type_id')
    driver_node_type_id = elem_config.get('driver_node_type_id')
    policy_id = elem_config.get('policy_id')
    min_workers = elem_config.get('min_workers', 2)
    max_workers = elem_config.get('max_workers', 5)
    spn_id = elem_config.get('spn_id')
    cluster_name = f"{job_name[:10]}_cluster"
    cluster_tags = elem_config.get('cluster_tags', {})
    spark_conf = elem_config.get('spark_conf', {})
    if not isinstance(cluster_tags, dict):
        print("warning: cluster_tags cant be applied since not in `Dict[str,str] format, kindly modify the yml file.")
        cluster_tags = {}
    d_cluster = get_cluster(cluster_name, instance_pool_id, policy_id, driver_instance_pool_id, 
                           min_workers, max_workers, cluster_tags, spark_conf,
                           node_type_id, driver_node_type_id)
    d_task = get_task(job_name, cluster_name, notebook_path, job_parameters)
    l_job_params = get_job_parameters(job_parameters)
    email_notify = elem_config.get('email_notification_list', "")
    l_email_notify = [x.strip() for x in email_notify.split(",")] if email_notify else []
    _schedule = elem_config.get('schedule')
    if _schedule and _schedule.strip() != '':
        pause_schedule = elem_config.get('pause_schedule', False)
        paused_status = 'PAUSED' if pause_schedule else 'UNPAUSED'
        schedule = get_schedule(_schedule, paused_status=paused_status)
    else:
        schedule = None
    timeout_seconds = elem_config.get('timeout_seconds', 7200)
    warning_threshold_seconds = elem_config.get('warning_threshold_seconds', 3600)
    max_concurrent_runs = elem_config.get('max_concurrent_runs', 1)
    queue_enabled = elem_config.get('is_queue_enabled', True)
    
    tags = elem_config.get('tags', {})
    if not isinstance(tags, dict):
        print("warning: tags cant be applied since not in `Dict[str,str] format, kindly modify the yml file.")
        tags = {}
    # pprint(d_cluster)
    # pprint(d_task)
    d_job = {
        "name": job_name,
        "tasks": [d_task],
        "job_clusters": [d_cluster],
        "queue": {
            "enabled": True
        }
    }
    if spn_id and str(spn_id).strip() != '':
        d_job["run_as"] = {
            "service_principal_name": spn_id
        }
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
    d_job["queue"]["enabled"] = queue_enabled
    # pprint(d_job)
    job_id = create_job(job_name, d_job, dry_run)
    # print(f"Job id: {job_id}")
    
    if not dry_run and job_id:
        d_permissions = elem_config.get('permissions')
        if d_permissions:
            try:
                update_permissions(job_id, d_permissions)
            except Exception as e:
                print(f"Error encountered while updating permissions for job_name: {job_name}, permissions:{d_permissions}")
                print(e)

# COMMAND ----------

# DBTITLE 1,Process All Jobs
print("\n" + "="*80)
print("PROCESSING JOBS")
print("="*80)

for job_config in jobs:
    elem_config = deepcopy(global_config)
    elem_config.update(job_config)
    job_name = elem_config.get('job_name')
    print(f"\n{'='*80}")
    print(f"Job: {job_name}")
    print(f"{'='*80}")
    create_job_from_config(job_name, elem_config, dry_run)

print("\n" + "="*80)
print("DEPLOYMENT COMPLETE")
print("="*80)
