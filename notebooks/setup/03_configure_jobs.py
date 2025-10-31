# Databricks notebook source
"""
Job Configuration for Lightweight Validation Framework

This config file defines:
1. Cluster profiles (reusable cluster configurations)
2. Table group schedules (which profile + schedule for each group)

Option A: Python config file (easy to read and edit)

Edit this file to:
- Add/modify cluster profiles
- Change schedules for table_groups
- Assign different profiles to table_groups
"""

# COMMAND ----------

# DBTITLE 1,Cluster Profiles
"""
Define reusable cluster profiles with different sizes and configurations.
You can have as many profiles as needed.
"""

CLUSTER_PROFILES = {
    "small": {
        "spark_version": "14.3.x-scala2.12",
        "node_type_id": "Standard_DS3_v2",  # Azure: 4 cores, 14GB RAM
        "num_workers": 2,
        "spark_conf": {
            "spark.databricks.cluster.profile": "singleNode",
            "spark.master": "local[*]"
        },
        "custom_tags": {
            "ResourceClass": "SingleNode"
        }
    },
    
    "medium": {
        "spark_version": "14.3.x-scala2.12",
        "node_type_id": "Standard_DS4_v2",  # Azure: 8 cores, 28GB RAM
        "num_workers": 4,
        "spark_conf": {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true"
        },
        "custom_tags": {
            "ResourceClass": "Medium"
        }
    },
    
    "large": {
        "spark_version": "14.3.x-scala2.12",
        "node_type_id": "Standard_DS5_v2",  # Azure: 16 cores, 56GB RAM
        "num_workers": 8,
        "spark_conf": {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.shuffle.partitions": "200"
        },
        "custom_tags": {
            "ResourceClass": "Large"
        }
    },
    
    "spot_medium": {
        "spark_version": "14.3.x-scala2.12",
        "node_type_id": "Standard_DS4_v2",  # Azure: 8 cores, 28GB RAM
        "num_workers": 4,
        "spark_conf": {
            "spark.sql.adaptive.enabled": "true"
        },
        "custom_tags": {
            "ResourceClass": "Medium",
            "InstanceType": "Spot"
        }
    }
}

# COMMAND ----------

# DBTITLE 1,Table Group Job Configurations
"""
Define schedule and cluster profile for each table_group.

Schedule format (quartz cron):
- "0 0 2 * * ?" = Daily at 2 AM
- "0 0 */6 * * ?" = Every 6 hours
- "0 0 * * * ?" = Every hour
- "0 */30 * * * ?" = Every 30 minutes

Set schedule to None for manual trigger only.
"""

TABLE_GROUP_CONFIGS = {
    "demo_dmv": {
        "cluster_profile": "small",
        "schedule": None,  # Daily at 2 AM
        "iteration_suffix": "daily",
        "timeout_seconds": 3600,
        "max_retries": 2
    },
    
    "hive_full_test": {
        "cluster_profile": "small",
        "schedule": None,  # Daily at 4 AM
        "iteration_suffix": "daily",
        "timeout_seconds": 7200,
        "max_retries": 2
    },
    
    "incremental_test_abhishek": {
        "cluster_profile": "small",
        "schedule": None,  # Every 6 hours
        "iteration_suffix": "6hourly",
        "timeout_seconds": 3600,
        "max_retries": 1
    },
    
    "incremntal_test_group": {
        "cluster_profile": "small",
        "schedule": None,  # Daily at 3 AM
        "iteration_suffix": "daily",
        "timeout_seconds": 3600,
        "max_retries": 2
    },
    
    "test_file_options": {
        "cluster_profile": "small",
        "schedule": None,  # Daily at 1 AM
        "iteration_suffix": "daily",
        "timeout_seconds": 1800,
        "max_retries": 1
    },
    
    # Add more table groups here...
    # "your_group": {
    #     "cluster_profile": "medium",
    #     "schedule": "0 0 2 * * ?",
    #     "iteration_suffix": "daily",
    #     "timeout_seconds": 3600,
    #     "max_retries": 2
    # }
}

# COMMAND ----------

# DBTITLE 1,Global Job Settings
"""
Global settings applied to all jobs
"""

GLOBAL_JOB_SETTINGS = {
    "max_concurrent_runs": 1,  # Only one run at a time per job
    "email_notifications": {
        "on_failure": [],  # Add email addresses for failure notifications
        "on_success": []   # Add email addresses for success notifications
    },
    "timeout_seconds": 3600,  # Default timeout (can be overridden per table_group)
    "max_retries": 2,  # Default retries (can be overridden per table_group)
    "min_retry_interval_millis": 60000,  # 1 minute between retries
    "retry_on_timeout": True
}

# COMMAND ----------

# DBTITLE 1,Job Name Template
"""
Job naming convention: {JOB_NAME_PREFIX}_{table_group}

The job name prefix is configurable via JOB_NAME_PREFIX constant in deltarecon.config.constants.
Default: "validation"

Example: If JOB_NAME_PREFIX = "validation" and table_group = "sales",
         the job name will be: "validation_sales"

Note: Jobs are identified by TAGS, not by name prefix.
You can change the naming pattern by updating JOB_NAME_PREFIX in constants.py.
The framework will still identify jobs via tags:
  - managed_by = "deltarecon"
  - table_group = "<group_name>"
"""

# Import job name prefix from constants
from deltarecon.config.constants import JOB_NAME_PREFIX

def get_job_name(table_group: str) -> str:
    """
    Get job name for a table group
    
    Format: {JOB_NAME_PREFIX}_{table_group}
    
    The prefix is configurable via JOB_NAME_PREFIX in deltarecon.config.constants.
    Change it there to customize all job names at once.
    """
    return f"{JOB_NAME_PREFIX}_{table_group}"

# COMMAND ----------

# DBTITLE 1,Validation
print("JOB CONFIGURATION VALIDATION")
# Validate cluster profiles exist
for table_group, config in TABLE_GROUP_CONFIGS.items():
    profile = config.get("cluster_profile")
    if profile not in CLUSTER_PROFILES:
        print(f"ERROR: Table group '{table_group}' uses undefined cluster profile '{profile}'")
    else:
        print(f"{table_group:20s} -> {profile:15s} -> {config.get('schedule', 'Manual')}")

print(f"\nTotal table groups configured: {len(TABLE_GROUP_CONFIGS)}")
print(f"Cluster profiles defined: {len(CLUSTER_PROFILES)}")
print("\nNext step: Run 04_generate_jobs.py to create the jobs")

