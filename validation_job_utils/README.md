# Validation Job Utils

Automated job deployment and management system for the lightweight validation framework, replicating the approach from the data ingestion framework (`job_utils`).

## 📁 Directory Structure

```
validation_job_utils/
├── validation_job_config.yml    # Job configuration file
├── job_deployment.ipynb          # Create/update jobs
├── pause_unpause_jobs.ipynb      # Bulk job management
└── README.md                     # This file
```

## 🚀 Quick Start

### 1. Configure Jobs

Edit `validation_job_config.yml`:

```yaml
# Global settings
driver_instance_pool_id: 'your-driver-pool-id'
instance_pool_id: 'your-worker-pool-id'
policy_id: 'your-policy-id'
min_workers: 2
max_workers: 5
cluster_tags:
  managed_by: validation_framework

# Define jobs
jobs:
  - table_group: group1_basic
    notebook_path: '/Workspace/.../notebooks/main'
    job_parameters:
      table_group: group1_basic
      iteration_suffix: test
      isFullValidation: "false"
    schedule: null  # Manual trigger
    email_notification_list: your-email@example.com
```

### 2. Deploy Jobs

Run `job_deployment.ipynb` notebook:
- **Dry Run**: Set `dry_run=Y` (default) to preview changes
- **Execute**: Set `dry_run=N` to create/update jobs

### 3. Manage Jobs

Use `pause_unpause_jobs.ipynb` to:
- Pause/resume job schedules
- Cancel active or queued runs
- Filter jobs by name pattern

---

## 📋 Configuration Guide

### Global Settings

| Setting | Description | Example |
|---------|-------------|---------|
| `driver_instance_pool_id` | Driver node instance pool ID | `pool-abc123` |
| `instance_pool_id` | Worker nodes instance pool ID | `pool-def456` |
| `policy_id` | Cluster policy ID | `policy-xyz789` |
| `spn_id` | Service principal for "Run As" | `spn-id` (optional) |
| `min_workers` | Default minimum workers | `2` |
| `max_workers` | Default maximum workers | `5` |
| `cluster_tags` | Tags applied to all clusters | `managed_by: validation_framework` |
| `timeout_seconds` | Default job timeout | `7200` (2 hours) |
| `warning_threshold_seconds` | Health alert threshold | `3600` (1 hour) |
| `permissions` | Default job permissions | See below |

### Job Configuration

Each job in the `jobs` array supports:

#### Required Fields

| Field | Description | Example |
|-------|-------------|---------|
| `table_group` | Table group name (used in job name) | `group1_basic` |
| `notebook_path` | Workspace path to main.py notebook | `/Workspace/.../notebooks/main` |
| `job_parameters` | Parameters passed to notebook | See below |

#### Job Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `table_group` | Table group to validate | Required |
| `iteration_suffix` | Iteration identifier | Required |
| `isFullValidation` | Full validation mode | `"false"` |

#### Optional Fields

| Field | Description | Default |
|-------|-------------|---------|
| `min_workers` | Override min workers | Global value |
| `max_workers` | Override max workers | Global value |
| `spark_conf` | Spark configuration | `{}` |
| `schedule` | Cron schedule | `null` (manual) |
| `email_notification_list` | Comma-separated emails | None |
| `timeout_seconds` | Job timeout | Global value |
| `warning_threshold_seconds` | Health alert threshold | Global value |
| `max_concurrent_runs` | Max concurrent runs | `1` |
| `is_queue_enabled` | Enable job queuing | `true` |
| `pause_schedule` | Start with paused schedule | `false` |
| `tags` | Custom job tags | `{}` |
| `permissions` | Override job permissions | Global value |

### Schedule Format (Quartz Cron)

| Expression | Description |
|------------|-------------|
| `0 0 2 * * ?` | Daily at 2 AM |
| `0 0 */6 * * ?` | Every 6 hours |
| `0 0 * * * ?` | Every hour |
| `0 */30 * * * ?` | Every 30 minutes |
| `null` or `''` | Manual trigger only |

**Timezone**: All schedules use `Asia/Kolkata` timezone.

### Permissions Format

```yaml
permissions:
  can_manage: group:admins
  can_manage_run: user:email@example.com, user:another@example.com
  can_view: group:analysts
```

**Format**: `type:identity` where type is `user`, `group`, or `service_principal`

---

## 📓 Notebook Usage

### job_deployment.ipynb

Creates or updates validation jobs from configuration.

**Widgets**:
- `config_file_path`: Path to YAML config (default: `./validation_job_config.yml`)
- `dry_run`: Preview mode (`Y` = preview, `N` = execute)

**Features**:
- ✅ Smart update: Updates existing jobs instead of failing
- ✅ Dry run mode: Preview changes before applying
- ✅ Permission management: Automatically sets job permissions
- ✅ Health monitoring: Configures duration warning alerts
- ✅ Queue management: Controls job queuing behavior
- ✅ Service principal support: Runs jobs as SPN

**Job Naming Convention**: `validation_job_{table_group}`

**Example**:
- Table group: `group1_basic`
- Job name: `validation_job_group1_basic`

### pause_unpause_jobs.ipynb

Bulk management of validation jobs.

**Widgets**:
- `job_name_contains`: Filter jobs by name (default: all `validation_job_*`)
- `pause_schedule`: Action (`pause` or `resume`)
- `cancel_all_runs`: Cancel all active runs (`Y`/`N`)
- `cancel_queued_runs`: Cancel only queued/pending runs (`Y`/`N`)
- `dry_run`: Preview mode (`Y`/`N`)

**Use Cases**:
1. **Pause all validation jobs**: 
   - `pause_schedule=pause`, `dry_run=N`
2. **Resume specific jobs**:
   - `job_name_contains=group1`, `pause_schedule=resume`, `dry_run=N`
3. **Cancel queued runs**:
   - `cancel_queued_runs=Y`, `dry_run=N`

---

## 🔧 Advanced Features

### 1. Health Monitoring

Jobs automatically monitor duration and send alerts when execution exceeds `warning_threshold_seconds`.

```yaml
warning_threshold_seconds: 3600  # Alert if job runs > 1 hour
email_notification_list: your-email@example.com
```

### 2. Job Queuing

Control how concurrent runs are handled:

```yaml
max_concurrent_runs: 1
is_queue_enabled: true  # Queue new runs if one is active
```

- `is_queue_enabled: true`: Queues runs when job is busy
- `is_queue_enabled: false`: Skips runs when job is busy

### 3. Service Principal Execution

Run jobs as a service principal instead of user:

```yaml
spn_id: 'your-service-principal-id'
```

### 4. Custom Cluster Configuration

Override cluster settings per job:

```yaml
jobs:
  - table_group: large_dataset_group
    min_workers: 8
    max_workers: 16
    spark_conf:
      spark.sql.shuffle.partitions: "400"
      spark.sql.adaptive.enabled: "true"
```

### 5. Custom Tags

Add custom tags for organization and cost tracking:

```yaml
tags:
  table_group: group1_basic
  validation_type: comprehensive
  environment: production
  cost_center: analytics
```

---

## 🔄 Workflow

### Initial Setup

1. **Configure environment**:
   - Get instance pool IDs from Databricks
   - Get cluster policy ID
   - Update `validation_job_config.yml`

2. **Define table groups**:
   - Add job entries for each table group
   - Configure schedules and resources
   - Set email notifications

3. **Deploy jobs (dry run)**:
   ```
   Run job_deployment.ipynb with dry_run=Y
   ```

4. **Deploy jobs (execute)**:
   ```
   Run job_deployment.ipynb with dry_run=N
   ```

### Updating Jobs

1. **Edit configuration**:
   - Modify `validation_job_config.yml`
   - Change schedules, resources, or parameters

2. **Redeploy**:
   ```
   Run job_deployment.ipynb with dry_run=N
   ```
   - Existing jobs are updated (not recreated)
   - Job history is preserved

### Maintenance

1. **Pause all jobs**:
   ```
   Run pause_unpause_jobs.ipynb:
   - pause_schedule=pause
   - dry_run=N
   ```

2. **Resume specific jobs**:
   ```
   Run pause_unpause_jobs.ipynb:
   - job_name_contains=group1
   - pause_schedule=resume
   - dry_run=N
   ```

3. **Cancel stuck runs**:
   ```
   Run pause_unpause_jobs.ipynb:
   - cancel_all_runs=Y
   - dry_run=N
   ```

---

## 🆚 Comparison with Previous System

| Feature | Old System (`notebooks/setup`) | New System (`validation_job_utils`) |
|---------|-------------------------------|-----------------------------------|
| Configuration | Python dict in notebook | YAML file |
| Update existing jobs | ❌ Errors if exists | ✅ Updates existing |
| Cluster type | Ephemeral `new_cluster` | Instance pools with autoscale |
| Dry run | ❌ No | ✅ Yes |
| Health monitoring | ❌ No | ✅ Duration warnings |
| Queue management | ❌ No | ✅ Configurable |
| Service principal | ❌ No | ✅ Yes |
| Permissions | Global only | Per-job or global |
| Bulk operations | Manual, one-by-one | Bulk pause/resume/cancel |
| Environment support | Single | Single (expandable) |

---

## 🎯 Best Practices

### 1. Configuration Management

- **Version control**: Commit `validation_job_config.yml` to git
- **Documentation**: Comment complex configurations
- **Validation**: Always test with `dry_run=Y` first

### 2. Resource Sizing

- **Small tables**: 2-4 workers
- **Medium tables**: 4-8 workers
- **Large tables**: 8-16 workers
- **Monitor**: Adjust based on actual job metrics

### 3. Scheduling

- **Off-peak hours**: Schedule during low-usage times
- **Stagger schedules**: Avoid all jobs starting simultaneously
- **Consider dependencies**: Schedule after ingestion jobs complete

### 4. Monitoring

- **Email alerts**: Configure for production jobs
- **Health thresholds**: Set realistic warning thresholds
- **Review regularly**: Check job run history weekly

### 5. Security

- **Service principals**: Use for production
- **Permissions**: Follow principle of least privilege
- **Instance pools**: Use pools with proper security settings

---

## 🐛 Troubleshooting

### Job Creation Fails

**Error**: "Config file not found"
- **Solution**: Ensure `validation_job_config.yml` is in the same directory or provide full path

**Error**: "Multiple jobs found with name"
- **Solution**: Manually delete duplicate jobs in Databricks UI

**Error**: "Invalid instance pool ID"
- **Solution**: Verify pool IDs exist in your workspace

### Job Runs Fail

**Error**: Cluster startup timeout
- **Solution**: Check instance pool availability and capacity

**Error**: Permission denied
- **Solution**: Verify service principal has workspace access

**Error**: Notebook not found
- **Solution**: Verify `notebook_path` is correct workspace path

### Pause/Unpause Issues

**Issue**: Jobs not being paused
- **Solution**: Ensure `dry_run=N` and job has a schedule

**Issue**: Cannot cancel runs
- **Solution**: Check you have CAN_MANAGE_RUN permission

---

## 📚 Additional Resources

- [Databricks Jobs API](https://docs.databricks.com/api/workspace/jobs)
- [Databricks SDK for Python](https://databricks-sdk-py.readthedocs.io/)
- [Quartz Cron Expressions](http://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html)
- [Instance Pools](https://docs.databricks.com/clusters/instance-pools.html)

---

## 🔐 Security Notes

⚠️ **Warning**: The deployment notebook uses `eval()` for template parsing. Ensure:
- Configuration files are from trusted sources only
- YAML files are properly validated before use
- Access to these notebooks is restricted to authorized users

---

## 📝 Change Log

### Version 1.0 (Initial Release)
- YAML-based configuration
- Job creation and update logic
- Bulk pause/resume functionality
- Health monitoring support
- Permission management
- Service principal support
- Dry run mode
- Queue management

---

## 🤝 Contributing

To extend this system:

1. **Add new features**: Modify helper functions in notebooks
2. **Update config**: Add new fields to YAML schema
3. **Document**: Update this README
4. **Test**: Always test with `dry_run=Y` first

---

## 📞 Support

For issues or questions:
1. Check troubleshooting section above
2. Review job run logs in Databricks UI
3. Contact your Databricks administrator for workspace-specific issues

---

**Happy Validating! 🎉**

