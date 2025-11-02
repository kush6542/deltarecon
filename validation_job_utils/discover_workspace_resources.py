# Databricks notebook source
# MAGIC %md
# MAGIC # Discover Workspace Resources
# MAGIC This notebook helps you find the correct policy_id and instance_pool_id for your workspace

# COMMAND ----------

# DBTITLE 1,Install/Upgrade SDK (if needed)
# Uncomment if you need to upgrade SDK
# %pip install --upgrade databricks-sdk==0.49.0
# %restart_python

# COMMAND ----------

# DBTITLE 1,Imports
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import compute
import json
from pprint import pprint

w = WorkspaceClient()

# COMMAND ----------

# DBTITLE 1,List All Cluster Policies
print("="*80)
print("AVAILABLE CLUSTER POLICIES")
print("="*80)

policies = list(w.cluster_policies.list())
print(f"\nFound {len(policies)} cluster policies:\n")

for policy in policies:
    print(f"Policy Name: {policy.name}")
    print(f"  Policy ID: {policy.policy_id}")
    print(f"  Created By: {policy.creator_user_name}")
    print(f"  Description: {policy.description or 'N/A'}")
    print("-" * 80)

# COMMAND ----------

# DBTITLE 1,Choose a Policy to Inspect
# UPDATE THIS with a policy_id from the list above
SELECTED_POLICY_ID = "001900960C882452"  # Update this!

print("="*80)
print("POLICY DETAILS")
print("="*80)

try:
    policy = w.cluster_policies.get(policy_id=SELECTED_POLICY_ID)
    policy_def = json.loads(policy.definition)
    
    print(f"\nPolicy Name: {policy.name}")
    print(f"Policy ID: {policy.policy_id}")
    print(f"\nPolicy Definition:")
    pprint(policy_def, width=100)
    
    print("\n" + "="*80)
    print("COMPUTE CONFIGURATION ANALYSIS")
    print("="*80)
    
    # Check what the policy defines
    defines_instance_pool = False
    defines_node_type = False
    
    if 'instance_pool_id' in policy_def:
        pool_config = policy_def['instance_pool_id']
        if isinstance(pool_config, dict):
            if pool_config.get('type') == 'fixed':
                defines_instance_pool = True
                print(f"\n✓ Policy DEFINES instance_pool_id as FIXED")
                print(f"  Value: {pool_config.get('value')}")
            elif pool_config.get('type') == 'unlimited':
                print(f"\n○ Policy allows instance_pool_id (unlimited)")
                print(f"  You CAN specify instance pools in YAML")
            else:
                print(f"\n○ Policy has instance_pool_id configuration:")
                pprint(pool_config)
        else:
            print(f"\n○ instance_pool_id: {pool_config}")
    else:
        print(f"\n○ Policy does NOT restrict instance_pool_id")
        print(f"  You CAN specify instance pools in YAML")
    
    if 'driver_instance_pool_id' in policy_def:
        driver_pool_config = policy_def['driver_instance_pool_id']
        if isinstance(driver_pool_config, dict):
            if driver_pool_config.get('type') == 'fixed':
                print(f"\n✓ Policy DEFINES driver_instance_pool_id as FIXED")
                print(f"  Value: {driver_pool_config.get('value')}")
            elif driver_pool_config.get('type') == 'unlimited':
                print(f"\n○ Policy allows driver_instance_pool_id (unlimited)")
            else:
                print(f"\n○ Policy has driver_instance_pool_id configuration:")
                pprint(driver_pool_config)
    
    if 'node_type_id' in policy_def:
        node_config = policy_def['node_type_id']
        if isinstance(node_config, dict):
            if node_config.get('type') == 'fixed':
                defines_node_type = True
                print(f"\n✓ Policy DEFINES node_type_id as FIXED")
                print(f"  Value: {node_config.get('value')}")
            elif node_config.get('type') == 'unlimited':
                print(f"\n○ Policy allows node_type_id (unlimited)")
            else:
                print(f"\n○ Policy has node_type_id configuration:")
                pprint(node_config)
    
    print("\n" + "="*80)
    print("RECOMMENDATION FOR YOUR YAML CONFIG")
    print("="*80)
    
    if defines_instance_pool or defines_node_type:
        print("\n✅ Your policy DEFINES compute resources")
        print("\nIn validation_job_config.yml:")
        print("  → COMMENT OUT instance_pool_id and driver_instance_pool_id")
        print("  → Use this policy_id:")
        print(f"\n  policy_id: '{SELECTED_POLICY_ID}'")
        print("  # driver_instance_pool_id: 'xxx'  # Commented - policy defines it")
        print("  # instance_pool_id: 'xxx'         # Commented - policy defines it")
    else:
        print("\n⚠️  Your policy does NOT define compute resources")
        print("\nIn validation_job_config.yml:")
        print("  → You MUST specify instance_pool_id OR node_type_id")
        print("  → Use this policy_id:")
        print(f"\n  policy_id: '{SELECTED_POLICY_ID}'")
        print("  driver_instance_pool_id: 'GET_FROM_NEXT_CELL'")
        print("  instance_pool_id: 'GET_FROM_NEXT_CELL'")

except Exception as e:
    print(f"Error: {e}")
    print("\nMake sure to update SELECTED_POLICY_ID with a valid policy ID from the list above")

# COMMAND ----------

# DBTITLE 1,List All Instance Pools
print("="*80)
print("AVAILABLE INSTANCE POOLS")
print("="*80)

pools = list(w.instance_pools.list())
print(f"\nFound {len(pools)} instance pools:\n")

if len(pools) == 0:
    print("⚠️  No instance pools found in this workspace")
    print("\nYou have two options:")
    print("1. Create instance pools (recommended for cost efficiency)")
    print("2. Use node_type_id instead (see next cell)")
else:
    for pool in pools:
        print(f"Pool Name: {pool.instance_pool_name}")
        print(f"  Pool ID: {pool.instance_pool_id}")
        print(f"  Node Type: {pool.node_type_id}")
        print(f"  State: {pool.state}")
        print(f"  Idle Instances: {pool.stats.idle_count if pool.stats else 'N/A'}")
        print(f"  Used Instances: {pool.stats.used_count if pool.stats else 'N/A'}")
        print("-" * 80)
    
    print("\n" + "="*80)
    print("RECOMMENDED CONFIGURATION")
    print("="*80)
    
    # Find a suitable pool (active and available)
    active_pools = [p for p in pools if p.state.value == 'ACTIVE']
    if active_pools:
        recommended_pool = active_pools[0]
        print(f"\nRecommended Pool: {recommended_pool.instance_pool_name}")
        print(f"\nAdd to your validation_job_config.yml:")
        print(f"\n  driver_instance_pool_id: '{recommended_pool.instance_pool_id}'")
        print(f"  instance_pool_id: '{recommended_pool.instance_pool_id}'")

# COMMAND ----------

# DBTITLE 1,List Available Node Types (Alternative to Instance Pools)
print("="*80)
print("AVAILABLE NODE TYPES")
print("="*80)
print("\nIf you don't have instance pools, you can use node types directly.\n")

# Get available node types
node_types = list(w.clusters.list_node_types())

# Get cloud provider
cloud_provider = None
if node_types:
    first_node = node_types[0].node_type_id
    if first_node.startswith('Standard_'):
        cloud_provider = 'Azure'
    elif first_node.startswith('i') or first_node.startswith('m') or first_node.startswith('c'):
        cloud_provider = 'AWS'
    elif first_node.startswith('n'):
        cloud_provider = 'GCP'

print(f"Cloud Provider: {cloud_provider}\n")

# Show some common general-purpose node types
general_purpose = [nt for nt in node_types if nt.category == 'General Purpose'][:10]

print("Common General Purpose Node Types:")
print("-" * 80)
for nt in general_purpose:
    print(f"Node Type ID: {nt.node_type_id}")
    print(f"  Memory: {nt.memory_mb / 1024:.1f} GB")
    print(f"  Cores: {nt.num_cores}")
    print(f"  Category: {nt.category}")
    print("-" * 40)

print("\n" + "="*80)
print("RECOMMENDED NODE TYPE CONFIGURATION")
print("="*80)

if general_purpose:
    # Recommend a mid-sized node
    recommended_node = None
    for nt in general_purpose:
        if nt.num_cores >= 4 and nt.num_cores <= 8:
            recommended_node = nt
            break
    
    if not recommended_node:
        recommended_node = general_purpose[0]
    
    print(f"\nRecommended Node Type: {recommended_node.node_type_id}")
    print(f"  Cores: {recommended_node.num_cores}")
    print(f"  Memory: {recommended_node.memory_mb / 1024:.1f} GB")
    print(f"\nAdd to your validation_job_config.yml:")
    print(f"\n  node_type_id: '{recommended_node.node_type_id}'")
    print(f"  driver_node_type_id: '{recommended_node.node_type_id}'")
    print("\n  # Comment out instance pools if using node types:")
    print("  # driver_instance_pool_id: 'xxx'")
    print("  # instance_pool_id: 'xxx'")

# COMMAND ----------

# DBTITLE 1,List Service Principals (for spn_id)
print("="*80)
print("AVAILABLE SERVICE PRINCIPALS")
print("="*80)

try:
    spns = list(w.service_principals.list())
    print(f"\nFound {len(spns)} service principals:\n")
    
    if len(spns) == 0:
        print("⚠️  No service principals found")
        print("\nYou can:")
        print("1. Create a service principal in your workspace")
        print("2. Or comment out the spn_id in YAML to run jobs as yourself")
    else:
        for spn in spns:
            print(f"Display Name: {spn.display_name}")
            print(f"  Application ID: {spn.application_id}")
            print(f"  Active: {spn.active}")
            print("-" * 80)
        
        print("\n" + "="*80)
        print("CONFIGURATION")
        print("="*80)
        active_spns = [s for s in spns if s.active]
        if active_spns:
            recommended_spn = active_spns[0]
            print(f"\nRecommended Service Principal: {recommended_spn.display_name}")
            print(f"\nAdd to your validation_job_config.yml:")
            print(f"\n  spn_id: '{recommended_spn.application_id}'")
except Exception as e:
    print(f"Error listing service principals: {e}")
    print("\nYou may not have permission to list service principals.")
    print("Contact your workspace admin or comment out spn_id in YAML.")

# COMMAND ----------

# DBTITLE 1,Generate Complete YAML Configuration
print("="*80)
print("COMPLETE YAML CONFIGURATION TEMPLATE")
print("="*80)
print("\nBased on the discoveries above, here's your configuration:\n")

yaml_template = """# ==============================================================================
# Global Job Settings
# ==============================================================================

# REQUIRED: Cluster Policy ID
policy_id: '{policy_id}'

# OPTIONAL: Instance Pool Configuration
# - If your policy already defines instance pools: LEAVE THESE COMMENTED
# - If your policy does NOT define instance pools: UNCOMMENT these lines
# -----------------------------------------------------------------------------
{pool_config}
# -----------------------------------------------------------------------------

# Service Principal (optional - comment out to run as yourself)
{spn_config}

# Cluster configuration
min_workers: 2
max_workers: 4

cluster_tags:
  managed_by: validation_framework
  purpose: data_validation

job_tags:
  managed_by: validation_framework

timeout_seconds: 7200
warning_threshold_seconds: 3600

permissions:
  can_manage: group:admins
  can_manage_run: user:{user_email}
  can_view: group:analysts

# Individual job configs
jobs:
  - job_name: validation_test_job
    notebook_path: '/Workspace/Users/{user_email}/deltarecon/notebooks/main'
    schedule: null
    job_parameters:
      table_group: test_group
      iteration_suffix: test
      isFullValidation: "false"
    email_notification_list: {user_email}
    timeout_seconds: 1800
    warning_threshold_seconds: 1200
    max_concurrent_runs: 1
    is_queue_enabled: false
    pause_schedule: false
    tags:
      environment: test
"""

# Get user email
try:
    current_user = w.current_user.me()
    user_email = current_user.user_name
except:
    user_email = "your-email@example.com"

# Build pool config
pool_config_str = "# driver_instance_pool_id: 'YOUR_POOL_ID'\n# instance_pool_id: 'YOUR_POOL_ID'"
if len(pools) > 0 and pools[0].state.value == 'ACTIVE':
    pool_id = pools[0].instance_pool_id
    pool_config_str = f"driver_instance_pool_id: '{pool_id}'\ninstance_pool_id: '{pool_id}'"

# Build spn config
spn_config_str = "# spn_id: 'YOUR_SPN_ID'"
try:
    if len(spns) > 0 and spns[0].active:
        spn_config_str = f"spn_id: '{spns[0].application_id}'"
except:
    pass

formatted_yaml = yaml_template.format(
    policy_id=SELECTED_POLICY_ID,
    pool_config=pool_config_str,
    spn_config=spn_config_str,
    user_email=user_email
)

print(formatted_yaml)

print("\n" + "="*80)
print("NEXT STEPS")
print("="*80)
print("\n1. Copy the YAML configuration above")
print("2. Update validation_job_config.yml with these values")
print("3. Review and adjust any commented/uncommented fields based on the analysis")
print("4. Update notebook_path and job parameters as needed")
print("5. Run job_deployment.py with dry_run=Y to test")
print("6. If dry run looks good, run with dry_run=N to deploy")

# COMMAND ----------

# DBTITLE 1,Quick Validation Check
print("="*80)
print("QUICK VALIDATION CHECK")
print("="*80)

checks_passed = True

print("\n1. Checking cluster policy...")
try:
    policy_test = w.cluster_policies.get(policy_id=SELECTED_POLICY_ID)
    print(f"   ✅ Policy '{policy_test.name}' is accessible")
except Exception as e:
    print(f"   ❌ Cannot access policy: {e}")
    checks_passed = False

print("\n2. Checking instance pools...")
if len(pools) > 0:
    print(f"   ✅ Found {len(pools)} instance pool(s)")
    active_count = len([p for p in pools if p.state.value == 'ACTIVE'])
    print(f"   ✅ {active_count} pool(s) are ACTIVE")
else:
    print("   ⚠️  No instance pools found - you'll need to use node_type_id instead")

print("\n3. Checking permissions...")
try:
    current_user = w.current_user.me()
    print(f"   ✅ Running as: {current_user.user_name}")
except Exception as e:
    print(f"   ❌ Cannot get current user: {e}")
    checks_passed = False

if checks_passed:
    print("\n" + "="*80)
    print("✅ ALL CHECKS PASSED - You're ready to configure and deploy!")
    print("="*80)
else:
    print("\n" + "="*80)
    print("⚠️  SOME CHECKS FAILED - Review the issues above")
    print("="*80)

