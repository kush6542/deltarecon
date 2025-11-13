"""
Diagnostic script to understand why no new batches are being found

Copy and paste this entire cell into a Databricks notebook and run it
"""

# === CONFIGURATION ===
table_group = "incremntal_test_group"
sample_table = "ai_27.gold.customer_po_partition_overwrite"  # Change this to one of your tables

print("=" * 100)
print("DIAGNOSTIC: Why are there no new batches?")
print("=" * 100)

# === QUERY 1: Check what batches exist for the sample table ===
print(f"\n1. ALL BATCHES for {sample_table} (last 20):")
print("-" * 100)
spark.sql(f"""
    SELECT 
        batch_load_id,
        status,
        operation_type,
        start_ts,
        end_ts
    FROM ts42_demo.migration_operations.serving_ingestion_audit
    WHERE target_table_name = '{sample_table}'
    ORDER BY batch_load_id DESC
    LIMIT 20
""").show(20, truncate=False)

# === QUERY 2: Count batches by operation_type ===
print(f"\n2. BATCH COUNTS by operation_type for {sample_table}:")
print("-" * 100)
spark.sql(f"""
    SELECT 
        operation_type,
        status,
        COUNT(*) as count
    FROM ts42_demo.migration_operations.serving_ingestion_audit
    WHERE target_table_name = '{sample_table}'
    GROUP BY operation_type, status
    ORDER BY count DESC
""").show(truncate=False)

# === QUERY 3: Check incremental batches specifically ===
print(f"\n3. INCREMENTAL BATCHES ONLY (operation_type='incremental_batch_ingestion'):")
print("-" * 100)
spark.sql(f"""
    SELECT 
        batch_load_id,
        status,
        operation_type,
        start_ts
    FROM ts42_demo.migration_operations.serving_ingestion_audit
    WHERE target_table_name = '{sample_table}'
        AND operation_type = 'incremental_batch_ingestion'
    ORDER BY batch_load_id DESC
    LIMIT 20
""").show(20, truncate=False)

# === QUERY 4: Check validation status of batches ===
print(f"\n4. VALIDATION STATUS of incremental batches:")
print("-" * 100)
spark.sql(f"""
    SELECT 
        t2.batch_load_id,
        t2.status as ingestion_status,
        t2.operation_type,
        t3.validation_run_status,
        t3.validation_run_start_time
    FROM ts42_demo.migration_operations.serving_ingestion_audit t2
    LEFT JOIN (
        SELECT 
            batch_load_id,
            validation_run_status,
            validation_run_start_time,
            row_number() OVER(PARTITION BY batch_load_id ORDER BY validation_run_start_time DESC) as rnk
        FROM cat_ril_nayeem_03.validation_v2.validation_log
    ) t3 ON t3.batch_load_id = t2.batch_load_id AND t3.rnk = 1
    WHERE t2.target_table_name = '{sample_table}'
    ORDER BY t2.batch_load_id DESC
    LIMIT 20
""").show(20, truncate=False)

# === QUERY 5: Check ALL tables in the group ===
print(f"\n5. ALL TABLES in group '{table_group}':")
print("-" * 100)
spark.sql(f"""
    SELECT 
        concat_ws('.', target_catalog, target_schema, target_table) as table_name,
        write_mode,
        is_active
    FROM ts42_demo.migration_operations.serving_ingestion_config
    WHERE group_name = '{table_group}'
""").show(truncate=False)

# === QUERY 6: Summary across all tables in group ===
print(f"\n6. SUMMARY: Unvalidated batches across ALL tables in '{table_group}':")
print("-" * 100)
spark.sql(f"""
    SELECT 
        t2.target_table_name,
        t2.batch_load_id,
        t2.status as ingestion_status,
        t2.operation_type,
        t3.validation_run_status,
        CASE 
            WHEN t3.validation_run_status IS NULL THEN 'UNVALIDATED'
            WHEN t3.validation_run_status = 'SUCCESS' THEN 'ALREADY_VALIDATED'
            ELSE 'VALIDATION_FAILED'
        END as validation_category
    FROM ts42_demo.migration_operations.serving_ingestion_audit t2
    INNER JOIN ts42_demo.migration_operations.serving_ingestion_config t4
        ON t2.target_table_name = concat_ws('.', t4.target_catalog, t4.target_schema, t4.target_table)
        AND t2.group_name = t4.group_name
    LEFT JOIN (
        SELECT 
            batch_load_id,
            validation_run_status,
            validation_run_start_time,
            row_number() OVER(PARTITION BY batch_load_id ORDER BY validation_run_start_time DESC) as rnk
        FROM cat_ril_nayeem_03.validation_v2.validation_log
    ) t3 ON t3.batch_load_id = t2.batch_load_id AND t3.rnk = 1
    WHERE t2.status = 'COMPLETED'
        AND t4.group_name = '{table_group}'
        AND t2.operation_type = 'incremental_batch_ingestion'
    ORDER BY t2.target_table_name, t2.batch_load_id DESC
""").show(100, truncate=False)

# === QUERY 7: Count unvalidated batches by table ===
print(f"\n7. COUNT of unvalidated batches per table:")
print("-" * 100)
spark.sql(f"""
    SELECT 
        t2.target_table_name,
        COUNT(*) as total_incremental_batches,
        COUNT(CASE WHEN t2.status = 'COMPLETED' THEN 1 END) as completed_batches,
        COUNT(CASE WHEN t3.validation_run_status IS NULL THEN 1 END) as unvalidated_batches,
        COUNT(CASE WHEN t3.validation_run_status = 'SUCCESS' THEN 1 END) as validated_success
    FROM ts42_demo.migration_operations.serving_ingestion_audit t2
    INNER JOIN ts42_demo.migration_operations.serving_ingestion_config t4
        ON t2.target_table_name = concat_ws('.', t4.target_catalog, t4.target_schema, t4.target_table)
        AND t2.group_name = t4.group_name
    LEFT JOIN (
        SELECT 
            batch_load_id,
            validation_run_status,
            row_number() OVER(PARTITION BY batch_load_id ORDER BY validation_run_start_time DESC) as rnk
        FROM cat_ril_nayeem_03.validation_v2.validation_log
    ) t3 ON t3.batch_load_id = t2.batch_load_id AND t3.rnk = 1
    WHERE t4.group_name = '{table_group}'
        AND t2.operation_type = 'incremental_batch_ingestion'
    GROUP BY t2.target_table_name
    ORDER BY t2.target_table_name
""").show(truncate=False)

print("\n" + "=" * 100)
print("DIAGNOSIS COMPLETE")
print("=" * 100)
print("""
WHAT TO LOOK FOR:

Query 1-2: Check if batches exist and what their operation_type values are
  → If operation_type is NOT 'incremental_batch_ingestion', that's your problem!
  
Query 3: Check if you have any incremental batches at all
  → If this shows 0 rows, you need to check your ingestion process
  
Query 4: Check if batches are already validated
  → If all show validation_run_status='SUCCESS', there are no NEW batches to validate
  
Query 6-7: Summary showing which batches are unvalidated
  → If unvalidated_batches = 0 for all tables, everything is already validated!

COMMON ISSUES:
1. operation_type is NULL or has wrong value → Fix in ingestion process
2. All batches already validated with SUCCESS → This is expected behavior
3. No COMPLETED batches → Wait for ingestion to complete
""")

