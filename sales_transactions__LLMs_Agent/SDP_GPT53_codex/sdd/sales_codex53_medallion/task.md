# Sales Codex53 Medallion Task Plan

## Phase 1: Prepare
1. Confirm active Databricks workspace/profile.
2. Validate source volume path and inferred source schema.
3. Create new development schemas for bronze/silver/gold.

## Phase 2: Bronze First
1. Implement bronze streaming table SQL with schema evolution and ingest metadata.
2. Upload bronze SQL to Databricks workspace files.
3. Create/update bronze-only serverless pipeline.
4. Execute bronze full refresh and wait for completion.
5. Validate bronze output:
   - table exists
   - row count > 0 and expected near source count
   - metadata columns exist
   - sample rows query succeeds

## Phase 3: Silver
1. Implement silver streaming table SQL with cleaning and enrichment.
2. Ensure no dedup logic is present.
3. Validate silver row flow and typed columns.

## Phase 4: Gold
1. Implement SCD2 dimension views for small entities.
2. Implement SCD1 fact view for larger transactional entity.
3. Implement two aggregate gold metric views.
4. Run full pipeline refresh and validate outputs.

## Phase 5: Iteration and Cleanup
1. If rerunning with new schema names, drop previous dev schemas (cascade).
2. Recreate schemas and redeploy.
3. Capture final validation evidence in execution summary.
