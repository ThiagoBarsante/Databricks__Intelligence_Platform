CREATE SCHEMA IF NOT EXISTS gpt55_codex.sales_bronze
  COMMENT 'Bronze layer for raw sales transaction ingestion';

CREATE SCHEMA IF NOT EXISTS gpt55_codex.sales_silver
  COMMENT 'Silver layer for cleaned and enriched sales transactions';

CREATE SCHEMA IF NOT EXISTS gpt55_codex.sales_gold
  COMMENT 'Gold layer for dimensional sales marts and aggregates';
