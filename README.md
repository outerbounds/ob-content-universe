## How to use this repository

## One-time setup
🚨 Do not do this in the production Snowflake account unless you want to tear down and re-create the entire content universe!

### Warehouse
Snowflake setup queries

### dbt setup

#### dbt init

🚨 Do not do this unless you want to re-create the dbt project from scratch! 
The contents of `ob_content_universe` will otherwise already be populated.

Run this command to create the `ob_content_universe` directory for dbt:
```
dbt init
```
Answering the questions will lead to dbt append an entry to a `profiles.yml` file like:
```
ob_content_universe:
  outputs:
    dev:
      account: PFB48862
      database: ob_content_universe_db
      password: ...
      role: ob_content_universe_dbt_role
      schema: google_analytics_schema
      threads: 8
      type: snowflake
      user: ...
      warehouse: google_analytics_wh
  target: dev
```

#### Install dbt packages
Check 
```
dbt deps
```

Separate staging files, so staging files are 1:1 with source files.