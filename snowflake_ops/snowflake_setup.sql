-- Assuming role to perform admin tasks
use role accountadmin;

-- Create warehouse if it does not exist
create warehouse if not exists google_analytics_wh 
with warehouse_size = 'x-small' warehouse_type = 'standard' auto_suspend = 60 auto_resume = true;
create warehouse if not exists ob_docs_site_wh 
with warehouse_size = 'x-small' warehouse_type = 'standard' auto_suspend = 60 auto_resume = true;

-- Create database if it does not exist
create database if not exists ob_content_universe_db;

-- Create roles if they do not exist
create role if not exists ob_content_universe_mf_task_role;
create role if not exists ob_content_universe_dbt_role;

-- Create schema if it does not exist
create schema if not exists ob_content_universe_db.ob_docs_site_schema;

-- Grant usage on warehouse to roles
grant usage on warehouse google_analytics_wh to role ob_content_universe_mf_task_role;
grant usage on warehouse google_analytics_wh to role ob_content_universe_dbt_role;
grant usage on warehouse ob_docs_site_wh to role ob_content_universe_mf_task_role;
grant usage on warehouse ob_docs_site_wh to role ob_content_universe_dbt_role;

-- Grant privileges on the database
grant usage, create schema on database ob_content_universe_db to role ob_content_universe_mf_task_role;
grant usage, create schema on database ob_content_universe_db to role ob_content_universe_dbt_role;

-- Grant privileges on the schema
grant all on schema ob_content_universe_db.ob_docs_site_schema to role ob_content_universe_mf_task_role;
grant all on schema ob_content_universe_db.ob_docs_site_schema to role ob_content_universe_dbt_role;

-- Grant privileges on the tables 
grant select on all tables in database ob_content_universe_db to role ob_content_universe_mf_task_role;
grant select on all tables in database ob_content_universe_db to role ob_content_universe_dbt_role;
grant select on future tables in database ob_content_universe_db to role ob_content_universe_mf_task_role;
grant select on future tables in database ob_content_universe_db to role ob_content_universe_dbt_role;
grant select on all views in database ob_content_universe_db to role ob_content_universe_mf_task_role;
grant select on all views in database ob_content_universe_db to role ob_content_universe_dbt_role;
grant select on future views in database ob_content_universe_db to role ob_content_universe_mf_task_role;
grant select on future views in database ob_content_universe_db to role ob_content_universe_dbt_role;

-- Grant roles to users
grant role ob_content_universe_mf_task_role to user eddieouterbounds;
grant role ob_content_universe_dbt_role to user eddieouterbounds;