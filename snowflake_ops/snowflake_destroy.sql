use role accountadmin;
drop warehouse if exists google_analytics_wh;
drop warehouse if exists ob_docs_site_wh;
drop database if exists ob_content_universe_db;
drop role if exists ob_content_universe_mf_task_role;
drop role if exists ob_content_universe_dbt_role;