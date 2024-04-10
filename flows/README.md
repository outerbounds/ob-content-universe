## Google Analytics Daily Sync

Run a workflow to fetch data from Google Analytics API from two days ago to yesterday.
This workflow pushes results to Snowflake.
This workflow should be run daily.

```
python daily_ga_sync.py --environment=pypi run
```

## Outerbounds Daily Documentation Site Scrape

```
python daily_ob_docs_site_scrape.py --environment=pypi run
```

## Transform in Snowflake, and Embed in LanceDB index
### Symlink the dbt directory
```
ln -s ../ob_content_universe_dbt ./ob_content_universe_dbt 
```


### Run the workflow
```
python transform_and_refresh_index.py run
```

## Google Analytics Historical Data Fill
TODO