-- Fails if the edit counts in the final table differ from the staging data
select 1
from (
    select
        (select sum(edits) from {{ ref('fct_edits_hourly') }}) as in_mart,
        (select count(*) from {{ ref('stg_edits') }}
          where is_article_edit and is_complete_hour) as in_staging
) t
where in_mart <> in_staging
