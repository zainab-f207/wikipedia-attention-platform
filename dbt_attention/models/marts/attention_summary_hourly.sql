-- One row per hour: are edited pages viewed more than the average page?
with all_pages as (
    select
        view_hour as hour,
        count(*) as pages_viewed,
        sum(views_total) as total_views
    from {{ ref('stg_pageviews') }}
    where is_article_candidate
    group by 1
),
edited_pages as (
    select
        hour,
        count(*) as edited_pages,
        sum(edits) as total_edits,
        avg(views_total) as avg_views_edited_page
    from {{ ref('fct_page_attention') }}
    group by 1
)
select
    a.hour,
    a.pages_viewed,
    a.total_views,
    round(a.total_views::numeric / a.pages_viewed, 2) as avg_views_per_page,
    e.edited_pages,
    e.total_edits,
    round(e.avg_views_edited_page::numeric, 2) as avg_views_edited_page
from all_pages a
join edited_pages e on e.hour = a.hour
