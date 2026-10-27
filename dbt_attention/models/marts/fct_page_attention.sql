-- Each edited page-hour with how many people viewed the page that hour
select
    e.edit_hour as hour,
    e.page_title,
    e.edits,
    e.bot_edits,
    e.human_edits,
    e.net_bytes,
    coalesce(p.views_total, 0) as views_total,
    coalesce(p.views_desktop, 0) as views_desktop,
    coalesce(p.views_mobile, 0) as views_mobile,
    (p.page_title is not null) as has_pageview_row
from {{ ref('fct_edits_hourly') }} e
left join {{ ref('stg_pageviews') }} p
    on p.view_hour = e.edit_hour
   and p.page_title = e.page_title
