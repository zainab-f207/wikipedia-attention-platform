-- One row per page per hour, with a real timestamp and a flag
select
    (date::timestamp + make_interval(hours => hour)) as view_hour,
    page_title,
    views_desktop,
    views_mobile,
    views_total,
    has_colon,
    is_main_page,
    (not has_colon and not is_main_page) as is_article_candidate
from {{ source('raw', 'pageviews_hourly') }}
