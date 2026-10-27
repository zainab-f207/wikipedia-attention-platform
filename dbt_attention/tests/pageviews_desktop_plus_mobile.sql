-- Fails if desktop + mobile does not add up to the total
select view_hour, page_title
from {{ ref('stg_pageviews') }}
where views_desktop + views_mobile <> views_total
