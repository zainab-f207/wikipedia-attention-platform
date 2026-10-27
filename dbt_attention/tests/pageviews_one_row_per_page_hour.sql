-- Fails if the same page appears twice in the same hour
select view_hour, page_title, count(*) as copies
from {{ ref('stg_pageviews') }}
group by 1, 2
having count(*) > 1
