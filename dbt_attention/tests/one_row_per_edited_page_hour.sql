-- Fails if the join created extra rows
select hour, page_title, count(*) as copies
from {{ ref('fct_page_attention') }}
group by 1, 2
having count(*) > 1
