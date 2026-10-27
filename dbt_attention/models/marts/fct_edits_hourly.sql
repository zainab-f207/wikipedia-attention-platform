-- Article edits per page per hour (complete hours only)
select
    edit_hour,
    page_title,
    count(*) as edits,
    sum(case when coalesce(is_bot, false) then 1 else 0 end) as bot_edits,
    sum(case when coalesce(is_bot, false) then 0 else 1 end) as human_edits,
    sum(byte_change) as net_bytes
from {{ ref('stg_edits') }}
where is_article_edit
  and is_complete_hour
group by 1, 2
