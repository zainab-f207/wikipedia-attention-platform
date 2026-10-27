-- One row per English Wikipedia edit event, with an hour column and a flag
select
    event_id,
    event_ts,
    date_trunc('hour', event_ts) as edit_hour,
    page_title,
    namespace,
    type,
    is_bot,
    byte_change,
    is_complete_hour,
    (namespace = 0 and type in ('edit', 'new')) as is_article_edit
from {{ source('raw', 'edits') }}
