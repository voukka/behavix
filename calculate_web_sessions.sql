create or replace view sessions_with_domains as
with ordered_events as (
	select event_id, event_time, user_id, domain from read_parquet('{input_file}')
),
clusters as (
	select
		user_id, event_id, event_time, domain,
		case when event_time - lag(event_time) over(partition by user_id order by event_time) <= interval 30 minutes then 0 else 1 end as cluster_start,
		row_number() over (partition by user_id order by event_time) as rn
	from ordered_events
),
cluster_groups as (
	select user_id, event_id, event_time, domain, sum(cluster_start) over(partition by user_id order by event_time) as cluster_id
	from clusters
),
sessions as (
	select user_id, cluster_id as session_id, event_id, event_time, domain,
	lead(event_time) over(partition by user_id, cluster_id order by event_time) as next_event_time,
	extract (epoch from (next_event_time - event_time)) as event_duration
	from cluster_groups
),
domains as (
	select user_id, session_id, domain, sum(event_duration) as domain_duration
	from sessions
	group by user_id, session_id, domain
)
select s.session_id,s.event_id, s.event_duration, d.domain_duration
from sessions s left join domains d on s.user_id = d.user_id and s.session_id = d.session_id and s.domain = d.domain;