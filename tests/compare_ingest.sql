-- Row-level fingerprint of everything the ingest writes, independent of
-- generated uuids and now() defaults. New files rows get last_seen = now()
-- on insert, so test-time values are folded to 'now'.
\pset footer off
select 'mods' as tbl, count(*), md5(string_agg(j, E'\n' order by j)) from (
  select (to_jsonb(m) - 'uuid' - 'our_time_created')::text j from mods m) t
union all
select 'files', count(*), md5(string_agg(j, E'\n' order by j)) from (
  select (to_jsonb(f) - 'uuid' - 'mod_uuid' - 'timestamp' - 'last_seen'
          || jsonb_build_object(
               'mod', jsonb_build_array(m.publishedfileid, m.creator_appid, m.consumer_appid, m.revision_change_number),
               'last_seen', case when f.last_seen >= '2026-10-01' then 'now' else f.last_seen::text end))::text j
  from files f left join mods m on m.uuid = f.mod_uuid) t
union all
select 'mod_stats', count(*), md5(string_agg(j, E'\n' order by j)) from (
  select (to_jsonb(s) - 'uuid' - 'mod_uuid'
          || jsonb_build_object('mod', jsonb_build_array(m.publishedfileid, m.creator_appid, m.consumer_appid, m.revision_change_number)))::text j
  from mod_stats s left join mods m on m.uuid = s.mod_uuid) t
union all
select 'ingest_event.stats', count(*), md5(string_agg(stats::text, E'\n' order by id)) from ingest_event;
