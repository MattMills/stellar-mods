"""Ingest several Steam workshop snapshots in one set-based transaction.

Produces the same rows as running postgres_mod_metadata_ingest.py on each
snapshot in turn, but faster: LOADERS connections read and parse the JSON
pages server-side (pg_read_file) in parallel into the unlogged staging table
ingest_stage_occ, then one transaction writes every table with a few
set-based statements:

  mods       one upsert per (publishedfileid, creator_appid, consumer_appid,
             revision_change_number) for the whole batch: columns the per-page
             upsert updates take the latest sighting, insert-only columns the
             first (what sequential upserts leave behind)
  mod_stats  one row per sighting whose (publishedfileid, revision) exists by
             that page, as the per-page dict check allows
  files      existing keys get last_seen = the last sighting's snapshot; new
             keys are inserted with last_seen = now() if seen once, else the
             last sighting's snapshot (insert-then-update, as sequentially)
  ingest_event  one row per snapshot with the same counters

Pages are taken in numeric order within a snapshot (the per-page script used
os.listdir order); this only matters for a mod listed twice in one snapshot.
The merge is one transaction: a failure leaves nothing behind, so a retry
cannot duplicate mod_stats rows. Snapshots that already have an ingest_event
are skipped, so re-running a batch after a crash between commit and zip is
safe.

usage: postgres_batch_ingest.py APPID SNAPSHOT_DIR [SNAPSHOT_DIR ...]
       (directories in processing order; $INGEST_DB, default stellar-mods;
        $INGEST_LOADERS, default 8)
"""
import logging
import os
import platform
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import psycopg2
import psycopg2.extras

DB = os.environ.get('INGEST_DB', 'stellar-mods')
LOADERS = int(os.environ.get('INGEST_LOADERS', '8'))  # parallel parsing connections
# ingest_event types that record a fully ingested snapshot folder
INGEST_TYPES = ('./postgres_mod_metadata_ingest.py', 'postgres_mod_metadata_ingest.py',
                './postgres_batch_ingest.py', 'postgres_batch_ingest.py')
SENTINEL_PREVIEW = '18446744073709551615'  # Steam's "no preview" hcontent_preview
# A listing missing any of these makes the per-page script's mogrify raise: a failed mod.
MOD_KEYS = ['creator_appid', 'consumer_appid', 'consumer_shortcutid', 'time_created', 'time_updated',
            'visibility', 'flags', 'workshop_file', 'workshop_accepted', 'banned', 'ban_reason', 'banner',
            'can_be_deleted', 'file_type', 'can_subscribe', 'language', 'maybe_inappropriate_sex',
            'maybe_inappropriate_violence', 'revision_change_number', 'ban_text_check_result', 'title',
            'file_description', 'creator']

# Staging for the parallel loaders: one row per listed mod, flattened to typed
# columns. Unlogged (it is rebuilt from the snapshot folders on any failure)
# and emptied at the start of every batch; only one ingest runs at a time.
SQL_STAGE_DDL = r"""
create unlogged table if not exists ingest_stage_occ (
  snap_idx int, page int, idx bigint, snap text, parse_date timestamp,
  pfid bigint, rcn int, valid boolean,
  creator_appid int, consumer_appid int, consumer_shortcutid int, steam_time_created int, steam_time_updated int,
  steam_visibility boolean, flags int, workshop_file boolean, workshop_accepted boolean, banned boolean,
  ban_reason text, banner bigint, can_be_deleted boolean, file_type int, can_subscribe boolean, language int,
  maybe_inappropriate_sex boolean, maybe_inappropriate_violence boolean, rcn_strict int,
  ban_text_check_result int, title text, description text, creator_steam_id bigint,
  num_comments_public int, subscriptions int, favorited int, followers int, lifetime_subscriptions int,
  lifetime_favorited int, lifetime_followers int, views int, num_children int, num_reports int,
  votes_score float8, votes_up int, votes_down int,
  hcontent_preview text, preview_url text, preview_file_size bigint,
  filename text, url text, file_size bigint, hcontent_file numeric, previews jsonb
);
"""

# Created in each loader connection. Reads, parses and flattens every page of
# one snapshot server-side; returns the page count. A page that is not JSON is
# counted and skipped, as the per-page script does.
SQL_LOADER_FN = r"""
create function pg_temp.load_snapshot(si int, dir text) returns int language plpgsql as $$
declare f text; n int := 0; doc jsonb; sn text := regexp_replace(dir, '^.*/', '');
begin
  for f in select pg_ls_dir(dir) order by 1 loop
    n := n + 1;
    begin
      doc := pg_read_file(dir || '/' || f)::jsonb;
    exception when others then
      continue;
    end;
    if jsonb_typeof(doc->'response'->'publishedfiledetails') is distinct from 'array' then
      continue;
    end if;
    insert into ingest_stage_occ
    select si, nullif(regexp_replace(f, '\D', '', 'g'), '')::int, e.ord, sn, sn::timestamp,
           (x->>'publishedfileid')::bigint,
           case when x->>'revision_change_number' ~ '^\s*-?\d+\s*$' then (x->>'revision_change_number')::int end,
           v,
           -- mod columns only for valid listings, cast strictly (a bad value fails like the insert would)
           case when v then (x->>'creator_appid')::int end, case when v then (x->>'consumer_appid')::int end,
           case when v then (x->>'consumer_shortcutid')::int end, case when v then (x->>'time_created')::int end,
           case when v then (x->>'time_updated')::int end, case when v then (x->>'visibility')::numeric <> 0 end,
           case when v then (x->>'flags')::int end, case when v then (x->>'workshop_file')::boolean end,
           case when v then (x->>'workshop_accepted')::boolean end, case when v then (x->>'banned')::boolean end,
           case when v then x->>'ban_reason' end, case when v then (x->>'banner')::bigint end,
           case when v then (x->>'can_be_deleted')::boolean end, case when v then (x->>'file_type')::int end,
           case when v then (x->>'can_subscribe')::boolean end, case when v then (x->>'language')::int end,
           case when v then (x->>'maybe_inappropriate_sex')::boolean end,
           case when v then (x->>'maybe_inappropriate_violence')::boolean end,
           case when v then (x->>'revision_change_number')::int end,
           case when v then (x->>'ban_text_check_result')::int end,
           case when v then x->>'title' end,
           -- description is insert-only: only a revision not yet in mods needs it (~27 MB a snapshot otherwise)
           case when v then case when not exists (select 1 from mods m where m.publishedfileid = (x->>'publishedfileid')::bigint
                                                  and m.revision_change_number = (x->>'revision_change_number')::int)
                            then x->>'file_description' end end,
           case when v then (x->>'creator')::bigint end,
           (x->>'num_comments_public')::int, (x->>'subscriptions')::int, (x->>'favorited')::int,
           (x->>'followers')::int, (x->>'lifetime_subscriptions')::int, (x->>'lifetime_favorited')::int,
           (x->>'lifetime_followers')::int, (x->>'views')::int, (x->>'num_children')::int, (x->>'num_reports')::int,
           (x->'vote_data'->>'score')::float8, (x->'vote_data'->>'votes_up')::int, (x->'vote_data'->>'votes_down')::int,
           x->>'hcontent_preview', x->>'preview_url', (x->>'preview_file_size')::bigint,
           x->>'filename', x->>'url', (x->>'file_size')::bigint, (x->>'hcontent_file')::numeric,
           case when jsonb_typeof(x->'previews') = 'array' then x->'previews' end
    from jsonb_array_elements(doc->'response'->'publishedfiledetails') with ordinality e(x, ord)
    cross join lateral (select x ?& %(mod_keys)s as v) vv;
  end loop;
  return n;
end $$;
"""

SQL_MERGE_SETUP = r"""
set local temp_buffers = '4GB';  -- keep the temp tables in memory
set local work_mem = '1GB';
create temp table snaps (snap_idx int, snap text, file_count int) on commit drop;
create temp view occ as select * from ingest_stage_occ;
"""

SQL_EXPAND = r"""
analyze ingest_stage_occ;

-- What the per-page dict held before the batch.
create temp table pre_pfid on commit drop as
select distinct publishedfileid as pfid from mods where publishedfileid in (select pfid from occ);
create temp table pre_key on commit drop as
select publishedfileid as pfid, revision_change_number as rcn from mods
where (publishedfileid, revision_change_number) in (select pfid, rcn from occ where rcn is not null);

-- First page (in batch order) that upserted each publishedfileid / key.
create temp table first_pfid on commit drop as
select distinct on (pfid) pfid, snap_idx, page from occ where valid order by pfid, snap_idx, page;
create temp table first_key on commit drop as
select distinct on (pfid, rcn) pfid, rcn, snap_idx, page from occ where valid order by pfid, rcn, snap_idx, page;

-- Per sighting: was it in the dict before its page (counters), and after its
-- page's upsert (stats and files)?
create temp table occ_flags on commit drop as
select o.snap_idx, o.page, o.idx,
       (pp.pfid is not null or coalesce((fp.snap_idx, fp.page) < (o.snap_idx, o.page), false)) as pfid_before,
       (pk.pfid is not null or coalesce((fk.snap_idx, fk.page) < (o.snap_idx, o.page), false)) as key_before,
       (pk.pfid is not null or coalesce((fk.snap_idx, fk.page) <= (o.snap_idx, o.page), false)) as key_after
from occ o
left join pre_pfid pp on pp.pfid = o.pfid
left join first_pfid fp on fp.pfid = o.pfid
left join pre_key pk on pk.pfid = o.pfid and pk.rcn = o.rcn
left join first_key fk on fk.pfid = o.pfid and fk.rcn = o.rcn;
"""

SQL_MODS = r"""
insert into mods (publishedfileid, creator_appid, consumer_appid, consumer_shortcutid,
                  steam_time_created, steam_time_updated, steam_visibility, flags,
                  workshop_file, workshop_accepted, banned, ban_reason,
                  banner, can_be_deleted, file_type, can_subscribe,
                  language, maybe_inappropriate_sex, maybe_inappropriate_violence,
                  revision_change_number, ban_text_check_result, title, our_time_updated, description, creator_steam_id)
select f.pfid, f.creator_appid, f.consumer_appid, f.consumer_shortcutid,
       f.steam_time_created, l.steam_time_updated, l.steam_visibility, l.flags,
       l.workshop_file, l.workshop_accepted, l.banned, l.ban_reason,
       l.banner, l.can_be_deleted, l.file_type, l.can_subscribe,
       l.language, l.maybe_inappropriate_sex, l.maybe_inappropriate_violence,
       f.rcn_strict, l.ban_text_check_result, l.title, l.parse_date, f.description, f.creator_steam_id
from (select distinct on (pfid, creator_appid, consumer_appid, rcn_strict) * from occ where valid
      order by pfid, creator_appid, consumer_appid, rcn_strict, snap_idx, page, idx) f
join (select distinct on (pfid, creator_appid, consumer_appid, rcn_strict) * from occ where valid
      order by pfid, creator_appid, consumer_appid, rcn_strict, snap_idx desc, page desc, idx desc) l
  using (pfid, creator_appid, consumer_appid, rcn_strict)
on conflict on constraint mod_uniqueness_constraint do update set
  our_time_updated = excluded.our_time_updated, steam_time_updated = excluded.steam_time_updated,
  steam_visibility = excluded.steam_visibility, flags = excluded.flags,
  workshop_file = excluded.workshop_file, workshop_accepted = excluded.workshop_accepted,
  banned = excluded.banned, ban_reason = excluded.ban_reason, banner = excluded.banner,
  can_be_deleted = excluded.can_be_deleted, file_type = excluded.file_type,
  can_subscribe = excluded.can_subscribe, language = excluded.language,
  maybe_inappropriate_sex = excluded.maybe_inappropriate_sex,
  maybe_inappropriate_violence = excluded.maybe_inappropriate_violence,
  ban_text_check_result = excluded.ban_text_check_result, title = excluded.title;
"""

SQL_STATS_FILES = r"""
-- Sightings the per-page script writes stats and files for, with their mod uuid.
create temp table elig on commit drop as
select o.*, m.uuid as mod_uuid
from occ o
join occ_flags fl using (snap_idx, page, idx)
join mods m on m.publishedfileid = o.pfid and m.revision_change_number = o.rcn
where fl.key_after;

insert into mod_stats (mod_uuid, num_comments_public, subscriptions, favorited,
                       followers, lifetime_subscriptions, lifetime_favorited, lifetime_followers,
                       views, num_children, num_reports, votes_score, votes_up, votes_down, timestamp)
select mod_uuid, num_comments_public, subscriptions, favorited,
       followers, lifetime_subscriptions, lifetime_favorited, lifetime_followers,
       views, num_children, num_reports, votes_score, votes_up, votes_down, parse_date
from elig;

-- Every files row the per-page script would upsert, in sighting order.
create temp table fr on commit drop as
select snap_idx, page, idx, 0 as sub, parse_date, mod_uuid,
       (select id from file_types where type = 'hcontent_preview') as file_type,
       hcontent_preview as file_name, preview_url as file_url, preview_file_size as file_size,
       0 as sort_order, hcontent_preview::numeric as steam_id
from elig where hcontent_preview <> %(sentinel)s
union all
select snap_idx, page, idx, 1, parse_date, mod_uuid,
       (select id from file_types where type = 'hcontent_file'),
       filename, url, file_size, 0, hcontent_file
from elig
union all
select e.snap_idx, e.page, e.idx, 1 + pv.ord, e.parse_date, e.mod_uuid, ft.id,
       case pt when 1 then pv.p->>'youtubevideoid' when 2 then pv.p->>'external_reference' else pv.p->>'filename' end,
       case pt when 1 then 'https://www.youtube.com/watch?v=' || (pv.p->>'youtubevideoid')
               when 2 then 'https://sketchfab.com/3d-models/' || (pv.p->>'external_reference')
               else pv.p->>'url' end,
       case when pt in (1, 2) then 0 else (pv.p->>'size')::bigint end,
       (pv.p->>'sortorder')::int, (pv.p->>'previewid')::numeric
from elig e
cross join lateral jsonb_array_elements(e.previews) with ordinality pv(p, ord)
cross join lateral (select (pv.p->>'preview_type')::int as pt) t
join file_types ft on ft.type = 'preview_type_' || t.pt
-- the per-page script skips a preview that raises KeyError
where e.previews is not null
  and pv.p ?& array['preview_type', 'sortorder', 'previewid']
  and case pt when 1 then pv.p ? 'youtubevideoid' when 2 then pv.p ? 'external_reference'
              else pv.p ?& array['filename', 'url', 'size'] end;

-- Collapse to one row per unique key. Rows with a null in the key never
-- conflict (unique indexes treat nulls as distinct), so they stay one per sighting.
create temp table fc on commit drop as
select mod_uuid, file_type, file_name, file_url, file_size, sort_order, steam_id,
       count(*) as n, (array_agg(parse_date order by snap_idx desc, page desc, idx desc, sub desc))[1] as last_parse
from fr
where mod_uuid is not null and file_type is not null and file_name is not null and file_url is not null
  and file_size is not null and sort_order is not null and steam_id is not null
group by 1, 2, 3, 4, 5, 6, 7
union all
select mod_uuid, file_type, file_name, file_url, file_size, sort_order, steam_id, 1, parse_date
from fr
where mod_uuid is null or file_type is null or file_name is null or file_url is null
   or file_size is null or sort_order is null or steam_id is null;
analyze fc;

update files f set last_seen = c.last_parse, delete_timestamp = null
from fc c
where f.mod_uuid = c.mod_uuid and f.file_type = c.file_type and f.file_url = c.file_url and f.file_size = c.file_size
  and f.steam_id = c.steam_id and f.sort_order = c.sort_order and f.file_name = c.file_name;

insert into files (mod_uuid, file_type, file_name, file_url, file_size, sort_order, steam_id, last_seen)
select mod_uuid, file_type, file_name, file_url, file_size, sort_order, steam_id,
       case when n > 1 then last_parse else now() end
from fc
on conflict on constraint file_uniqueness_constraint do nothing;
"""

SQL_EVENTS = r"""
select s.snap_idx, s.snap, s.file_count,
       count(o.*) as total_mod_count,
       count(*) filter (where fl.pfid_before) as existing_mod_count,
       count(*) filter (where fl.pfid_before and fl.key_before) as existing_mod_update_count,
       count(*) filter (where fl.pfid_before and not fl.key_before) as existing_mod_new_revision_count,
       count(*) filter (where o.snap_idx is not null and not o.valid) as failed_mod_count
from snaps s
left join occ o on o.snap_idx = s.snap_idx
left join occ_flags fl on fl.snap_idx = o.snap_idx and fl.page = o.page and fl.idx = o.idx
group by s.snap_idx, s.snap, s.file_count order by s.snap_idx;
"""


def setup_log(start):
    log = logging.getLogger('batch_ingest')
    log.setLevel(logging.INFO)
    fmt = logging.Formatter('[%(asctime)s] (%(levelname)s): %(message)s')
    logdir = f'logs/{os.path.basename(sys.argv[0])}'
    os.makedirs(logdir, exist_ok=True)
    for h in (logging.FileHandler(f'{logdir}/{start}.log'), logging.StreamHandler(sys.stdout)):
        h.setFormatter(fmt)
        log.addHandler(h)
    return log


def main():
    if len(sys.argv) < 3:
        sys.exit('usage: postgres_batch_ingest.py APPID SNAPSHOT_DIR [SNAPSHOT_DIR ...]')
    given = [d.rstrip('/') for d in sys.argv[2:]]
    missing = [d for d in given if not os.path.isdir(d)]
    if missing:
        sys.exit(f'not a directory: {missing}')
    start = datetime.now()
    log = setup_log(start)
    log.info('start: %d snapshots, %s .. %s', len(given), os.path.basename(given[0]), os.path.basename(given[-1]))
    timings = []

    dbh = psycopg2.connect(f'dbname={DB} user=postgres')
    cur = dbh.cursor()
    psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)

    # A snapshot with an ingest_event is fully ingested (the event commits with
    # its data), e.g. when a run died between committing and zipping: skip it,
    # the loop still archives it.
    cur.execute("select distinct substring(config_metadata->>'folder' from '([^/]+)/?$') from ingest_event "
                "where type in %s and config_metadata->>'folder' like any(%s)",
                (INGEST_TYPES, ['%/' + os.path.basename(d) + '/' for d in given]))
    done = {r[0] for r in cur.fetchall()}
    dirs = [d for d in given if os.path.basename(d) not in done]
    if done:
        log.info('already ingested, archive only: %s', ' '.join(sorted(done)))
    if not dirs:
        log.info('nothing to ingest')
        return
    cur.execute(SQL_STAGE_DDL)
    cur.execute('truncate ingest_stage_occ')
    dbh.commit()

    # Parse in parallel: each loader connection flattens whole snapshots into
    # the staging table. Nothing shared is written here, so order is irrelevant.
    t = time.time()
    local, conns, conns_lock = threading.local(), [], threading.Lock()

    def load(job):
        i, d = job
        if not hasattr(local, 'conn'):
            local.conn = psycopg2.connect(f'dbname={DB} user=postgres')
            with conns_lock:
                conns.append(local.conn)
            with local.conn.cursor() as k:
                k.execute(SQL_LOADER_FN, {'mod_keys': MOD_KEYS})
        with local.conn.cursor() as k:
            k.execute('select pg_temp.load_snapshot(%s, %s)', (i, os.path.abspath(d)))
            n = k.fetchone()[0]
        local.conn.commit()
        return n

    try:
        with ThreadPoolExecutor(min(LOADERS, len(dirs))) as pool:
            file_counts = list(pool.map(load, enumerate(dirs)))
    finally:
        for c in conns:
            c.close()
    timings.append(('read+parse', time.time() - t))

    # Merge, in batch order, as one transaction.
    def step(name, sql, params=None):
        t = time.time()
        cur.execute(sql, params)
        timings.append((name, time.time() - t))

    step('setup', SQL_MERGE_SETUP)
    psycopg2.extras.execute_values(cur, 'insert into snaps values %s',
                                   [(i, os.path.basename(d), n) for i, (d, n) in enumerate(zip(dirs, file_counts))])
    step('dict state', SQL_EXPAND)
    step('mods', SQL_MODS)
    step('stats+files', SQL_STATS_FILES, {'sentinel': SENTINEL_PREVIEW})
    step('events', SQL_EVENTS)
    events = cur.fetchall()
    end = datetime.now()
    for snap_idx, snap, file_count, total, existing, updates, new_revs, failed in events:
        stats = {'file_count': file_count, 'total_mod_count': total, 'file_mod_count': 0,
                 'existing_mod_count': existing, 'existing_mod_update_count': updates,
                 'existing_mod_new_revision_count': new_revs, 'failed_mod_count': failed}
        cur.execute('insert into ingest_event (start_timestamp, end_timestamp, stats, type, config_metadata) '
                    'values (%s, %s, %s, %s, %s)',
                    (start, end, stats, sys.argv[0],
                     {'host': platform.node(), 'python_version': platform.python_version(), 'argv': sys.argv,
                      'pid': os.getpid(), 'folder': dirs[snap_idx] + '/', 'batch': len(dirs)}))
        log.info('%s: %s', snap, stats)
    cur.execute('truncate ingest_stage_occ')
    t = time.time()
    dbh.commit()
    timings.append(('commit', time.time() - t))
    total = sum(s for _, s in timings)
    log.info('done: %d snapshots in %.1f s (%.2f s each): %s', len(dirs), total, total / len(dirs),
             ', '.join(f'{n} {s:.1f}' for n, s in timings))


if __name__ == '__main__':
    main()
