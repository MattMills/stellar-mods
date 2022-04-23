from datetime import datetime
import os
from zipfile import ZipFile
import logging
import json
import sys
import psycopg2
import psycopg2.extras


script_name = sys.argv[0]

start_time = datetime.now()

log_folder = 'logs/%s/' % (script_name, )

# Setup logging
try:
    os.makedirs(log_folder)
except:
    pass

log = logging.getLogger()
log.setLevel(logging.DEBUG)

class msecFormatter(logging.Formatter):
    converter= datetime.fromtimestamp
    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            s = ct.strftime("%Y-%m-%dT%H:%M:%S.%f")
        return s

log_formatter = msecFormatter(fmt='[%(asctime)s] %(name)s (%(levelname)s): %(message)s')

log_file_handler = logging.FileHandler('%s%s.log' % (log_folder, start_time,), )
log_file_handler.setLevel(logging.INFO)
log_file_handler.setFormatter(log_formatter)

log_console_handler = logging.StreamHandler()
log_console_handler.setLevel(logging.DEBUG)
log_console_handler.setFormatter(log_formatter)

log.addHandler(log_file_handler)
log.addHandler(log_console_handler)

log.info('Postgres mod stats daily summary refresh start')

# Connect to Postgres
dbh = psycopg2.connect("dbname=stellar-mods user=postgres") or logging.critical('Unable to connect to database');
cur = dbh.cursor(cursor_factory = psycopg2.extras.RealDictCursor) #results return associative dicts instead of tuples.
cur2 = dbh.cursor() #used for updates
psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)
psycopg2.extensions.register_adapter(list, psycopg2.extras.Json)
psycopg2.extensions.register_adapter(tuple, psycopg2.extras.Json)

cur.execute(

'insert into mod_stats_summary_daily '
'(start_time, end_time, publishedfileid, num_comments_public, subscriptions, favorited, followers, lifetime_subscriptions, lifetime_favorited, lifetime_followers, views, num_children, num_reports, votes_score, votes_up, votes_down) ' 
'with daily_interval as ( '
"select start_time, start_time + '1 day'::interval as end_time from ( "
" SELECT date_trunc('day', last_day) as start_time "
'  FROM generate_series( ' 
'   ( '
'    coalesce( '
"     (select max(start_time) - '2 days'::interval as timestamp from mod_stats_summary_daily), "
'     (select min(timestamp) from mod_stats), '
"     (select  now() - '2 days'::interval as timestamp) "
'    ) '
'   ), '
'   (select max(timestamp) from mod_stats), '
"   '1 day':: interval "
') last_day '
') as start_times ) '
'select '
'  p.start_time as start_time, p.end_time as end_time, '
'  mods.publishedfileid as publishedfileid, '
'  coalesce(max(m.num_comments_public)-min(m.num_comments_public), 0) as num_comments_public, '
'  coalesce(max(m.subscriptions)-min(m.subscriptions), 0) as subscriptions, '
'  coalesce(max(m.favorited)-min(m.favorited), 0) as favorited, '
'  coalesce(max(m.followers)-min(m.followers), 0) as followers, '
'  coalesce(max(m.lifetime_subscriptions)-min(m.lifetime_subscriptions), 0) as lifetime_subscriptions, '
'  coalesce(max(m.lifetime_favorited)-min(m.lifetime_favorited), 0) as lifetime_favorited, '
'  coalesce(max(m.lifetime_followers)-min(m.lifetime_followers), 0) as lifetime_followers, '
'  coalesce(max(m.views)-min(m.views), 0) as views, '
'  coalesce(max(m.num_children)-min(m.num_children), 0) as num_children, '
'  coalesce(max(m.num_reports)-min(m.num_reports), 0) as num_reports, '
'  coalesce(max(m.votes_score)-min(m.votes_score), 0) as votes_score, '
'  coalesce(max(m.votes_up)-min(m.votes_up), 0) as votes_up, '
'  coalesce(max(m.votes_down)-min(m.votes_down), 0) as votes_down '
'from mod_stats m '
' right join daily_interval p on (m.timestamp >= p.start_time and m.timestamp <= p.end_time) '
' join mods on (mods.uuid = m.mod_uuid) '
' where mods.publishedfileid is not null '
' group by p.start_time, p.end_time, mods.publishedfileid '
' order by p.start_time '
'  on conflict on constraint mod_stats_period_publishedfileid_constraint '
' do update set '
'   num_comments_public = EXCLUDED.num_comments_public, '
'   subscriptions = EXCLUDED.subscriptions, '
'   favorited = EXCLUDED.favorited, '
'   followers = EXCLUDED.followers, '
'   lifetime_subscriptions = EXCLUDED.lifetime_subscriptions, '
'   lifetime_followers = EXCLUDED.lifetime_followers, '
'   views = EXCLUDED.views, '
'   num_children = EXCLUDED.num_children, '
'   votes_score = EXCLUDED.votes_score, '
'   votes_up = EXCLUDED.votes_up, '
'   votes_down = EXCLUDED.votes_down '
        );

dbh.commit()
log.info('Postgres mod stats daily summary refresh start - Rows: %s' % ( cur.rowcount))
log.info('Postgres mod stats 7 day summary refresh start')
cur.execute(

'insert into mod_stats_summary_7day '
'(start_time, end_time, publishedfileid, num_comments_public, subscriptions, favorited, followers, lifetime_subscriptions, lifetime_favorited, lifetime_followers, views, num_children, num_reports, votes_score, votes_up, votes_down) '
'with day7_interval as ( '
"select start_time, start_time + '7 days'::interval as end_time from ( "
" SELECT date_trunc('hour', last_7day) as start_time "
'  FROM generate_series( '
'   ( '
'    coalesce( '
"     (select max(start_time) as timestamp from mod_stats_summary_7day), "  # Use max(start_time) from mod_stats_summary_7day if it is populated with data
'     (select min(timestamp) from mod_stats), '                             # If not, use the minimum timestamp from mod_stats, since the summary table is empty
"     (select  now() - '7 days'::interval as timestamp) "                   # Else start a week ago for sanity (dunno why it'd ever hit this)
'    ) '
'   ), '
"   (select max(timestamp) - '7 days'::interval from mod_stats), "                               # Up to our latest data
"   '1 hour':: interval "
') last_7day '
') as start_times ) '
'select '
'  p.start_time as start_time, p.end_time as end_time, '
'  mods.publishedfileid as publishedfileid, '
'  coalesce(max(m.num_comments_public)-min(m.num_comments_public), 0) as num_comments_public, '
'  coalesce(max(m.subscriptions)-min(m.subscriptions), 0) as subscriptions, '
'  coalesce(max(m.favorited)-min(m.favorited), 0) as favorited, '
'  coalesce(max(m.followers)-min(m.followers), 0) as followers, '
'  coalesce(max(m.lifetime_subscriptions)-min(m.lifetime_subscriptions), 0) as lifetime_subscriptions, '
'  coalesce(max(m.lifetime_favorited)-min(m.lifetime_favorited), 0) as lifetime_favorited, '
'  coalesce(max(m.lifetime_followers)-min(m.lifetime_followers), 0) as lifetime_followers, '
'  coalesce(max(m.views)-min(m.views), 0) as views, '
'  coalesce(max(m.num_children)-min(m.num_children), 0) as num_children, '
'  coalesce(max(m.num_reports)-min(m.num_reports), 0) as num_reports, '
'  coalesce(max(m.votes_score)-min(m.votes_score), 0) as votes_score, '
'  coalesce(max(m.votes_up)-min(m.votes_up), 0) as votes_up, '
'  coalesce(max(m.votes_down)-min(m.votes_down), 0) as votes_down '
'from mod_stats m '
' right join day7_interval p on (m.timestamp >= p.start_time and m.timestamp <= p.end_time) '
' join mods on (mods.uuid = m.mod_uuid) '
' where mods.publishedfileid is not null '
' group by p.start_time, p.end_time, mods.publishedfileid '
' order by p.start_time '
'  on conflict on constraint mod_stats_summary_7day_start_time_end_time_publishedfileid_key '
' do update set '
'   num_comments_public = EXCLUDED.num_comments_public, '
'   subscriptions = EXCLUDED.subscriptions, '
'   favorited = EXCLUDED.favorited, '
'   followers = EXCLUDED.followers, '
'   lifetime_subscriptions = EXCLUDED.lifetime_subscriptions, '
'   lifetime_followers = EXCLUDED.lifetime_followers, '
'   views = EXCLUDED.views, '
'   num_children = EXCLUDED.num_children, '
'   votes_score = EXCLUDED.votes_score, '
'   votes_up = EXCLUDED.votes_up, '
'   votes_down = EXCLUDED.votes_down '
        );
dbh.commit()
log.info('Postgres mod stats 7 day summary refresh start - Rows: %s' % ( cur.rowcount))
import platform
import sys
import os
config_metadata = {
    'host': platform.node(),
    'python_version': platform.python_version(),
    'user': os.getlogin(),
    'argv': sys.argv,
    'pid' : os.getpid(),
}

cur.execute(
    'insert into ingest_event '
    '(start_timestamp, end_timestamp, stats, type, config_metadata) '
    'values ( %s, %s, %s, %s, %s)',
    (start_time, datetime.now(), stats, script_name, config_metadata)
)




dbh.commit()


cur.close()
dbh.close()
