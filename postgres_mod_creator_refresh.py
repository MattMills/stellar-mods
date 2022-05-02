from datetime import datetime
import os
from zipfile import ZipFile
import logging
import json
import gzip
import sys
import requests
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
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)

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

log.info('Postgres creator refresh start')

# Connect to Postgres
dbh = psycopg2.connect("dbname=stellar-mods user=postgres") or logging.critical('Unable to connect to database');
cur = dbh.cursor()#cursor_factory = psycopg2.extras.RealDictCursor) #results return associative dicts instead of tuples.
psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)
psycopg2.extensions.register_adapter(list, psycopg2.extras.Json)
psycopg2.extensions.register_adapter(tuple, psycopg2.extras.Json)
stats = {
        'users_rowcount': 0,
        'steam_usercount': 0,
        }


page = 0
limit = 100

url = 'http://api.steampowered.com/ISteamUser/GetPlayerSummaries/v2/'
params = {}
with open('SECRET_STEAM_API_KEY', 'r') as fh:
    params['key'] = fh.read().strip()

app_id = 281990
output_dir = 'steam_creator_data/%s/%s' % (app_id, datetime.utcnow().isoformat())
try:
    os.makedirs(output_dir)
except:
    pass


# zcat steam_creator_data/281990/2022-05-02T06\:13\:34.310471/*.json.gz  | jq '.response .players[] | keys' | sort | uniq -c
#   9759 [
#   9759 ]
#   9759   "avatar",
#   9759   "avatarfull",
#   9759   "avatarhash",
#   9759   "avatarmedium",
#   3321   "commentpermission",
#   9759   "communityvisibilitystate",
#    282   "gameextrainfo",
#    282   "gameid",
#     17   "gameserverip",
#     17   "gameserversteamid",
#      2   "lastlogoff",
#     32   "lobbysteamid",
#   1642   "loccityid",
#   5239   "loccountrycode",
#   2924   "locstatecode",
#   9759   "personaname",
#   9759   "personastate",
#   8003   "personastateflags",
#   8003   "primaryclanid",
#   9611   "profilestate",
#   9759   "profileurl",
#   3993   "realname",
#   1756   "steamid"
#   8003   "steamid",
#   8003   "timecreated"

db_columns = (
    'steamid', 'communityvisibilitystate', 'profilestate', 'personaname', 
    'commentpermission', 'profileurl', 'avatar', 'avatarmedium', 
    'avatarfull', 'avatarhash', 'personastate', 'primaryclanid', 
    'timecreated', 'personastateflags'
    )

while(True):
    cur.execute("select string_agg(creator_steam_id::text, ',') from (select distinct creator_steam_id from mods order by creator_steam_id asc limit %s offset %s) as tbl", (limit, page*limit));
    users = cur.fetchall()[0][0]

    if cur.rowcount <= 0 or users == None:
        break
    stats['users_rowcount'] += users.count(',')+1
    
    # do fetch steam
    params['steamids'] = users

    result = requests.get(url, params=params)
    with gzip.open('%s/%s.json.gz' % (output_dir, page), 'w') as fh:
        fh.write(result.text.encode('utf-8'))
    user_data = json.loads(result.text)['response']['players']
    stats['steam_usercount'] += len(user_data)

    insert_query = (
        'insert into steam_users ( '
        'steamid, communityvisibilitystate, profilestate, personaname, '
        'commentpermission, profileurl, avatar, avatarmedium, '
        'avatarfull, avatarhash, personastate, primaryclanid, '
        'steam_timecreated, personastateflags, other '
        ' ) values '
    ).encode('utf-8')

    for user in user_data:
        other = {}
        for key in user.keys():
            if key not in db_columns:
                other[key] = user[key]
        for key in db_columns:
            if key not in user:
                user[key] = None

        user['other'] = other
        insert_query += cur.mogrify(('('
                '%(steamid)s, %(communityvisibilitystate)s, %(profilestate)s, %(personaname)s, '
                '%(commentpermission)s, %(profileurl)s, %(avatar)s, %(avatarmedium)s, '
                '%(avatarfull)s, %(avatarhash)s, %(personastate)s, %(primaryclanid)s, '
                '%(timecreated)s, %(personastateflags)s, %(other)s '
                '),'),
                user
                )
    insert_query = insert_query[:-1] #remove last comma
    insert_query += (
        ' ON CONFLICT ON CONSTRAINT steam_users_steamid_pkey_constraint DO UPDATE SET timestamp = now(), '
        ' communityvisibilitystate = EXCLUDED.communityvisibilitystate, '
        ' profilestate = EXCLUDED.profilestate, '
        ' personaname = EXCLUDED.personaname, '
        ' commentpermission = EXCLUDED.commentpermission, '
        ' profileurl = EXCLUDED.profileurl, '
        ' avatar = EXCLUDED.avatar, '
        ' avatarmedium = EXCLUDED.avatarmedium, '
        ' avatarfull = EXCLUDED.avatarfull, '
        ' avatarhash = EXCLUDED.avatarhash, '
        ' personastate = EXCLUDED.personastate, '
        ' primaryclanid = EXCLUDED.primaryclanid, '
        ' steam_timecreated = EXCLUDED.steam_timecreated, '
        ' personastateflags = EXCLUDED.personastateflags, '
        ' other = EXCLUDED.other'
        ).encode('utf-8')
        
    cur.execute(insert_query)
    dbh.commit()

    log.info('Postgres steam creator refresh - got %s records from steam - Batch %s / %s' % (len(user_data), users.count(',')+1, page*limit,))
    page += 1
    
log.info('Postgres steam creator refresh complete - Users from DB: %s  / Users from Steam: %s' % ( stats['users_rowcount'], stats['steam_usercount'] ))

stats['pages'] = page
stats['limit'] = limit




import platform
import sys
import os
config_metadata = {
    'host': platform.node(),
    'python_version': platform.python_version(),
    'user': os.getlogin(),
    'argv': sys.argv,
    'pid' : os.getpid(),
    'output_dir': output_dir,
    'app_id': app_id,
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
