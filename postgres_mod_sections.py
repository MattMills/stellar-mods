from datetime import datetime
import os
from zipfile import ZipFile,BadZipFile
import logging
import json
import sys
import psycopg2
import psycopg2.extras

from parse_pdxscript import parser,check_path_pdxscript_txt,parse_zip_file


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

log.info('Postgres mod section load start')

# Connect to Postgres
dbh = psycopg2.connect("dbname=stellar-mods user=postgres") or logging.critical('Unable to connect to database');
cur = dbh.cursor(cursor_factory = psycopg2.extras.RealDictCursor) #results return associative dicts instead of tuples.
cur2 = dbh.cursor() #used for updates
psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)
psycopg2.extensions.register_adapter(list, psycopg2.extras.Json)
psycopg2.extensions.register_adapter(tuple, psycopg2.extras.Json)
stats = {}


stats['total_rows'] = 0;
stats['total_zip_files'] = 0;
stats['skipped_mods'] = 0;
stats['skipped_files'] = 0;
stats['error_zip_files'] = 0;


stats['total_sections'] = 0;
stats['total_files'] = 0;
stats['total_parsed_files'] = 0
last_file = '' #used for debug



# Fetch list of mods and file uuids from the database
#cur.execute(
#    'select mods.uuid as mod_uuid, mods_filelist.uuid as file_uuid, filename from mods '
#    '  left join mods_filelist on (mods.uuid = mods_filelist.mod_uuid)'
#    " where "#our_time_created >= now() - INTERVAL '15 day'"
#    #"  and 
#    "lower(mods_filelist.filename) like '%.txt'"
#    )
cur.execute(
        'WITH limit_mmf AS( '
'    SELECT '
'        * '
'        ,row_number() OVER (PARTITION BY mod_file_id) as row_num '
'    FROM mod_mod_files '
' ) '
'select limit_mmf.mod_uuid, mf.id as mod_file_id, mod_file_paths.path, mod_file_names.filename '
'from mod_files mf '
'left join limit_mmf on (limit_mmf.mod_file_id = mf.id and limit_mmf.row_num = 1) '
'left join mod_file_paths on (limit_mmf.mod_file_path_id = mod_file_paths.id) '
'left join mod_file_names on (limit_mmf.mod_file_name_id = mod_file_names.id) '
'where '
"    mod_file_names.filename like '%.txt' "
"      and  mod_file_names.filename not like 'manifest_%.txt' "
'  and ( '
"    mod_file_paths.path like 'common%' "
"    or mod_file_paths.path like 'events%' "
"    or mod_file_paths.path like 'flags%' "
"    or mod_file_paths.path like 'map%' "
"    or mod_file_paths.path like 'music%' "
"    or mod_file_paths.path like 'prescripted_countries%' "
"    or mod_file_paths.path like 'interface/resource_groups%' "
"    or mod_file_paths.path like 'gfx/advisorwindow%' "
"    or mod_file_paths.path like 'gfx/pingmap%' "
"    or mod_file_paths.path like 'gfx/portraits/asset_selectors%' "
"    or mod_file_paths.path like 'gfx/portraits/portraits%' "
"    or mod_file_paths.path like 'gfx/projectiles%' "
"    or mod_file_paths.path like 'gfx/shipview%' "
"    or mod_file_paths.path like 'gfx/worldgfx%' "
'  ) '
'      and mf.id not in (select distinct mod_file_id from sections) '
'      and mf.size > 0 '
'order by mod_uuid asc, path asc, filename asc '
)

insert_todo = []

mods_finished = 'finished_mods/281990/'
last_mod_uuid = ''
target_file_aggr = []
filename_to_id = {}

for row in cur.fetchall():
    mod_uuid = row['mod_uuid']
    mod_file_id = row['mod_file_id']
    file_path = row['path']
    file_name = row['filename']
    target_file = '%s/%s' % (file_path, file_name)


    stats['total_rows'] += 1

    if last_mod_uuid == '':
        last_mod_uuid = mod_uuid


    if last_mod_uuid == mod_uuid:
        target_file_aggr.append(target_file)
        filename_to_id[target_file] = mod_file_id
        continue


    try:
        dirs = os.listdir('%s%s' % (mods_finished, last_mod_uuid))
    except Exception as e:
        log.warning('Unable to list dirs for %s, (%s) %s' % (last_mod_uuid, type(e), e))
        continue

    for f in dirs:
        if f.lower()[-4:] != '.zip':
            continue

        stats['total_zip_files'] += 1
        zip_filename = '%s%s/%s' % (mods_finished, last_mod_uuid, f)
        
        log.info('Accessing zip file (%s) for mod (%s)' % (f, last_mod_uuid))

        try: 
            for section in parse_zip_file(stats, zip_filename, target_file_aggr, True, False):
                insert_todo.append((section['order'], section['section'], filename_to_id[section['filename']]))
        except BadZipFile as e:
            log.warning('Encountered bad/broken zip file (%s) for mod (%s): [%s]: %s' % (f, last_mod_uuid, type(e),e))
        except Exception as e:
            log.warning('Unknown Exception parsing zipfile (%s) for mod (%s): [%s]: %s' % (f, last_mod_uuid, type(e), e))
            #raise

    last_mod_uuid = mod_uuid
    target_file_aggr = []
    target_file_aggr.append(target_file)

    filename_to_id = {}
    filename_to_id[target_file] = mod_file_id


    if len(insert_todo) >= 1000:
        sql = 'insert into sections (section_order, section, mod_file_id) values '
        sql += ','.join(cur2.mogrify('(%s, %s, %s)', i ).decode('utf-8') for i in insert_todo)
        sql += " on conflict do nothing"
        sql = sql.replace("\u0000", "\\\\u0000").replace("\\u0000", "\\\\u0000")

        cur.execute(sql)
        dbh.commit()
        insert_todo = []
   

if len(insert_todo) > 0:
    sql = 'insert into sections (section_order, section, mod_file_id) values '
    sql += ','.join(cur2.mogrify('(%s, %s, %s)', i ).decode('utf-8') for i in insert_todo)
    sql += " on conflict do nothing"
    sql = sql.replace("\u0000", "\\\\u0000").replace("\\u0000", "\\\\u0000")
    cur.execute(sql)
    dbh.commit()

log.info('Final stats: %s' % (stats))

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
