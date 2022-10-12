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
stats['error_zip_files'] = 0;


stats['total_sections'] = 0;
stats['total_files'] = 0;
stats['total_parsed_files'] = 0
last_file = '' #used for debug



# Fetch list of mods and file uuids from the database
cur.execute(
    'select mods.uuid as mod_uuid, mods_filelist.uuid as file_uuid, filename from mods '
    '  left join mods_filelist on (mods.uuid = mods_filelist.mod_uuid)'
    " where "#our_time_created >= now() - INTERVAL '15 day'"
    #"  and 
    "lower(mods_filelist.filename) like '%.txt'"
    )
mod_files = {}

for row in cur.fetchall():
    stats['total_rows'] += 1
    if row['mod_uuid'] not in mod_files:
        mod_files[row['mod_uuid']] = {}
    mod_files[row['mod_uuid']][row['filename']] = row['file_uuid']



stats['db_mod_count'] = len(mod_files)

mods_finished = 'finished_mods/281990/'
for mod_uuid in mod_files.keys():
    error = False
    if os.path.exists('%s/%s/.section_v1_ingest_complete' % (mods_finished, mod_uuid)):
        stats['skipped_mods'] += 1
        continue

    try:
        dirs = os.listdir('%s%s' % (mods_finished, mod_uuid))
    except Exception as e:
        log.warning('Unable to list dirs for %s, (%s) %s' % (mod_uuid, type(e), e))
        continue

    for f in dirs:
        if f.lower()[-4:] != '.zip':
            continue

        stats['total_zip_files'] += 1
        zip_filename = '%s%s/%s' % (mods_finished, mod_uuid, f)
        
        log.info('Accessing zip file (%s) for mod (%s)' % (f, mod_uuid))

        try: 
            for section in parse_zip_file(stats, zip_filename, True, False):
                if section['filename'] not in mod_files[mod_uuid]:
                    error = True
                    continue
                section['file_uuid'] = mod_files[mod_uuid][section['filename']]
                cur.execute(cur.mogrify('insert into sections_v1 (file_uuid, section_order, section) values (%(file_uuid)s, %(order)s, %(section)s) on conflict do nothing', section))

        except BadZipFile as e:
            log.warning('Encountered bad/broken zip file (%s) for mod (%s): [%s]: %s' % (f, mod_uuid, type(e),e))
        except Exception as e:
            log.warning('Unknown Exception parsing zipfile (%s) for mod (%s): [%s]: %s' % (f, mod_uuid, type(e), e))
            #raise
        finally:
            log.info(stats)
            dbh.commit()
    if error == False:
        with open('%s/%s/.section_v1_ingest_complete' % (mods_finished, mod_uuid), 'w') as fh:
            fh.write(json.dumps({'stats': stats}))


   

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
