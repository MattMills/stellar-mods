from datetime import datetime
import os
from zipfile import ZipFile
import logging
import json
import sys
import hashlib
import psycopg2
import psycopg2.extras

import hashlib


#https://stackoverflow.com/questions/22058048/hashing-a-file-in-python/44873382#44873382
def sha1sum(filename):
    with open(filename, 'rb', buffering=0) as fh:
        return sha1sum_fh(fh)

def sha1sum_fh(fh):
    h  = hashlib.sha1()
    b  = bytearray(128*1024)
    mv = memoryview(b)

    while n := fh.readinto(mv):
        h.update(mv[:n])

    return h.hexdigest()

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

log.info('Postgres mod file checksum start')

# Connect to Postgres
dbh = psycopg2.connect("dbname=stellar-mods user=postgres") or logging.critical('Unable to connect to database');
cur = dbh.cursor(cursor_factory = psycopg2.extras.RealDictCursor) #results return associative dicts instead of tuples.
cur2 = dbh.cursor() #used for updates
psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)
psycopg2.extensions.register_adapter(list, psycopg2.extras.Json)
psycopg2.extensions.register_adapter(tuple, psycopg2.extras.Json)
stats = {}


stats['total_rows'] = 0;
stats['found_files'] = 0;
stats['not_found_files'] = 0;
stats['new_files'] = 0;
stats['directories'] = 0;
stats['directories_skipped'] = 0

log.info('Postgres mod filelist checksum START')


mods_finished = 'finished_mods/281990/'
for mod_uuid in os.listdir(mods_finished):
    stats['directories'] += 1
    if os.path.exists('%s/%s/.file_ingest_complete' % (mods_finished, mod_uuid)):
        stats['directories_skipped'] += 1
        continue

    cur.execute("select uuid, mod_uuid, filename, filesize, sha1 from mods_filelist where mod_uuid = %s order by mod_uuid, uuid, filename desc", (mod_uuid,));


    database_files = {}
    missing_database_files = {}
    for row in cur.fetchall():
        database_files[row['filename']] = {
                'filename': row['filename'],
                'uuid': row['uuid'],
                'mod_uuid': row['mod_uuid'],
                'filesize': row['filesize'],
                'sha1': row['sha1'],
                }


    disk_files = os.listdir('%s/%s' %(mods_finished, mod_uuid))
    found_files_sha1 = {}
    zip_files = []

    for filename in disk_files:
        if filename in ('.section_v1_ingest_complete', '.localization_v1_ingest_complete'):
            continue
        if filename not in database_files:
            missing_database_files[filename] = {
                    'filename': filename,
                    'mod_uuid': mod_uuid,
                    'filesize': os.path.getsize('%s/%s/%s' % (mods_finished, mod_uuid, filename)),
                    'sha1': sha1sum('%s/%s/%s' % (mods_finished, mod_uuid, filename)),
                }
            log.warning('File (%s) missing from database for mod (%s)' % (filename, mod_uuid))
        else:
            found_files_sha1[filename] = sha1sum('%s/%s/%s' % (mods_finished, mod_uuid, filename))

        if filename[-4:] == '.zip':
            log.info('Accessing zip file (%s) for mod (%s)' % (filename, mod_uuid))
            try:
                current_zip = ZipFile('%s/%s/%s' % (mods_finished, mod_uuid, filename), 'r')
            except Exception as e:
                log.error('Failed to open zip file (%s) for mod (%s), except: (%s %s)' % (filename, mod_uuid, e, type(e)))
            for inner_filename in database_files:
                if filename in found_files_sha1:
                    continue #we already found this file on disk and generated an sha1
                try: 
                    fh = current_zip.open(inner_filename)
                    found_files_sha1[inner_filename] = sha1sum_fh(fh)
                except Exception as e:
                    log.warning('Exception opening file (%s) in zipfile (%s) for mod (%s): [%s]: %s' % (inner_filename, filename, mod_uuid, type(e), e))
                finally:
                    fh.close()

            current_zip.close()


   
    zipped_files = []

    for filename in found_files_sha1:
        cur2.execute('update mods_filelist set sha1 = %s where uuid = %s', (found_files_sha1[filename], database_files[filename]['uuid']))

    dbh.commit()
    log.info('Found %s rows, %s files, %s new files, %s hashed files on disk for mod uuid %s' % (cur.rowcount, len(disk_files), len(missing_database_files), len(found_files_sha1), mod_uuid))
    if(cur.rowcount == len(found_files_sha1)):
        with open('%s/%s/.file_ingest_complete' % (mods_finished, mod_uuid), 'w') as fh:
            fh.write(json.dumps({'stats': stats, 'disk_files': disk_files, 'missing_database_files': missing_database_files, 'found_files_sha1': found_files_sha1}))

    

    stats['total_rows'] += cur.rowcount

dbh.commit()

stats['daily_rowcount'] = cur.rowcount




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
