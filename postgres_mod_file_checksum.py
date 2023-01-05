from datetime import datetime
import os
from zipfile import ZipFile
import logging
import json
import sys
import hashlib
import psycopg2
import psycopg2.extras
from itertools import chain
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
cur2 = dbh.cursor(cursor_factory = psycopg2.extras.RealDictCursor) #used for updates
psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)
psycopg2.extensions.register_adapter(list, psycopg2.extras.Json)
psycopg2.extensions.register_adapter(tuple, psycopg2.extras.Json)
stats = {}


stats['total_rows'] = 0;
stats['disk_files'] = 0;
stats['zip_files'] = 0;
stats['mod_files'] = 0;
stats['new_file_names'] = 0;
stats['new_file_paths'] = 0;
stats['directories'] = 0;
stats['directories_skipped'] = 0

log.info('Postgres mod filelist checksum START')

def refresh_mod_file_names():
    cur.execute('select * from mod_file_names');
    mod_file_names = {}
    for row in cur.fetchall():
        mod_file_names[row['filename']] = row['id']

    return mod_file_names

def refresh_mod_file_paths():
    cur.execute('select * from mod_file_paths');
    mod_file_paths = {}
    for row in cur.fetchall():
        mod_file_paths[row['path']] = row['id']

    return mod_file_paths

def refresh_mod_files():
    cur.execute("select id,encode(sha1, 'hex') as sha1 from mod_files");
    mod_files = {}
    for row in cur.fetchall():
        mod_files[row['sha1']] = row['id']

    return mod_files


mod_file_names = refresh_mod_file_names()
mod_file_paths = refresh_mod_file_paths()
mod_files = refresh_mod_files()

mods_finished = 'finished_mods/281990/'
for mod_uuid in os.listdir(mods_finished):
    ingest_error = False
    stats['directories'] += 1
    if os.path.exists('%s/%s/.file_ingest_complete_v2' % (mods_finished, mod_uuid)):
        stats['directories_skipped'] += 1
        continue


    disk_files = os.listdir('%s/%s' %(mods_finished, mod_uuid))
    files = []
    zip_files = []

    for filename in disk_files:
        if filename in ('.section_v1_ingest_complete', '.localization_v1_ingest_complete', '.file_ingest_complete', '.file_ingest_complete_v2'):
            continue

        files.append({
                'filename': filename,
                'filepath': '',
                'mod_uuid': mod_uuid,
                'filesize': os.path.getsize('%s/%s/%s' % (mods_finished, mod_uuid, filename)),
                'sha1': sha1sum('%s/%s/%s' % (mods_finished, mod_uuid, filename)),
        })

        if filename[-4:] == '.zip':
            log.info('Accessing zip file (%s) for mod (%s)' % (filename, mod_uuid))
            try:
                current_zip = ZipFile('%s/%s/%s' % (mods_finished, mod_uuid, filename), 'r')

                for info in current_zip.infolist():
                    try: 
                        fh = current_zip.open(info.filename)

                        zip_files.append({
                            'filename': os.path.basename(info.filename),
                            'filepath': os.path.dirname(info.filename),
                            'mod_uuid': mod_uuid,
                            'filesize': info.file_size,
                            'sha1': sha1sum_fh(fh),
                        })
    
                    except Exception as e:
                        ingest_error = True
                        log.warning('Exception opening file (%s) in zipfile (%s) for mod (%s): [%s]: %s' % (lename, filename, mod_uuid, type(e), e))
                    finally:
                        try:
                            fh.close()
                        except:
                            pass
            except Exception as e:
                log.error('Failed to open zip file (%s) for mod (%s), except: (%s %s)' % (filename, mod_uuid, e, type(e)))
                ingest_error = True
            finally:
                try:
                    current_zip.close()
                except:
                    pass


    new_filenames = []
    new_filepaths = []
    new_files = []

    for file in chain(files, zip_files):
        if file['filename'] not in mod_file_names:
            new_filenames.append((file['filename'],))
        if file['filepath'] not in mod_file_paths:
            new_filepaths.append((file['filepath'],))
        if file['sha1'] not in mod_files:
            new_files.append((bytes.fromhex(file['sha1']), file['filesize']))

    if(len(new_filenames)> 0):
        sql = 'insert into mod_file_names (filename) values '
        sql += ','.join(cur2.mogrify('(%s)', i).decode('utf-8') for i in new_filenames)
        sql += 'on conflict do nothing RETURNING *'

        cur2.execute(sql)
        for row in cur2.fetchall():
            mod_file_names[row['filename']] = row['id']

    if(len(new_filepaths) > 0):
        sql = 'insert into mod_file_paths (path) values '
        sql += ','.join(cur2.mogrify('(%s)', i).decode('utf-8') for i in new_filepaths)
        sql += ' on conflict do nothing RETURNING *'

        cur2.execute(sql)
        for row in cur2.fetchall():
            mod_file_paths[row['path']] = row['id']

    stats['new_file_names'] += len(new_filenames)
    stats['new_file_paths'] += len(new_filepaths)

    if(len(new_files) > 0):
        sql = "insert into mod_files (sha1, size) values "
        sql += ','.join(cur2.mogrify('(%s, %s)', i ).decode('utf-8') for i in new_files)
        sql += " on conflict do nothing RETURNING encode(sha1, 'hex') as sha1, id"
    
        cur2.execute(sql)
        for row in cur2.fetchall():
            mod_files[row['sha1']] = row['id']
    stats['disk_files'] += len(files)
    stats['zip_files'] += len(zip_files)

    if(len(files)+len(zip_files) > 0):
        sql = 'insert into mod_mod_files (mod_file_id, mod_uuid, mod_file_path_id, mod_file_name_id) values '
        sql += ','.join(cur2.mogrify('(%s, %s, %s, %s)', (
            mod_files[file['sha1']], 
            file['mod_uuid'], 
            mod_file_paths[file['filepath']], 
            mod_file_names[file['filename']]
            )).decode('utf-8') for file in chain(files, zip_files))

        sql += ' on conflict do nothing'
        cur2.execute(sql)
        stats['mod_files'] += cur2.rowcount

    dbh.commit()

    log.info('Found %s rows, %s files, %s hashed files on disk, %s hashed files in zip for mod uuid %s' % (cur.rowcount, len(disk_files), len(files), len(zip_files), mod_uuid))
    if(ingest_error == False):
        with open('%s/%s/.file_ingest_complete_v2' % (mods_finished, mod_uuid), 'w') as fh:
            fh.write(json.dumps({'stats': stats, 'disk_files': files, 'zip_files': zip_files}))

    


dbh.commit()




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
