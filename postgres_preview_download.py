import psycopg2
import psycopg2.extras
import time
from datetime import datetime
import os
import logging
import sys
import subprocess
import re
import gzip
import shutil
import glob
import hashlib
import json
import requests
#import zipfile

script_name = sys.argv[0]

def get_hashes(filename):
        md5 = hashlib.md5()
        sha1 = hashlib.sha1()
        sha256 = hashlib.sha256()
        with open(filename, 'rb') as fh:
            while True:
                data = fh.read(65536)
                if not data:
                    break
                sha1.update(data)
                md5.update(data)
                sha256.update(data)

        hashes = {
                'file': filename,
                'md5': md5.hexdigest(),
                'sha1': sha1.hexdigest(),
                'sha256': sha256.hexdigest()
                }

        return hashes


start_time = datetime.now()

# Setup logging
try:
    os.makedirs('logs/%s/' % (script_name, ))
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

log_formatter = msecFormatter(fmt='[%(asctime)s] (%(levelname)s): %(message)s')

log_file_handler = logging.FileHandler('logs/%s/%s.log' % (script_name, start_time,), )
log_file_handler.setLevel(logging.INFO)
log_file_handler.setFormatter(log_formatter)

log_console_handler = logging.StreamHandler()
log_console_handler.setLevel(logging.DEBUG)
log_console_handler.setFormatter(log_formatter)

log.addHandler(log_file_handler)
log.addHandler(log_console_handler)

log.info('Steam workshop preview download start')



# Connect to Postgres
dbh = psycopg2.connect("dbname=stellar-mods user=postgres") or logging.critical('Unable to connect to database');
cur = dbh.cursor(cursor_factory = psycopg2.extras.RealDictCursor) #results return associative dicts instead of tuples.
cur2 = dbh.cursor() #used for updates
psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)
psycopg2.extensions.register_adapter(list, psycopg2.extras.Json)
psycopg2.extensions.register_adapter(tuple, psycopg2.extras.Json)



existing_file_detail = {}
seen_file_detail = {}

cur.execute(
        'select '
        '  files.uuid as file_uuid,'
        '  mod_uuid,file_types.type,'
        '  mods.creator_appid,'
        '  file_name,' 
        '  file_url,'
        '  file_size,'
        '  sort_order,'
        '  hashes,'
        '  steam_id,'
        '  last_seen,'
        '  mods.publishedfileid,'
        '  mods.revision_change_number,'
        '  file_time,'
        '  fetch_time'
        ' from files'
        '  join file_types on (file_types.id = files.file_type)'
        '  join mods on (mod_uuid = mods.uuid)'
        ' where '
        "  type != 'hcontent_file' and "
        '  fetch_time is null and '
        '  file_url is not null and '
        "  file_url != '' and"
        "  type != 'preview_type_1' and " 
        "  type != 'preview_type_2' and "
        "  files.last_seen > now()- INTERVAL '7 days'"
        #'  mod_uuid in ('
        #'   select uuid as mod_uuid from ('
        #'    select distinct ON (publishedfileid) uuid from mods  order by publishedfileid, revision_change_number desc'
        #'    ) as active_mods'
        #'   )'
        );#preview_type_1 == youtube




#init stats variables
stats = {}
stats['pending_record_count'] = cur.rowcount
stats['new_file_count'] = 0
stats['existing_file_count'] = 0
stats['archive_count'] = 0

#RealDictRow([('file_uuid', '31463dfc-b8ac-11ec-a0fb-c13e2634f11d'), ('mod_uuid', '31463cfd-b8ac-11ec-a0fb-c13e2634f11d'), ('type', 'hcontent_preview'), ('creator_appid', 281990), ('file_name', '183920283375878720'), ('file_url', 'https://steamuserimages-a.akamaihd.net/ugc/183920283375878720/27272987944A21E2C8A16D730F2D49959D43F4EE/'), ('file_size', 76372), ('sort_order', 0), ('hashes', None), ('steam_id', 183920283375878720), ('last_seen', datetime.datetime(2022, 4, 13, 9, 5, 1, 995872)), ('publishedfileid', 845336788), ('revision_change_number', 0), ('file_time', None), ('fetch_time', None)])
#< HTTP/2 200 
#< content-length: 76372
#< content-type: image/jpeg
#< content-md5: OXhPEVvv7S6JLweiYcFLxQ==
#< last-modified: Wed, 18 Jan 2017 23:25:38 GMT
#< accept-ranges: bytes
#< etag: "0x8D43FF94F9D0D4A"
#< server: Windows-Azure-Blob/1.0 Microsoft-HTTPAPI/2.0
#< x-ms-request-id: a258128f-901e-00bd-7319-4f7aad000000
#< x-ms-version: 2017-04-17
#< x-ms-lease-status: unlocked
#< x-ms-lease-state: available
#< x-ms-blob-type: BlockBlob
#< content-disposition: inline; filename*=UTF-8''previewfile_845336788.jpg;
#< x-ms-server-encrypted: false
#< access-control-expose-headers: x-ms-request-id,Server,x-ms-version,Content-Type,Last-Modified,ETag,Content-MD5,x-ms-lease-status,x-ms-lease-state,x-ms-blob-type,Content-Disposition,x-ms-server-encrypted,Accept-Ranges,Content-Length,Date,Transfer-Encoding
#< access-control-allow-origin: *
#< cache-control: max-age=604784
#< expires: Wed, 20 Apr 2022 09:35:25 GMT
#< date: Wed, 13 Apr 2022 09:35:41 GMT


import requests.adapters

session = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=20)
session.mount('https://', adapter)



for file_detail in  cur:
    error_flag = False
    finished_dir = 'finished_previews/%s/%s/%s/' % (file_detail['creator_appid'], file_detail['mod_uuid'], file_detail['revision_change_number'])
    file_path = '%s/%s' % (finished_dir, file_detail['file_uuid'])
    file_head = '%s.head' % (file_path,)

    try: 
        os.makedirs(finished_dir)
    except:
        pass

    if os.path.isfile(file_path):
        #TODO: If-Not-Modified w/ etag, but we should limit revisit frequency
        stats['existing_file_count'] += 1
        log.info('Existing file: %s, mod: %s publishedfileid: %s steamid: %s' % (file_detail['file_uuid'], file_detail['mod_uuid'], file_detail['publishedfileid'], file_detail['steam_id']))
    else:
        try:
            request = session.get(file_detail['file_url'])
        except:
            log.error('File %s, publishedfileid %s, steamid: %s has no URL' % (file_detail['file_uuid'], file_detail['publishedfileid'], file_detail['steam_id']))
            continue
        if request.status_code == requests.codes.ok:
            #TODO: Check content-length and content-md5
            with open(file_path, 'wb') as fh:
                for chunk in request.iter_content(chunk_size=128):
                    fh.write(chunk)
            with gzip.open(file_head, 'wb') as fh:
                fh.write(str(request.headers).encode('utf-8'))
            stats['new_file_count'] += 1
        else:
            log.warning('Non OK status code: %s %s' % (file_detail['file_uuid'], request.status_code))
            error_flag = True

        try:
            #Wed, 18 Jan 2017 23:25:38 GMT
            parsed_datestamp = datetime.strptime(request.headers['last-modified'], '%a, %d %b %Y %H:%M:%S %Z')
            updated_time = time.mktime(parsed_datestamp.timetuple())
            os.utime(file_path, (updated_time, updated_time))
        except Exception as e: 
            log.error('Error updating timestamp, %s %s' % (e, type(e)))
            error_flag = True



        if not error_flag:
            hashes = get_hashes(file_path)
            hashes['file'] = hashes['file'][len(finished_dir)]

        if(error_flag == False):
            log.info('App: %s pubfileid: %s file_uuid %s fetch successful' % (file_detail['creator_appid'], file_detail['publishedfileid'], file_detail['file_uuid']))
            cur2.execute('update files set file_time=%s, fetch_time=now(), hashes=%s where uuid=%s', (parsed_datestamp, hashes, file_detail['file_uuid']))
        else:
            log.warning('Error flag is set , App: %s pubfileid: %s file_uuid %s not updated' % (file_detail['creator_appid'], file_detail['publishedfileid'], file_detail['file_uuid']))
            try:
                os.unlink(file_path)
            except:
                log.warning('Error deleting file %s' % (file_path))
                pass

    dbh.commit()


logging.info('Parse stats: %s' % (stats))


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
cur2.close()
dbh.close()
