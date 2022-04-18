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
    os.makedirs('mod_dl_logs/depotdownloader/')
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

log_file_handler = logging.FileHandler('mod_dl_logs/%s.log' % (start_time,), )
log_file_handler.setLevel(logging.INFO)
log_file_handler.setFormatter(log_formatter)

log_console_handler = logging.StreamHandler()
log_console_handler.setLevel(logging.DEBUG)
log_console_handler.setFormatter(log_formatter)

log.addHandler(log_file_handler)
log.addHandler(log_console_handler)

log.info('Steam workshop mod download start')



# Connect to Postgres
dbh = psycopg2.connect("dbname=stellar-mods user=postgres") or logging.critical('Unable to connect to database');
cur = dbh.cursor(cursor_factory = psycopg2.extras.RealDictCursor) #results return associative dicts instead of tuples.
cur2 = dbh.cursor() #used for updates
psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)
psycopg2.extensions.register_adapter(list, psycopg2.extras.Json)
psycopg2.extensions.register_adapter(tuple, psycopg2.extras.Json)



existing_mod_detail = {}
seen_mod_detail = {}

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
        "  type = 'hcontent_file' and "
        '  fetch_time is null'

        #'  mod_uuid in ('
        #'   select uuid as mod_uuid from ('
        #'    select distinct ON (publishedfileid) uuid from mods  order by publishedfileid, revision_change_number desc'
        #'    ) as active_mods'
        #'   )'
        );


log.info('Got %s rows from database' % (cur.rowcount, ))

#init stats variables
stats = {}
stats['pending_record_count'] = cur.rowcount
stats['new_file_count'] = 0
stats['existing_file_count'] = 0
stats['archive_count'] = 0

#RealDictRow([('file_uuid', '31463dfd-b8ac-11ec-a0fb-c13e2634f11d'), ('mod_uuid', '31463cfd-b8ac-11ec-a0fb-c13e2634f11d'), ('type', 'hcontent_file'), ('creator_appid', 281990), ('file_name', ''), ('file_url', ''), ('file_size', 295673), ('sort_order', 0), ('sha256', None), ('steam_id', 195436278166515171), ('last_seen', datetime.datetime(2022, 4, 12, 15, 5, 2, 204422)), ('publishedfileid', 845336788), ('file_time', None), ('fetch_time', None)])
#CompletedProcess(args=['dotnet', '/opt/depotdownloader/DepotDownloader.dll', '-app', '281990', '-pubfile', '845336788', '-validate'], 
#returncode=0, 
#stdout=b'No username given. Using anonymous account with dedicated server subscription.\n
#Connecting to Steam3... Done!\n
#Logging anonymously into Steam3... Done!\n
#Using Steam3 suggested CellID: 65\n
#Got AppInfo for 281990\n
#Got depot key for 281990 result: OK\n
#Processing depot 281990 - \n
#Downloading depot manifest...Got manifest request code for 281990 195436278166515171 result: 14977892331439202635\n
#Done!\n
#Manifest 195436278166515171 (1/18/2017 11:25:34 PM)\n
#Downloading depot 281990 - \n
#Pre-allocating depots/281990/8381018/walrus.zip\n
#100.00% depots/281990/8381018/walrus.zip\n
#Depot 281990 - Downloaded 53648 bytes (295673 bytes uncompressed)\n
#Total downloaded: 53648 bytes (295673 bytes uncompressed) from 1 depots\nDisconnected from Steam\n', stderr=b'')

regex_files_pre = re.compile(r"Pre-allocating ([^\n]+)")
regex_files_existing = re.compile(r"Validating ([^\n]+)")
regex_file_parts = re.compile(r"depots/([^/]+)/([^/]+)/([^\n]+)")

regex_manifest_date = re.compile(r"Manifest ([^\s]+) ([^\n]+)")
regex_total_bytes = re.compile(r"Total downloaded: ([^\n]+)")

for mod_detail in  cur:
    error_flag = False
    command = ['dotnet', '/opt/depotdownloader/DepotDownloader.dll', 
            '-app', '%s' % mod_detail['creator_appid'], 
            '-ugc', '%s' % mod_detail['steam_id'],
            ]
    output = subprocess.run(command, capture_output=True)
    stdout = output.stdout.decode("utf-8")
    stderr = output.stderr.decode("utf-8")

    result_files_pre = regex_files_pre.search(stdout)
    result_files_existing = regex_files_existing.search(stdout)
    result_manifest_date = regex_manifest_date.search(stdout)
    result_total_bytes = regex_total_bytes.search(stdout)

    try:
        files_pre = result_files_pre.groups()
    except:
        files_pre = ()

    try:
        files_existing = result_files_existing.groups()
    except:
        files_existing = ()

    try:
        manifest_id = result_manifest_date.groups()[0]
    except:
        manifest_id = None

    try:
        manifest_datestamp = result_manifest_date.groups()[1]
    except:
        manifest_datestamp = None

    try:
        total_bytes = result_total_bytes.groups()[0]
    except:
        total_bytes = None

    if len(files_pre) > 0:
        result_file_parts = regex_file_parts.search(files_pre[0])
    elif len(files_existing) > 0:
        result_file_parts = regex_file_parts.search(files_existing[0])

    files_all = files_pre + files_existing

    try:
        depotdir = 'depots/%s/%s' %( result_file_parts.groups()[0], result_file_parts.groups()[1])
    except:
        depotdir = None

    stats['new_file_count'] += len(files_pre)
    stats['existing_file_count'] += len(files_existing)

    mod_download_detail = '%s Mod depot download: App: %s pubfileid: %s rc: %s files_new: %s files_existing: %s manifest_id: %s manifest_date: %s, total_bytes: %s\n' % (datetime.now(), mod_detail['creator_appid'], mod_detail['publishedfileid'], output.returncode, len(files_pre), len(files_existing), manifest_id, manifest_datestamp, total_bytes)
    mod_download_detail = mod_download_detail.encode('utf-8')
    if len(stdout) > 0:
        with gzip.open('mod_dl_logs/depotdownloader/%s_%s.stdout.gz' % (mod_detail['creator_appid'], mod_detail['publishedfileid']), 'a') as fh:
            fh.write(mod_download_detail)
            fh.write(output.stdout)

    if len(stderr) > 0:
        with gzip.open('mod_dl_logs/depotdownloader/%s_%s.stderr.gz' % (mod_detail['creator_appid'], mod_detail['publishedfileid']), 'a') as fh:
            fh.write(mod_download_detail)
            fh.write(output.stderr)

    logging.info('Mod depot download: App: %s pubfileid: %s rc: %s files_new: %s files_existing: %s manifest_id: %s manifest_date: %s, total_bytes: %s depotdir: %s'
                    % ( mod_detail['creator_appid'], mod_detail['publishedfileid'], output.returncode, len(files_pre), len(files_existing), manifest_id, manifest_datestamp, total_bytes, depotdir))
    try:
        parsed_datestamp = datetime.strptime(manifest_datestamp, '(%m/%d/%Y %I:%M:%S %p)')
        updated_time = time.mktime(parsed_datestamp.timetuple())
    except: 
        log.error('Error updating timestamp')
        error_flag = True

    finished_dir = 'finished_mods/%s/%s/' % (mod_detail['creator_appid'], mod_detail['mod_uuid'])

    try:
        os.makedirs(finished_dir)
    except:
        pass


    for filename in files_all:
        os.utime(filename, (updated_time, updated_time))


    if len(files_all) == 1 and files_all[0][-4:].lower() == '.zip':
        #Entire mod is already zipped, use existing rather than re-zipping.
        try:
            shutil.move(files_all[0], finished_dir)
        except Exception as e:
            log.error('Finished mod zip file already exists %s %s %s' % (zip_file_name, e, type(e)))
            error_flag = True

    elif depotdir == None or not os.path.isdir(depotdir):
        log.error("Depotdir doesn't exist, download failed?")
        error_flag = True
    else:
        prev_dir = os.getcwd()
        zip_file_name = '%s/%s/%s_%s_%s.zip' % (prev_dir, finished_dir, mod_detail['creator_appid'], mod_detail['publishedfileid'], mod_detail['revision_change_number'])
        if os.path.exists(zip_file_name):
            log.error('Finished zip file already exists %s' % (zip_file_name,))
            error_flag = True

        os.chdir(depotdir)
        command = ['zip', '-JXDor' , zip_file_name, './', '-x', '.DepotDownloader', '.DepotDownloader/*']
        output = subprocess.run(command, capture_output=True)
        os.chdir(prev_dir)

        logging.info('Created zip for App: %s pubfileid: %s rc: %s' % (mod_detail['creator_appid'], mod_detail['publishedfileid'], output.returncode))
        if(output.returncode != 0):
            log.warning('Non zero returncode for zip')
            error_flag = True

    
    for filename in glob.glob(r'%s/manifest_*.txt' % (depotdir,)):
        try:
            shutil.move(filename, finished_dir)
        except:
            log.error('Error moving file %s from %s to %s' % (filename, depotdir, finished_dir))
            error_flag = True

    hash_list = []
    for filename in glob.glob(r'%s/*' % (finished_dir,)):
        hashes = get_hashes(filename)
        hashes['file'] = hashes['file'][len(finished_dir)]
        hash_list.append(hashes)

    if(len(hash_list) == 2 and error_flag == False):
        stats['archive_count'] += 1
        cur2.execute('update files set file_time=%s, fetch_time=now(), hashes=%s where uuid=%s', (parsed_datestamp, hash_list, mod_detail['file_uuid']))
    else:
        log.warning('Error flag is set or hash size not right, App: %s pubfileid: %s not updated' % (mod_detail['creator_appid'], mod_detail['publishedfileid']))
        shutil.rmtree(finished_dir)

    dbh.commit()
    try:
        shutil.rmtree(depotdir)
    except:
        log.warning("Failed to cleanup Depotdir")


logging.info('Parse stats: %s' % (stats))

dbh.commit()
cur.close()
cur2.close()
dbh.close()
