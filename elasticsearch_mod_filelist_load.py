from datetime import datetime
from elasticsearch import Elasticsearch
import os
from zipfile import ZipFile
import logging
import json
import urllib3
urllib3.disable_warnings()


start_time = datetime.now()

log_folder = 'logs/elasticsearch_mod_filelist_ingest/'

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
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("elasticsearch").setLevel(logging.WARNING)
logging.getLogger("elasticsearch.trace").setLevel(logging.WARNING)
logging.getLogger("elastic_transport.transport").setLevel(logging.WARNING)



log.info('ElasticSearch mod filelist ingest start')


es = Elasticsearch("https://10.5.10.6:9200", verify_certs=False, http_auth=('ingest_es', 'z9E22#m*M557AJk7dRxU'))
from elasticsearch.helpers import bulk

index_dir = 'finished_mods'

dirs = os.listdir(index_dir)
for appid in dirs:
    if os.path.isdir('%s/%s' % (index_dir, appid)):
        app_dir = os.listdir('%s/%s' % (index_dir, appid))
        for mod_uuid in app_dir:
            mod_dir = os.listdir('%s/%s/%s' % (index_dir, appid, mod_uuid))
            for zipfile in mod_dir:
                filename_full = '%s/%s/%s/%s' % (index_dir, appid, mod_uuid, zipfile)
                if os.path.isfile(filename_full) and filename_full[-4:].lower() == '.zip':
                    try:
                        with ZipFile(filename_full, 'r') as current_zip:
                            actions = []
                            log.info('Ingest start %s %s' % (mod_uuid, zipfile,))
                            try:
                                result = es.search(index='mod-filelist-complete-index-v1', query={ 'match_phrase': {'name': filename_full} })
                                if(result['hits']['total']['value'] > 0):
                                   log.info('File already indexed, skipping - %s %s' % (mod_uuid, filename_full))
                                   continue
                            except Exception as e:
                                log.warning('Exception in search: %s %s' % (type(e), e))
                            document = {}
                            document['namelist'] = current_zip.namelist()
                            document['source_mod_uuid'] = mod_uuid
                            document['source_app_id'] = appid
                            document['source_zip_file'] = zipfile

                            actions.append({'_index': 'mod-filelist-index-v1' , '_source': document})


                            log.info('Ingest complete %s %s' % (mod_uuid, zipfile))
                            actions.append({'_index':'mod-filelist-complete-index-v1', '_source':{'type': 'zipped_file', 'name': filename_full, 'timestamp': datetime.utcnow(), 'record_count':  len(document['namelist'])}})
                    except Exception as e:
                        log.error('Zipfile failed to open - %s %s' % (type(e), e))
            bulk(es, actions)

