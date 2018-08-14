import pymongo
import datetime
import pprint
import time
import logging

from pymongo import MongoClient
from datetime import datetime,timedelta


# logs - daily timestamp log file
today = datetime.now()#current date
todaydatelog='{:%Y%m%d}'.format(today)
logfile = '...../log/historic_{}.log'.format(todaydatelog)
logging.basicConfig(filename=logfile,filemode='a', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s [%(lineno)d] %(message)s')




# DB connection
client = MongoClient()

# Get the database
db = client['DATABASE-NAME']
db.collection_names(include_system_collections=False)

# get SOURCES collection
records_source_col = db['records']
operations_source_col = db['operations']

# DESTINATION collections
records_dest_col = db['historic_records']
operations_dest_col = db['historic_operations']


########## Functions declarations #############

def process_record(idx, record_id):

  # operations per record
    operations_cursor = operations_source_col.find( { 'record_id' :  record_id } )
    logging.debug('-- Processing operations for record:  <%s>', record_id)
    o_counter = 0
    for operation in operations_cursor:
        o_counter += 1
        result = operations_dest_col.insert_one(operation)
        logging.debug('----- Operation <%s>  inserted in destination collection <%s>', result.inserted_id,operations_dest_col.name)

        operations_source_col.remove(operation['_id'])
        logging.debug('----- Operation <%s>  removed from source collection <%s>', operation['_id'], operations_source_col.name )
    #enf of for

    logging.debug('--- Managed [%s] operations documents', o_counter)


    result = records_dest_col.insert_one(record)
    logging.debug('-- Record: <%s> inserted OK in DESTINATION collection <%s> ', result.inserted_id, records_dest_col.name)
    result = records_source_col.remove(record_id)
    logging.debug('-- Verification Record id[%s]: <%s>  Removed OK from SOURCE Collection <%s>', idx, record_id, records_source_col.name)

#end of process_record function

logging.debug('-----------------------------------')
logging.debug('--- Starting process ----')
logging.debug('-----------------------------------')
logging.debug('')
logging.debug('-- Calculating Dates ---')

logging.debug('--- Today:  <%s>', today)
days90 = timedelta(days=90)
threshold_date90 = today - days90
logging.debug('--- Today less 90 days: <%s>', threshold_date90)


timestsamp_threshold90 = int(time.mktime(threshold_date90.timetuple()))
logging.debug('--- Timestamp from timestsamp_threshold90 ago: <%s>', timestsamp_threshold90)
logging.debug('---')


logging.debug('')
logging.debug('')

## processing UNSSUBSCRIPTION Records ##
logging.debug('RECORDS UNSUBSCRIPTION PROCESS')

records_cursor = records_source_col.find( { 'status':{ '$eq': 'UNSUBSCRIBED' } } );
cuenta =  records_source_col.find( { 'status':{ '$eq': 'UNSUBSCRIBED' } }  ).count()
logging.debug('Total number of UNSUBSCRIBED records: [%s]', cuenta)

r_counter=1
for record in records_cursor:

    logging.debug('--')
    logging.debug('-- RECORD: <%s> process', record['_id'])

    process_record(r_counter,record['_id'])
    r_counter += 1

# end of for RECORDS to insert

logging.debug(' Processed [%s] Records - FINISHED UNSUBSCRIPTION PROCESS', r_counter)
logging.debug('-------------------------------------------------')
logging.debug('')


## processing PURCHASED ##
logging.debug('RECORDS PURCHASED PROCESS')

records_cursor_cobrado = records_source_col.find( {"$and" : [ { 'last_updated' :  { '$lt': timestsamp_threshold90 } }, { 'status': 'PURCHASED'} ] } )
cuenta =  records_source_col.find(  {"$and" : [ { 'last_updated' :  { '$lt': timestsamp_threshold90 } }, { 'status': 'PURCHASED'} ] }).count()
logging.debug('Total number of purchased records older than <%s>: [%s]  -- ', threshold_date90, cuenta)
logging.debug('---')


r_counter=1
for record in records_cursor_cobrado:
    logging.debug('--- RECORD: <%s>', record["_id"])

    process_record(r_counter,record['_id'])
    r_counter += 1
# end of for RECORDS to insert

logging.debug(' Processed [%s] Records - FINISHED PURCHASED PROCESS', r_counter)
logging.debug('-----------------------------------------------------')
logging.debug('')
logging.debug('')


logging.debug('-----------------------------------')
logging.debug('FINISHED PROCESS')
logging.debug('-----------------------------------')


