
import sys
import pysftp
import time
import datetime
import logging
import glob
import os



from datetime import datetime,timedelta



hoy = datetime.now()#current date

# logs
fechalog='{:%Y%m%d}'.format(hoy)
logfile = '..../log/send_sftp_{}.log'.format(fechalog)
logging.basicConfig(filename=logfile,filemode='a', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s [%(lineno)d] %(message)s')



logging.debug('------------------------------------')
logging.debug('----  Starting sending process -----')
logging.debug('------------------------------------')


#deshabilito hostkeys chequeo
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None    # disable host key checking.

logging.debug('Preparing connection')


try:
    #connection setup
    srv = pysftp.Connection(host="hostname",
                            username="username",
                            password="password",
                            port=19219,
                            cnopts=cnopts)


    logging.debug('Connection established: <%s>', srv)

    logging.debug('----------------------------------')

except Exception:
    logging.error(traceback.format_exc())
    #logging.error('timeout <%s> exceeded! exiting program ',seconds)
    print("Unexpected error:", sys.exc_info()[0])
    sys.exit()



os.chdir("/home/backups")


files = glob.glob("*.gz")
files.sort(key=os.path.getmtime)
#print("\n".join(files))

last_file = files[-1]
logging.debug('Last File: <%s>', last_file)

# upload the file
srv.put(last_file)


# Close the connection
srv.close()


logging.debug('-----------------------------------')
logging.debug('----  finished sending process -----')
logging.debug('------------------------------------')
