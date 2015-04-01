"""A test file investigating how to use ftp
commands to pull from the server"""

from ftplib import FTP
import sys
import StringIO
import csv

try:
    #pull files from the ftp server
    ftp_url = 'ftp.euromoneydigital.com'
    user = 'investec'
    password = 'Jc3L9mn3'
    file_identifier = 'News'

    ftp = FTP(ftp_url)     # connect to host, default port
    ftp.login(user, password)
    #print ftp.
    print ftp.retrlines('LIST')
    for item in ftp.nlst():
        if item.find(file_identifier) != -1:
            print 'Found'
            main_file = item
            print main_file
            print 'RETR %s' % main_file
            buf = StringIO.StringIO()
            print ftp.retrbinary('RETR %s' % main_file, buf.write)
            print len(buf.getvalue())
            buf.seek(0)
            print buf.seek(0) 

            csvreader = csv.DictReader(buf, delimiter='|', quotechar=' ')
            for story in csvreader:
                print story

except Exception as e :
    print 'failed', e