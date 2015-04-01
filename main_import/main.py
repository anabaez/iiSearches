#!/usr/bin/env python
"""
This is the template for a script file. Please change this docstring to reflect
realities.
"""
import argparse
import ConfigParser
import logging
import os
import sys
import time
import csv
import HTMLParser
from ftplib import FTP
from squirro_client import ItemUploader 

# Script version. It's recommended to increment this with every change, to make
# debugging easier.
VERSION = '0.9.0'


# Set up logging.
log = logging.getLogger('{0}[{1}]'.format(os.path.basename(sys.argv[0]),
                                          os.getpid()))
def add_keywords(obj):
    # if 'ks' not in locals():
    #   ks = {}

    ks[obj] = []
    for key in story[obj].split(','):
        ks[obj].append(key)


def ftp_connection(config):
    #pull files from the ftp server
    try:
    ftp_url = config.get('squirro','ftp_url')
    user = config.get('squirro','user')
    password = config.get('squirro','password')
    file_identifier = config.get('squirro','file_identifier')

    ftp = FTP(ftp_url)     # connect to host, default port
    ftp.login(user, password)

    for item in ftp.nlst():
        if item.find(file_identifier) != -1:
            main_file = item
            buf = StringIO.StringIO()
            buf.seek(0)
            return buf

except Exception as e:
    print 'failed', e


def main(args, config):
   
    """Uploads items from the main file"""

    cluster = config.get('squirro', 'cluster')
    token = config.get('squirro', 'token')
    project_id = config.get('squirro', ' project_id')

    uploader = ItemUploader(project_id=args.project_id, source_name=args.source_name, token=args.token, cluster=args.cluster)
    buf = ftp_connection(config)
    csvreader = csv.DictReader(buf, delimiter='|', quotechar=' ')
    items = []

    for story in csvreader:
        try:
            print "Story:", h.unescape(story['Body']).replace(h.unescape('&#92;n'),'')
            item={}
            item['title'] = story['Title']
            item['id'] = story['Article ID']
            item['link'] = story['Article Link']
            categories = ", ".join(story['Categories'].split(','))

            body = u"""
                <html>
                <head> 
                <H6> 
                Source: {source} <br/>
                </H6>
                </head>
               
                <body>
                    {body}
                    <br/>
                    Categories: {categories}
                </body>
                
                </html>
                """.format(source=story['Source'], body=h.unescape(story['Body']).replace(h.unescape('&#92;n'),''), categories=categories)
            
            item['body'] = body

            #Add keywords
            ks = {}
            
            ks['iiKeywords'] = []

            for key in story['Keywords'].split(','):
                ks['iiKeywords'].append(key)

            add_keywords('Related Mandates')
            add_keywords('Related Funds')
            add_keywords('Related Consultants')
            item['keywords'] = ks
            items.append(item)

        except:
            continue
    uploader.upload(items)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', action='version', version=VERSION)
    parser.add_argument('--verbose', '-v', action='count',
                        help='Show additional information.')
    parser.add_argument('--log-file', dest='log_file',
                        help='Log file on disk.')
    parser.add_argument('--config-file', dest='config_file',
                        help='Configuration file to read settings from.')

    return parser.parse_args()


def setup_logging(args):
    """Set up logging based on the command line options.
    """
    # Set up logging
    fmt = '%(asctime)s %(name)s %(levelname)-8s %(message)s'
    if args.verbose == 1:
        level = logging.INFO
        logging.getLogger(
            'requests.packages.urllib3.connectionpool').setLevel(logging.WARN)
    elif args.verbose >= 2:
        level = logging.DEBUG
    else:
        # default value
        level = logging.WARN
        logging.getLogger(
            'requests.packages.urllib3.connectionpool').setLevel(logging.WARN)

    # configure the logging system
    if args.log_file:
        out_dir = os.path.dirname(args.log_file)
        if out_dir and not os.path.exists(out_dir):
            os.makedirs(out_dir)
        logging.basicConfig(
            filename=args.log_file, filemode='a', level=level, format=fmt)
    else:
        logging.basicConfig(level=level, format=fmt)

    # Log time in UTC
    logging.Formatter.converter = time.gmtime


def get_config(args):
    """Parse the config file and return a ConfigParser object.

    Always reads the `main.ini` file in the current directory (`main` is
    replaced by the current basename of the script).
    """
    cfg = ConfigParser.SafeConfigParser()

    root, _ = os.path.splitext(__file__)
    files = [root + '.ini']
    if args.config_file:
        files.append(args.config_file)

    log.debug('Reading config files: %r', files)
    cfg.read(files)
    return cfg


# This is run if this script is executed, rather than imported.
if __name__ == '__main__':
    args = parse_args()
    setup_logging(args)
    config = get_config(args)

    log.info('Starting process (version %s).', VERSION)
    log.debug('Arguments: %r', args)

    # run the application
    try:
        main(args, config)
    except Exception as e:
        log.exception('Processing error')
