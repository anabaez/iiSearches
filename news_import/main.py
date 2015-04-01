#!/usr/bin/env python
"""
This is a file for uploading content from 'content' files provided by iiSearches
to squirro.
"""
from squirro_client import ItemUploader
import argparse
import ConfigParser
import logging
import os
import sys
import time
import csv
import HTMLParser
import hashlib
import json
from ftplib import FTP
import StringIO
from bs4 import BeautifulSoup

# Script version. It's recommended to increment this with every change, to make
# debugging easier.
VERSION = '0.9.0'

# Set up logging.
log = logging.getLogger('{0}[{1}]'.format(os.path.basename(sys.argv[0]),
                                          os.getpid()))

def get_keywords(row, field_name):
    """split row by comma and adds values to the squirro keyname"""
    if field_name in row:
        return row[field_name].split(',')
    else:
        return None


def get_ftp_csv(config):
    """pull files from ftp server, returns a dictionary containing
    content from the feeds file."""
    try:
        ftp_url = config.get('squirro', 'ftp_url')
        user = config.get('squirro', 'user')
        password = config.get('squirro', 'password')
        file_identifier = config.get('squirro', 'file_identifier')

        ftp = FTP(ftp_url)     # connect to host, default port
        ftp.login(user, password)

        for item in ftp.nlst():
            if file_identifier in item:
                main_file = item
                buf = StringIO.StringIO()
                print ftp.retrbinary('RETR %s' % main_file, buf.write)
                buf.seek(0)
                feeds_dict = csv.DictReader(buf, delimiter='|', quotechar=' ')
                return feeds_dict
    except Exception as e:
        log.error('Could not access files %r', e)
        raise


def main(args, config):
    """import files from ftp server, should check to see if file is updated"""

    feeds_dict = get_ftp_csv(config)
    items = []
    html_parser = HTMLParser.HTMLParser()

    # content = csv.DictReader(news_file, delimiter='|', quotechar=' ')

    uploader = ItemUploader(project_id=config.get('squirro', 'project_id'),
                            source_name=config.get('squirro', 'source_name'),
                            token=config.get('squirro', 'token'),
                            cluster=config.get('squirro', 'cluster'))
    for row in feeds_dict:
        try:
            if 'rows' not in row['Article ID']:
                print row['Title']
                item = {'title': row['Title'],
                    'id': row['Article ID'],
                    'link': row['Article Link']}
                #print BeautifulSoup(row['Body']).contents
                #item['body'] = html_parser.unescape(row['Body'].decode('utf-8').replace('\\n', '<br/>'))
                item['body'] = BeautifulSoup(row['Body'].replace('\\n', '<br/>')).prettify()
                #Add keywords
                keywords = {
                    'iiKeyword': get_keywords(row, 'Keywords'),
                    'Related_Mandates': get_keywords(row, 'Related Mandates'),
                    'Related_Funds': get_keywords(row, 'Related Funds'),
                    'Related_Consultants': get_keywords(row, 'Related Consultants'),
                }

                item['keywords'] = keywords
                items.append(item)
            else:
                print 'none row'

        except Exception:
            log.exception('Could not parse row: %r', row)
            continue
        print 'Upload Items'
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
    parser.add_argument('--ignore_hash', '-i', action="count", default=0,
                        help='ignore any existing hash file and load from content')
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
