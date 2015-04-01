#!/usr/bin/env python
"""
This is the script file for importing the iisearches feed file.
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
import sys
import StringIO

# Script version. It's recommended to increment this with every change, to make
# debugging easier.
VERSION = '0.9.0'


# Set up logging.
log = logging.getLogger('{0}[{1}]'.format(os.path.basename(sys.argv[0]),
                                          os.getpid()))
def get_keywords(row, field_name):
    """ Function to assign keywords"""
    return row[field_name].split(',')


def get_ftp_csv(config):
    """Function to pull csv files from iisearches ftp server"""
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
                csvreader = csv.DictReader(buf, delimiter='|', quotechar=' ')
                return csvreader
    except Exception as e:
        log.exception('Could not import file %r', e)
        raise


def main(args, config):

    """Uploads items from the feed file"""

    uploader = ItemUploader(project_id=config.get('squirro', 'project_id'),
                            source_name=config.get('squirro', 'source_name'),
                            token=config.get('squirro', 'token'),
                            cluster=config.get('squirro', 'cluster'))
    

    feed_file = get_ftp_csv(config)

    items = []

    for row in feed_file:
        try:
            #print "Story:", h.unescape(story['Comments']).replace(h.unescape('&#92;n'), '')
            item = {
                'title': str(row['Fund']) + ' - ' + str(row['Fund Type']),
                'id': row['Mandate ID'],
                'link': row['URL'],
            }

            if row['Mandate Size Amount']== None:
                mandate_size = ''
            else:
                mandate_size = 'Mandate Size: '
            #print row

            # if valid_date(row['Last Updated'])==False:
            #     print 'no date'
            #     continue


            body = u"""
                        <H5> 
                        Search Status: {search_status}<br/>
                        Last Updated: {last_updated}<br/>
                        Region: {region}<br/>
                        Search Consultant: {search_consul} <br/>
                        {mandate}
                        </H5>

                    <body>
                    {body}
                    </body>
                """.format(search_status=row['Search Status'],
                            last_updated=row['Last Updated'],
                            mandate =mandate_size + '$' + str(row['Mandate Size Amount']),
                            region= row['Region'],
                            search_consul=row['Search Consultant'],
                            sub_class=row['Sub Asset Class'],body= row['Comments'])

            item['body'] = body

            #Add keywords
            keywords = {
               'Search Status' : get_keywords(row,'Search Status'),
               'Region': get_keywords(row, 'Region'),
               'Search Consultant': get_keywords(row, 'Search Consultant'),
               'Sub Asset Class':get_keywords(row, 'Sub Asset Class')
               #'Mandate Size Amount': mandate_size + get_keywords(row, 'Mandate Size Amount')
            }

            item['keywords'] = keywords
            items.append(item)

        except Exception:
            log.exception('Unable to parse item %r', item)
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
