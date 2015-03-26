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


# Script version. It's recommended to increment this with every change, to make
# debugging easier.
VERSION = '0.9.0'

# Set up logging.
log = logging.getLogger('{0}[{1}]'.format(os.path.basename(sys.argv[0]),
                                          os.getpid()))

def add_keyword(ks,row,key_name,field_name):
    #split row by comma and adds values to the squirro keyname
    ks[key_name]=[]
    for key in row[field_name].split(','):
        ks[key_name].append(key)


def fund_lookup(main_file):
    #open the main file that we use for reference, create a dictionary with the fundID as the key
    #and a list of the main content as a value 
    with open(main_file, 'rb') as mainfile:
        reader = csv.DictReader(mainfile, delimiter='|', quotechar=' ')
        main = {}
        for row in reader:
            main[row['Fund ID']] = row
        return main


def update_check(cache_folder, content_file):
    #Check to see if content file had been uploaded before to avoid
    #additional load on squirro/ftp servers 
    m = hashlib.md5()
    m.update(content_file)
    key = m.hexdigest()

    cache_file_path = "%s/%s.json" % (cache_folder, key)
    try:
        #HIT
        with open(cache_file_path, 'rb') as cache_file:
            return True
    except IOError:
        #MISS
        with open(cache_file_path, 'wb') as cache_file:
            with open(config.get('squirro_credentials','file'), 'rb') as contentfile:
                content = csv.DictReader(contentfile, delimiter='|',quotechar=' ')
                for row in content:
                        cache_file.write(json.dumps(row, indent=4))
                        cache_file.write('\n')
        return False


def main(args, config):

    cache_folder = 'C:/_Squirro'
    content_file = config.get('squirro_credentials','file')

    #check if the content file is new
    if update_check(cache_folder,content_file):
        print 'No new file detected'
        if args.ignore_hash == 0:
            print "Squirro not updated"
            sys.exit(0)

    #open csv content file 
    with open(config.get('squirro_credentials','file'), 'rb') as contentfile:
        items = []
        h = HTMLParser.HTMLParser()
        content = csv.DictReader(contentfile, delimiter='|',quotechar=' ')
        uploader = ItemUploader(project_id=config.get('squirro_credentials','project_id'), 
                                source_name=config.get('squirro_credentials','source_name'), 
                                token=config.get('squirro_credentials','token'), 
                                cluster=config.get('squirro_credentials','cluster'))
        for row in content: 
            try:
                item={}
                item['title'] = row['Title']
                item['id'] = row['Article ID']
                item['link'] = row['Article Link']

                
                item['body'] = u"""
                    <html>
                        <head> 
                            <H5> 
                            Source: {source} <br/>
                            Date: {date}

                            </H5>
                        </head>
                        <body>
                        {body}
                        <br/>
                        Categories: {categories}
                        </body>
                    </html>
                    """.format(source=row['Source'],
                               body=h.unescape(row['Body']).replace(h.unescape('&#92;n'),''),
                               categories=", ".join(row['Categories'].split(',')),
                               date=time.strftime("%Y-%m-%d", time.localtime()))    

                #Add keywords      
                ks = {} 
                add_keyword(ks,row,'iiKeyword','Keywords')
                add_keyword(ks,row,'Related_Mandates','Related Mandates')
                add_keyword(ks,row,'Related_Funds','Related Funds')
                add_keyword(ks,row,'Related_Consultants','Related Consultants')

                main = fund_lookup(config.get('squirro_credentials','main_file'))

                for FundID in row['Related Funds'].split(','):
                        if FundID in main:
                            print 'Adding to ks'
                            ks['Consultant Parent ID'] = main[FundID]['Consultant Parent ID']
                            ks['Asset Class'] =  main[FundID]['Asset Class']
                            ks['Mandate ID'] =  main[FundID]['Mandate ID']
                            ks['Search Consultant'] =  main[FundID]['Search Consultant']
                            ks['Search Status'] = main[FundID]['Search Status']
                            ks['Fund Status Last Updated'] = main[FundID]['Last Updated']
                            ks['Fund Region'] = main[FundID]['Region']
                            ks['Fund Type'] = main[FundID]['Fund Type']
                            ks['Consultant ID'] = main[FundID]['Consultant ID']
                            ks['Sub Asset Class'] = main[FundID]['Sub Asset Class']
                            ks['Mandate Size Amount'] = main[FundID]['Mandate Size Amount']
                            ks['Fund ID'] = main[FundID]['Fund ID']

                item['keywords'] = ks
                items.append(item)      
            except:
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
    parser.add_argument('--ignore_hash','-ih', action="count", default=0,
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