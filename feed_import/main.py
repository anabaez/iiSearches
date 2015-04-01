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

# Script version. It's recommended to increment this with every change, to make
# debugging easier.
VERSION = '0.9.0'


# Set up logging.
log = logging.getLogger('{0}[{1}]'.format(os.path.basename(sys.argv[0]),
                                          os.getpid()))


def main(args, config):
    """
    The main method. Any exceptions are caught outside of this method and will
    be handled.
    """
    log.warn('This method has been intentionally left almost blank.')
    log.info('Number of bytes (sample config): %d',
             config.getint('example', 'bytes'))
    log.info('And a string config example as well: %s',
             config.get('example', 'greeting'))
    raise Exception('Testing exception handling')


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
