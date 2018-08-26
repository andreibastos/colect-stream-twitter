#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
  author: 
    - name: @GustavoRPS
      url: https://gustavorps.net?ref=repo/twitter-gather
"""
import os
from backports import configparser

import dotenv

__all__ = ['twitter', 'logging']


_PATH_ROOT = os.path.dirname(__file__)
_PATH_DOTENV = os.path.join(_PATH_ROOT, '.env')
_PATH_SETTINGS_INI = os.path.join(_PATH_ROOT, 'settings.ini')

_path_join_root = lambda path: os.path.join(_PATH_ROOT, path)

_config = configparser.ConfigParser()

if os.path.exists(_PATH_SETTINGS_INI):
  _config.read(_PATH_SETTINGS_INI)
else:
  if os.path.exists(_PATH_DOTENV):
    dotenv.load_dotenv(_PATH_DOTENV)

  env = os.environ.get
  
  t = _config['twitter'] = {}
  t['keys'] = {}
  t['keys']['file'] = env('TWITTER_KEYS_FILE', _path_join_root('twitter-keys.json'))
  t['keys']['rest'] = {}
  t['keys']['rest']['uri'] = env('TWITTER_KEYS_REST_URI', None)
  t['queries'] = {}
  t['queries']['file'] = env('TWITTER_QUERIES_FILE', _path_join_root('twitter-queries.json'))
  t['labeler'] = {}
  t['labeler']['uri'] = env('TWITTER_LABELER_URI', None)
  t['labeler']['property'] = env('TWITTER_LABELER_PROPERTY', 'keywords')
  t['repository'] = {}
  t['repository']['rest'] = {}
  t['repository']['rest']['uri'] = env('TWITTER_REPOSITORY_REST_URI', None)
  t['repository']['rest']['bulk_site'] = env('TWITTER_REPOSITORY_REST_BULK_SIZE', 10)
  t['repository']['es'] = {}
  t['repository']['es']['uri'] = env('TWITTER_REPOSITORY_ES_URI', None)
  t['repository']['es']['bulk_site'] = env('TWITTER_REPOSITORY_ES_BULK_SIZE', 50)

  l = _config['logging'] = {}
  l['handlers'] = {}
  # REF: https://github.com/sashgorokhov/python-telegram-handler
  l['handlers']['telegram'] = {}
  l['handlers']['telegram']['class'] = env('LOGGING_HANDLERS_TELEGRAM_CLASS', None)
  l['handlers']['telegram']['token'] = env('LOGGING_HANDLERS_TELEGRAM_TOKEN', None)
  l['handlers']['telegram']['chat_id'] = env('LOGGING_HANDLERS_TELEGRAM_CHAT_ID', None)
  # REF: https://docs.python.org/3/library/logging.config.html#dictionary-schema-details
  l['handlers']['file'] = {}
  l['handlers']['file']['class'] = env('LOGGING_HANDLERS_FILE_CLASS', 'logging.handlers.RotatingFileHandler')
  l['handlers']['file']['formatter'] = env('LOGGING_HANDLERS_FILE_FORMATTER', 'precise')
  l['handlers']['file']['filename'] = env('LOGGING_HANDLERS_FILE_FILENAME', 'logging.log')
  l['handlers']['file']['maxBytes'] = env('LOGGING_HANDLERS_FILE_MAX_BYTES', 1024)
  l['handlers']['file']['backupCount'] = env('LOGGING_HANDLERS_FILE_BACKUP_COUNT', 2)

twitter = _config['twitter']
logging = _config['logging']
