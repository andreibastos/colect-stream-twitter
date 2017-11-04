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

__all__ = ['collector', 'endpoint', 'files', 'datasource']


PATH_ROOT = os.path.dirname(__file__)
PATH_DOTENV = os.path.join(PATH_ROOT, '.env')
PATH_SETTINGS_INI = os.path.join(PATH_ROOT, 'settings.ini')

__config = configparser.ConfigParser()

if os.path.exists(PATH_SETTINGS_INI):
  __config.read(PATH_SETTINGS_INI)
else:
  if os.path.exists(PATH_DOTENV):
    dotenv.load_dotenv(PATH_DOTENV)

  env = os.environ.get
  
  __config['collector'] = {}
  __config['collector']['categorize_namefield'] = env('CATEGORIZE_NAMEFIELD', '')
  __config['collector']['chat_id'] = env('TELEGRAM_CHAT_ID', '')
  __config['collector']['NUM_PER_INSERT'] = env('NUM_PER_INSERT', '10')
  __config['collector']['sendTelegram'] = env('TELEGRAM_BOT_ENABLED', 'False')

  __config['endpoint'] = {}
  __config['endpoint']['api_bot_telegram'] = env('TELEGRAM_BOT_API_URL', 'http://localhost:4000/telegram_bot')
  __config['endpoint']['api_categorize'] = env('CATEGORIZE_00_API_URL', 'http://localhost:4000/categorize_v1')
  __config['endpoint']['api_categorize2'] = env('CATEGORIZE_01_API_URL', 'http://localhost:4000/categorize_v2')
  # TODO: Move to datasource
  __config['endpoint']['api_database'] = env('DATASOURCE_API_URL', 'http://localhost:4000/categorize_v2')

  __config['files'] = {}
  __config['files']['filename_log'] = env('TWITTER_LOG_FILE', os.path.join(PATH_ROOT, 'twitter.log'))
  __config['files']['filename_keys'] = env('TWITTER_KEYS_FILE', os.path.join(PATH_ROOT, 'keys.json'))
  __config['files']['filename_querys'] = env('TWITTER_QUERIES', os.path.join(PATH_ROOT, 'queries.json'))

  __config['datasource'] = {}
  __config['datasource']['es_uri'] = env('ES_URI', 'localhost:9200')

collector = __config['collector']
datasource = __config['datasource']
endpoint = __config['endpoint']
files = __config['files']
