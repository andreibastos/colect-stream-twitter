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


path_root = os.path.dirname(__file__)
path_dotenv = os.path.join(path_root, '.env')
path_settings_ini = os.path.join(path_root, 'settings.ini')

config = configparser.ConfigParser()

if os.path.exists(path_settings_ini):
  config.read(path_settings_ini)
else:
  if os.path.exists(path_dotenv):
    dotenv.load_dotenv(path_dotenv)

  env = os.environ.get
  
  config['collector'] = {}
  config['collector']['categorize_namefield'] = env('CATEGORIZE_NAMEFIELD', '')
  config['collector']['chat_id'] = env('TELEGRAM_CHAT_ID', '')
  config['collector']['NUM_PER_INSERT'] = env('NUM_PER_INSERT', '10')
  config['collector']['sendTelegram'] = env('TELEGRAM_BOT_ENABLED', 'False')

  config['endpoint'] = {}
  config['endpoint']['api_bot_telegram'] = env('TELEGRAM_BOT_API_URL', 'http://localhost:4000/telegram_bot')
  config['endpoint']['api_categorize'] = env('CATEGORIZE_00_API_URL', 'http://localhost:4000/categorize_v1')
  config['endpoint']['api_categorize2'] = env('CATEGORIZE_01_API_URL', 'http://localhost:4000/categorize_v2')
  # TODO: Move to datasource
  config['endpoint']['api_database'] = env('DATASOURCE_API_URL', 'http://localhost:4000/categorize_v2')

  config['files'] = {}
  config['files']['filename_log'] = env('TWITTER_LOG_FILE', os.path.join(path_root, 'twitter.log'))
  config['files']['filename_keys'] = env('TWITTER_KEYS_FILE', os.path.join(path_root, 'keys.json'))
  config['files']['filename_querys'] = env('TWITTER_QUERIES', os.path.join(path_root, 'queries.json'))

  config['datasource'] = {}
  config['datasource']['es_uri'] = env('ES_URI', 'localhost:9200')

collector = config['collector']
datasource = config['datasource']
endpoint = config['endpoint']
files = config['files']