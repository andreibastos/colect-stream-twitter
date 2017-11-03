#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  Autor Gustavo RPS
#  You can contact me by email (developers+gustavorps@labic.net)
from __future__ import absolute_import
import os

from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch import (ConnectionError, 
                           ConnectionTimeout, 
                           ImproperlyConfigured,
                           NotFoundError,
                           RequestError,
                           TransportError,)

__all__ = ['ElasticsearchEngine']


class ElasticsearchEngine(object):
  """
  base of IndexTypeDocument based on mongoengine.Document
  """
  def __init__(self, uri='http://localhost:9200', alias='default', **kwargs):
    try:
      _kargs = {
        'maxsize': 25, 
        'send_get_body_as': 'POST',
      }
      _kargs.update(kwargs)
      # self.client = Elasticsearch(uri.split(','), **_kargs)
      self.client = Elasticsearch(hosts=uri.split(','))
    except ImproperlyConfigured as e:
      # TODO: handler properly
      raise e
    except ConnectionError as e:
      # TODO: handler properly
      raise e
    except ConnectionTimeout as e:
      # TODO: handler properly
      raise e

  # def insert(index, type, routing, doc_or_docs, load_bulk=True, **kwargs):
  def insert(self, index, type, routing, doc_or_docs, load_bulk=True):
      try:
        if load_bulk:
          # TODO: 
          # actions = (await __transform(a) for a in doc_or_docs)
          actions = []; append_action = actions.append
          for doc in doc_or_docs:
            action = {
              '_index': index,
              '_routing': routing,
              '_type': type,
              '_source': doc,
            }
            
            append_action(action)

          # result = helpers.bulk(self.client, actions, **kwargs)
          result = helpers.bulk(self.client, actions)
        else:
          action_create = {
            'index': index,
            'doc_type': type,
            'body': doc_or_docs,
          }
          self.client.create(doc_or_docs)
      except TransportError as e:
        # TODO: handler properly
        raise e
      except ConnectionError as e:
        # TODO: handler properly
        raise e
      except TransportError as e:
        # TODO: handler properly
        raise e
