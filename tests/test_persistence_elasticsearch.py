import unittest
import os.path
import json

import settings
from persistence import elasticsearch



class TestPersistenceElasticsearchTwitter(unittest.TestCase):
  """
  elasticsearchengine based on [mongoengine]
  """

  def setUp(self):
    PATH_ABS = os.path.abspath(os.path.dirname(__file__))
    
    uri = settings.datasource.get('es_uri', 'http://localhost:9200')
    self.elasticsearch = elasticsearch.ElasticsearchEngine(uri=uri)
    self.index = 'twitter-test'
    self.type = 'statuses'
    self.routing = 'tests'
    statusues_test_file = os.path.join(PATH_ABS, 'data/twitter-statusues.json')
    with open(statusues_test_file) as statusues_test:
      self.statusues = json.load(statusues_test)
  
  def test_ping(self):
    self.assertTrue(self.elasticsearch.client.ping())

  def test_insert_statusues_bulk(self):
    self.elasticsearch.insert(
      index=self.index,
      type=self.type,
      routing=self.routing,
      doc_or_docs=self.statusues,
    )