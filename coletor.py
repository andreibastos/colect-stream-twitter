#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  Autor Andrei Bastos
#  You can contact me by email (andreibastos@outlook.com) or write to:
#  Via Labic/Ufes - Vitória/ES - Brazil

############### IMPORT's ###############
import json, datetime, time
from dateutil.parser import *

import threading, tweepy, socket, traceback, sys

import requests, urllib, urllib3

import lib_text 

from persistence import elasticsearch

#import datasource.elasticsearch

#######################################
urllib3.disable_warnings()

############### VARIÁVEIS GLOBAIS ###################
##### Configurações #####
api_database = ''
api_bot_telegram = ''
api_categorize = ''
filename_log = ''
filename_keys = '';
filename_querys = '';
datasource = {}

sendTelegram = False
chat_id = 0

NUM_PER_INSERT = 10 ;
DATE_FORMAT_TWITTER = "%a %b %d %H:%M:%S %z %Y";

##### VARIÁVEIS ######
log_system = ''
index_key = 0;
keys = []
querys = []
log_system = ''
active_collectors = []

elastic_search = {}
flags_enable = {}

#####################################################

######################### Classes  ###############################
class log_collector():
	logs = []	
	"""docstring for log_collector"""
	def __init__(self):	
		date = time.strftime("%Y%m%d_%H%M",time.localtime());
		log = '\"date_created\";\"text\"\n'
		self.filename = filename_log;				
		# self.file.write(log)

	def send_telegram(self,text):
		if not sendTelegram:
			return

		print(text)			
		params = {'chat_id':chat_id,'text':text}
		
		try:			
			r = requests.get(api_bot_telegram, params=params)				
			r.raise_for_status()
			result = r.json()			
			self.new(result)		
		except Exception as e:			
			log_system.error('send_telegram', e)
			return {}

	def read_file(self, filename):		
		text = 'read file: {0}.'.format(filename);		
		self.new(text);

	def error(self,source, error_msg):		
		text = 'Erro em {0}. Message_error:{1}.'.format(source, str(error_msg)) 
		self.send_telegram(text);
		self.new(text);		

	def insert_tweets(self, NUM_PER_INSERT):
		text = 'insert: {0} tweets.'.format(NUM_PER_INSERT);
		self.new(text);		

	def streaming_tweets(self, query):
		text = "streaming retweets for query '{0}'" .format(unicode(query))
		self.new(text);		

	def new(self, text):				
		log = "\"{0}\";\"{1}\"\n".format(str(string_time_now()),text)			
		self.write(log);

	def write(self, log):		
		with open(self.filename, "a") as file_log:
			file_log.write(log)    	
		pass

class Collector(threading.Thread):
	def __init__(self, query, languages, key, log=None):
		self.query = query        
		self.languages = languages
		self.key = key
		self.log = log
		self.auth = {}
		self.count = 0
		self.active = False
		self.connected = False
		self.stream = None
		self.documents_to_insert = []  
		super(Collector, self).__init__()


	def swap_key(self, key):
		if self.key:					
			text_send_telegram = "troquei de {0} para {1} ".format(self.key[0], key[0])
		log_system.send_telegram(text_send_telegram)			
		self.key = key	

	def swap_auth(self):
		
		## pega as informações da chave de identificação
		consumer_key, consumer_secret, \
			access_token, access_token_secret = self.key

		## cria a autenticação 
		self.auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
		self.auth.set_access_token(access_token, access_token_secret)

	def main(self):
		global log_system

		self.swap_auth()

		## Adiciona no log
		log_system.streaming_tweets(self.query)
		print(self.query)		
		
		listener = StreamingListener(self)

		#cria uma escuta 
		self.stream = tweepy.streaming.Stream(self.auth, listener, timeout=60.0)
		self.active = True
		try:
			self.stream.filter(track=self.query, languages=self.languages)
		except Exception as e:
			log_system.error('stream.filter', e)
			log_system.new('colect:{0} spleep 60 seconds'.format(self.query))		

		#enqunto estiver ativo
		while (self.active):
			try:
				self.connected = True

			except socket.gaierror as sg:
				log_system.error('socket.gaierror', e)				
				self.connected = False
				c = active_collectors.pop(int(job_id) - 1)
				c.stop()
				time.sleep(60)
			except Exception as e:
				self.connected = False
				traceback.print_exc(file=sys.stdout)
				sys.stdout.flush()
				time.sleep(60)
				log_system.error('connected', e)								
		return 0

	def run(self):        
		self.main()

	def stop(self):				
		print("Stopping collector: " +  ", ".join(c for c in self.query))
		self.active = False	
		self.connected = False
		if 	self.stream is not None:
			self.stream.disconnect()		
		
class StreamingListener(tweepy.StreamListener):
	def __init__(self, collector, *args, **kwargs):
		self.collector = collector
		self.count = 0
		self.documents_to_insert = []
		super(StreamingListener, self).__init__(*args, **kwargs)
	def on_data(self, data):		
		try:
			#recebe o dado do twitter e transforma em objeto
			status = json.loads(data)

			#corrige os campos do status para inserir no database corretamente
			status_fixed = fix_status(status)

			#categoriza usando endpoints de categorização			
			if flags_enable.get("send_categorie"):				
				categories = get_categories(status_fixed)

			#verifica se o dado tem algum block
			if flags_enable.get("blocked_enable"):				
				blocked = is_blocked(status_fixed)

			#captura os links se houver
			if flags_enable.get("get_articles"):				
				articles = get_articles(status_fixed)

			#gera as palavras como um vetor para fazer uma pesquisa de texto
			if flags_enable.get("word_split"):				
				words = get_words(status_fixed)

			# #ajusta o documento para o database
			document = prepare_document(status_fixed, categories, blocked, words)

			# #atualiza o contador e adiciona na lista
			self.collector.count += 1
			self.collector.documents_to_insert.append(document) 

			#verifica se tem quantidade suficiente no bulk para ser enviado
			if((self.collector.count % NUM_PER_INSERT) == 0):
				if flags_enable.get("send_mongodb_api"):
					insert_tweets(self.collector.documents_to_insert)
					print('save in database {0} tweets'.format(NUM_PER_INSERT))	
				if flags_enable.get("send_elastic"):				
					elastic_search.insert_statusues_bulk(self.collector.documents_to_insert)

				self.collector.documents_to_insert = []			
				self.collector.count = 0

		except Exception as e:  
			log_system.error('on_data',e)						
			
		super(StreamingListener, self).on_data(data)
	def on_error(self, status_code):
		log_system.error('on_error', status_code)				
		if status_code == 401:
			raise Exception("Authentication error")
		if status_code == 420:
			time.sleep(60)
			self.collector.stop()
			self.collector.swap_key(get_key());
			self.collector.swap_auth()
			self.collector.main()

			# raise Exception("Enhance Your Calm")
	def on_status(self, status):
		# print(status)
		pass

class PersistenceElasticsearchTwitter(object):
	"""docstring for PersistenceElasticsearchTwitter"""

	def __init__(self):		
		uri = datasource.get('es_uri', 'http://localhost:9200')
		self.elasticsearch = elasticsearch.ElasticsearchEngine(uri=uri)
		self.index = 'twitter-test'
		self.type = 'statuses'
		self.routing = 'tests'		
		# super(PersistenceElasticsearchTwitter, self).__init__()


	def ping(self):
		self.assertTrue(self.elasticsearch.client.ping())

	def insert_statusues_bulk(self, statusues ):    	
		self.elasticsearch.insert(
			index=self.index,
			type=self.type,
			routing=self.routing,
			doc_or_docs=statusues,
		)
		

##################################################################

######################### Funções ################################
def getConfig():
	global api_database, api_bot_telegram, api_categorize, api_categorize2,NUM_PER_INSERT, filename_keys, filename_querys, filename_log, categorize_namefield, chat_id, sendTelegram,datasource, flags_enable 
	try:
		data = {}
		with open('config.json') as data_file:    
			data = json.load(data_file)

		if data:

			endpoints = data.get('endpoints', None)
			files = data.get('files', None)			
			collector = data.get('collector',None)
			datasource = data.get('datasource',None)
			flags_enable = data.get("flags_enable", None)
			


			NUM_PER_INSERT = collector.get('NUM_PER_INSERT')
			categorize_namefield = collector.get('categorize_namefield')

			sendTelegram = collector.get('sendTelegram')
			chat_id = collector.get('chat_id')

			api_database = endpoints.get('api_database')
			api_bot_telegram = endpoints.get('api_bot_telegram')
			api_categorize = endpoints.get('api_categorize')
			api_categorize2 = endpoints.get('api_categorize2')

			filename_log = files.get('filename_log')
			filename_keys = files.get('filename_keys')
			filename_querys = files.get('filename_querys')								
			pass
		pass
	except Exception as e:		
		log_system.error('getConfig',e)
	pass

def fix_status(status):
	try:
		#corrige o tempo 
		if status.get('timestamp_ms') :
			status['timestamp_ms'] = long(status['timestamp_ms'])
		else:
			if status.get("created_at"):				
				status['timestamp_ms'] =  long(time.mktime(datetime.datetime.strptime(status['created_at'], '%a %b %d %H:%M:%S +0000 %Y').timetuple())*1000)							

		#corrige o status.id
		status['id'] = long(status['id'])

		#corrige o place.id
		place = status.get('place')
		if(place):
			place['id'] = str(place.get('id',""))
			status['place'] = place
	  	
	  	#corrige o user
		user = status['user']
		user['id'] = long(user['id'])
		status['user'] = user
		screen_name = status['user']['screen_name']
								
		text = str(unicode(status['text']).encode('utf-8')).decode("utf-8").replace("\n","")			
		print("@"+screen_name+": "+ text + " : " + status.get("created_at"))
		
		return status
	except Exception as e:
		print 'fix_status'
		raise e

#adaptação para atender a uma segunda categorização
def get_categories(status):
	global api_categorize,api_categorize2	
	categories_api1 = categoriza(status, api_categorize)
	categories_api2 = categoriza(status, api_categorize2)

	keywords = []
	reverse_geocode = []

	if categories_api1:
		keywords = categories_api1.get("keywords")			
		reverse_geocode = categories_api1.get("reverse_geocode")
		if reverse_geocode:
			reverse_geocode = list(map(float,reverse_geocode))                    
	if categories_api2:
		if keywords:
			keywords = list(set(keywords + categories_api2.get("keywords")))
		else:
			keywords = categories_api2.get("keywords")	

	categories = {"reverse_geocode":reverse_geocode,"keywords":keywords}

	return categories


def is_blocked(status):	
	#not implements
	return False

def get_articles(status):
	#not implements
	return {}
	# urls = status["entities"]["urls"]
	# if (urls):
	# 	for url in urls:
	# 		expanded_url = url["expanded_url"]
	# 		print(expanded_url)
			# pass	

def get_words(status):
	try:		
		text = str(unicode(status['text']).encode('utf-8')).decode("utf-8").replace("\n","").lower()
		words = lib_text.remove_punctuation(text)
		words = lib_text.remove_punctuation_special(words)
		# words = str(unicode(words.replace("\n","")).encode("utf-8")).decode("utf-8")				
		words = words.split(" ")
		return words
	except Exception as e:
		print 'get_words'
		raise e
	

def prepare_document(status, categories, blocked, words):
	document = {}				
	document['status'] = status
	if categories:			
		document['keywords'] = categories.get("keywords", None)
		document['categories'] = categories.get("keywords", None)
		document['reverse_geocode'] = categories.get("reverse_geocode", None)
	if blocked:		
		document['blocked'] = True
	else:
		document['blocked'] = False
	if words:			
		document['words'] = words
	return document

def insert_tweets(documents):
	headers = {'user-agent': 'coletor-tweets', 'content-type': 'application/json'}		
	try:
		data=json.dumps(documents)
		r = requests.post(api_database, data=data, headers=headers)
		r.raise_for_status()		
		return {'ok':1, 'msg':'gravado com sucesso'}			
	except requests.exceptions.HTTPError as e:
		log_system.error('saveData',e)
		raise e

# chaves de idenficação
def read_keys():	
	try:
		global log_system
		# ler arquivo	
		f = open(filename_keys,'r')

		log_system.read_file(filename_keys)
		return json.loads(f.read());	
		pass
	except Exception as e:
		raise e

def get_key():
	from random import randint

	global keys
	global index_key;
	## troca a chave atual por outra.
	index_key = randint(0, len(keys)-1)
	key = keys[index_key];

	text = 'usando \'key\': {0}'.format(key[0]) 
	print(text) 
	log_system.new(text)
	
	return key

# querys 
def read_querys():
	global log_system
	f = open(filename_querys,'r')
	log_system.read_file(filename_querys)
	return json.loads(f.read());

# conversões de data
def string_time_now():
	return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def string_to_date(date_string):	 
	return datetime.datetime.fromtimestamp(time.mktime(time.strptime(date_string,"%a %b %d %H:%M:%S +0000 %Y")), None)

def string_to_date(date_string,date_format):	 
	return datetime.datetime.fromtimestamp(time.mktime(time.strptime(date_string,date_format)), None)

def categoriza(status, api_categorize):	
	try:
		text = str(unicode(status['text'].replace("\n", "")).encode('utf-8'))
		username = status['user']['screen_name']
		location = status['user']['location']
		place = status['place'] 
		if place:		
			place = place['full_name']
			place = str(unicode(place).encode('utf-8'))		
			print(place)

		else:
			place = ""

		if not location:
			location = ""
		else:
			location = str(unicode(location).encode('utf-8'))
	except Exception as e:
		log_system.error('categoriza', e)
		
	params = {'text':text, 'username':username, 'location':location, 'place':place}

	try:			
		r = requests.get(api_categorize, params=params)
		
		r.raise_for_status()
		categories = r.json()		
		return categories
	except Exception as e:
		log_system.error('request categoriza', e)		
		return {}




###################################################################

######################## Rotina Principal #########################
def main():
	getConfig();

	global log_system, keys, elastic_search
	# cria o objeto de log do sistema
	log_system = log_collector();

	# ler as chaves
	keys = read_keys();
	
	#ler as querys
	querys = read_querys();

	# cria o objeto do elastic search
	if flags_enable.get("send_elastic"):
		elastic_search = PersistenceElasticsearchTwitter()

	
	index_query = 1;
	for query in querys:
		if index_query % 1 == 0:
			key = get_key(); 

		query['track'] = [str(unicode(x).encode('utf-8')).decode("utf-8") for x in query['track']]
		
		c = Collector(query['track'], query['languages'], key)
				
		c.start()
		active_collectors.append(c)
		index_query = index_query + 1
	
	while True:
		try:
			raw_input('Ctrl+C stop program')
		except (KeyboardInterrupt, EOFError):
			print('stopping program...')
			for c in active_collectors:
				c.stop();				
			print('program stop.')	
			break
			sys.exit(1)
	sys.exit(1)
		


if __name__ == '__main__':
	main()