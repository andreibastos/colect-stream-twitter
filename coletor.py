#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  Autor Andrei Bastos
#  You can contact me by email (andreibastos@outlook.com) or write to:
#  Via Labic/Ufes - Vitória/ES - Brazil

############### IMPORT's ###############
import json, datetime, time
from dateutil.parser import *
from random import randint

import threading, tweepy, socket, traceback, sys

import requests, urllib, urllib3

import lib_text, random_querys
import pymongo

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
filename_keywords = 'keywords.txt'
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
config_elastic_search = None
config_mongodb = None
mongodb = None
elastic_search = {}
flags_enable = {}
geojson= False

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
			#log_system.error('send_telegram', 'mensagem muito grande')
			return {}

	def read_file(self, filename):		
		text = 'read file: {0}.'.format(filename);		
		self.new(text);

	def error(self,source, error_msg):		
		text = 'Erro em {0}. Message_error:{1}.'.format(source, str(error_msg))
		print text 
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
			# log_system.error('stream.filter', e)
			# log_system.new('colect:{0} sleep 60 seconds'.format(self.query))
			time.sleep(5)
			self.stop()
			# log_system.error('stream.filter', 'retornou')	
			self.main()	
		finally:
			#enqunto estiver ativo
			while (self.active):
				try:
					self.connected = True

				except socket.gaierror as sg:
					log_system.error('socket.gaierror', sg)				
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
		self.active = False	
		self.connected = False
		if 	self.stream is not None:
			self.stream.disconnect()		
		querys = ", ".join(c for c in self.query)
		print("Stopping collector: " +  querys)
		#log_system.send_telegram('stopping_colletor: '+ querys)
		
class StreamingListener(tweepy.StreamListener):
	def __init__(self, collector, *args, **kwargs):
		self.collector = collector
		self.count = 0
		self.documents_to_insert = []
		super(StreamingListener, self).__init__(*args, **kwargs)
	def on_data(self, data):		
		try:
			words = None
			categories = None
			blocked = None


			#recebe o dado do twitter e transforma em objeto
			status = json.loads(data)

			#corrige os campos do status para inserir no database corretamente
			if 	not 'limit' in status:

				status_fixed = fix_status(status)


				#categoriza usando endpoints de categorização			
				if flags_enable.get("send_categorie"):				
					categories = get_categories(status_fixed)
					print_keywords(categories)

				#verifica se o dado tem algum block
				if flags_enable.get("blocked_enable"):				
					blocked = is_blocked(status_fixed)

				#captura os links se houver
				if flags_enable.get("get_articles"):				
					articles = get_articles(status_fixed)

				#gera as palavras como um vetor para fazer uma pesquisa de texto
				if flags_enable.get("word_split"):				
					get_words(status_fixed)

				# #ajusta o documento para o database
				document = prepare_document(status_fixed, categories, blocked)

				# #atualiza o contador e adiciona na lista
				self.collector.count += 1
				self.collector.documents_to_insert.append(document) 

				#verifica se tem quantidade suficiente no bulk para ser enviado
				if((self.collector.count % NUM_PER_INSERT) == 0):
					try:

						try:
							if flags_enable.get("send_mongodb_api"):
								insert_tweets(self.collector.documents_to_insert)
						except Exception as e:
							log_system.error('send_mongodb_api',e)	
							raise Exception('send_mongodb_api')	
						try:
							if flags_enable.get("send_elastic"):					
								elastic_search.insert_statusues_bulk(self.collector.documents_to_insert)				
						except Exception as e:													
							self.collector.documents_to_insert = []			
							self.collector.count = 0	
							# raise Exception('send_elastic')	

						try:
							if flags_enable.get("send_mongodb_uri"):
								mongodb.insert_tweets(self.collector.documents_to_insert)
						except Exception as e:
							raise Exception('send_mongodb_uri')	


						print('save in database {0} tweets'.format(len(self.collector.documents_to_insert)))	
						self.collector.documents_to_insert = []			
						self.collector.count = 0	
			
					except Exception as e:
						raise Exception('insert_tweets_num_per_insert')	
						
		except Exception as e:  
			pass
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

	def __init__(self, config_elastic_search):		
		uri = datasource.get('es_uri', 'http://localhost:9200')
		self.elasticsearch = elasticsearch.ElasticsearchEngine(uri=uri)
		self.type = 'statuses'
		self.routing = config_elastic_search.get("routing","labic")		
		self.index = config_elastic_search.get("index","twitter")
		# super(PersistenceElasticsearchTwitter, self).__init__()


	def ping(self):
		self.assertTrue(self.elasticsearch.client.ping())

	def insert_statusues_bulk(self, statusues ):
		today = str( datetime.datetime.utcnow().strftime('%Y-%m-%d'))	
		
		self.elasticsearch.insert(
			index=self.index.replace("YYYY-MM-DD",today),
			type=self.type,
			routing=self.routing,
			doc_or_docs=statusues,
		)

class Mongodb(object):
	"""docstring for Mongodb"""

	def __init__(self, config_mongodb):
		self.uri = datasource["mongodb_uri"]
		self.collection_name = config_mongodb["collection_name"]
		self.database_name = config_mongodb["database_name"]
		self.setup();

	def setup(self):
		try:
			self.client = pymongo.MongoClient(self.uri)
			self.database = self.client[self.database_name]
			self.collection = self.database[self.collection_name]
		except Exception as e:
			raise e


	def insert_tweets(self,documents):
		docs = documents[:]
		try:			
			self.collection.insert_many(docs)
		except Exception as e:
			print str(e)
			log_system.error("Mongodb.insert_tweets",e)
			raise e
		
		
##################################################################


######################### Funções ################################
def get_config():
	global api_database, api_bot_telegram, api_categorize, api_categorize2,NUM_PER_INSERT, filename_keys, filename_querys, filename_log, categorize_namefield, chat_id, sendTelegram,datasource, flags_enable, config_elastic_search,config_mongodb, geojson
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
			config_elastic_search = data.get("elasticsearch", None)
			config_mongodb = data.get("mongodb")	


			geojson = collector.get("geojson", False)

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
					
	except Exception as e:		
		log_system.error('get_config',e)
		raise Exception('get_config', e)
	

def fix_status(status):
	try:
		status["geo"] = None

		#corrige o tempo 
		if status.get('timestamp_ms') :
			status['timestamp_ms'] = long(status['timestamp_ms'])
		else:
			if status.get("created_at"):				
				status['timestamp_ms'] =  long(time.mktime(datetime.datetime.strptime(status['created_at'], '%a %b %d %H:%M:%S +0000 %Y').timetuple())*1000)							

		try:
			#corrige o status.id
			status['id'] = long(status['id'])
			
		except Exception as e:
			log_system.error("status[id]"+status['id'], e)

		try:
			#corrige o place.id
			place = status.get('place')
			if(place):
				place['id'] = str(place.get('id'))
				status['place'] = place			
		except Exception as e:
			log_system.error("place: "+place['id'],e )
	  	
	  	#corrige o user
	  	try:
			user = status['user']
			user['id'] = long(user['id'])
			status['user'] = user  		
	  	except Exception as e:
	  		log_system.error("user: "+user["id"], e)


		
		return status
	except Exception as e:
		raise Exception('fix_status',e)

#adaptação para atender a uma segunda categorização
def get_categories(status,categories={}):	
	screen_name = status['user']['screen_name']
	text = str(unicode(status['text']).encode('utf-8')).decode("utf-8").replace("\n","")			
	print("[(@"+screen_name+") ("+ text + ") ")

	retweeted_status = status.get("retweeted_status")
	quoted_status = status.get("quoted_status")

	if retweeted_status:
		categories= get_categories(retweeted_status, categories=categories)

	if quoted_status:
		categories = get_categories(quoted_status, categories=categories)

	try:
		global api_categorize,api_categorize2	
		categories_api1 = categoriza(status, api_categorize)
		categories_api2 = categoriza(status, api_categorize2)

		keywords = []
		# reverse_geocode = []
		reverse_geocode = {}

		if categories_api1:
			keywords = categories_api1.get("keywords")		
			reverse_geocode = categories_api1.get("reverse_geocode")
			# if reverse_geocode:
			# 	reverse_geocode = list(map(float,reverse_geocode))
			# 	reverse_geocode = list(reversed(reverse_geocode))		

		if categories_api2:
			if keywords:
				keywords = list(set(keywords + categories_api2.get("keywords")))
			else:
				keywords = categories_api2.get("keywords")	

		categories["keywords"] = list(set(keywords))
		if categories:
			categories["keywords"] = list(set(categories["keywords"]+keywords))

		categories["reverse_geocode"] = reverse_geocode
		
	except Exception as e:
		raise Exception('get_categories', e)
	finally:
		return categories

def print_keywords(categories):
	keywordss = categories.get("keywords")
	if keywordss:	
		print "\t("+ ", ".join(x for x in keywordss)+")]\n"


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

	retweeted_status = status.get("retweeted_status")
	quoted_status = status.get("quoted_status")



	if retweeted_status:
		get_words(retweeted_status)
	
	if quoted_status:
		get_words(quoted_status)

	status_terms = []
	try:
		extended_tweets = status.get("extended_tweet")	
		if extended_tweets:
			text = str(unicode(extended_tweets["full_text"]).encode('utf-8')).decode("utf-8").replace("\n","")
			status_terms = lib_text.get_words(text.lower())
			status["full_text_terms"] = status_terms

		text = str(unicode(status['text']).encode('utf-8')).decode("utf-8").replace("\n","")
		status_terms = lib_text.get_words(text.lower())
		status["text_terms"] = status_terms

	except Exception as e:		
		raise Exception('get_words', e)
	finally:
		return status_terms

def prepare_document(status, categories, blocked):
	document = {}				
	try:
		if categories:			
			document['keywords'] = categories.get("keywords", None)
			document['categories'] = categories.get("keywords", None)
			document['reverse_geocode'] = categories.get("reverse_geocode", None)
		if blocked:		
			document['block'] = True
		else:
			document['block'] = False
	
	except Exception as e:
		raise Exception('prepare_document', e)
	finally:
		document['status'] = status
		return document

def insert_tweets(documents):
	headers = {'user-agent': 'coletor-tweets', 'content-type': 'application/json'}		    
	resposta = {'ok':0, 'msg':'error'}
	r = None
	try:
		data=json.dumps(documents)
		r = requests.post(api_database, data=data, headers=headers)
		r.raise_for_status()        
		resposta = {'ok':1, 'msg':'ok'}			
	except (requests.exceptions.HTTPError, requests.exceptions.RequestException) as e:    
		log_system.error('insert_tweets',e)
		log_system.error('insert_tweets', r.text)	    
	finally:
		return resposta

# chaves de idenficação
def read_keys():	
	keys = []
	try:
		global log_system
		# ler arquivo	
		f = open(filename_keys,'r')
		log_system.read_file(filename_keys)
		keys = json.loads(f.read())
	except Exception as e:
		raise Exception('read_keys', e)
	finally:
		return keys;	
		

def get_key():
	global keys
	global index_key;
	try:
		## troca a chave atual por outra.
		index_key = randint(0, len(keys)-1)
		key = keys[index_key];

		text = 'usando \'key\': {0}'.format(key[0]) 
		print(text) 
		log_system.new(text)

	except Exception as e:
		raise Exception('get_key', e)
	finally:
		pass
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

		else:
			place = ""

		if not location:
			location = ""
		else:
			location = str(unicode(location).encode('utf-8'))
	except Exception as e:
		log_system.error('categoriza', e)
		
	params = {'text':text, 'username':username, 'location':location, 'place':place}
	if geojson:
		params["geojson"] = geojson


	try:			
		r = requests.get(api_categorize, params=params)
		
		r.raise_for_status()
		categories = r.json()
		
		return categories
	except Exception as e:
		print (str(e))
		log_system.error('request categoriza', e)		
		return {}




###################################################################

######################## Rotina Principal #########################
def main():
	get_config();

	global log_system, keys, elastic_search, flags_enable,mongodb
	# cria o objeto de log do sistema
	log_system = log_collector();

	# ler as chaves
	keys = read_keys();
	
	#ler as querys
	random_querys.filename_keywords = filename_keywords
	random_querys.filename_keys = filename_keys
	random_querys.gerar_querys();
	querys = read_querys();

	# cria o objeto do elastic search
	if flags_enable.get("send_elastic"):
		elastic_search = PersistenceElasticsearchTwitter(config_elastic_search)


	if flags_enable.get("send_mongodb_uri",False):
		mongodb = Mongodb(config_mongodb)


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