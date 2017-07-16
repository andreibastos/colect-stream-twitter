# coding: utf-8
#
#  Autor Andrei Bastos
#  You can contact me by email (andreibastos@outlook.com) or write to:
#  Via Labic/Ufes - Vitória/ES - Brazil

############### IMPORT's ###############
import json, datetime, time

import threading, tweepy
#######################################


############### VARIÁVEIS GLOBAIS ###################

##### CONSTANTES #####
PATH_KEYS = 'keys_exemplo.json';
PATH_QUERYS = 'query_exemplo.json';
NUM_PER_INSERT = 10


##### VARIÁVEIS ######
log_system = ''
index_key = 0;
keys = []
querys = []
log_system = ''
active_collectors = []

#####################################################

######################### Classes  ###############################
class log_collector():
	logs = []	
	"""docstring for log_collector"""
	def __init__(self):	
		date = time.strftime("%Y%m%d_%H%M",time.gmtime());
		self.filename = 'log_' + date + '.log';		
		self.file = open(self.filename, 'w');

	def read_file(self, filename):
		text = 'read file: {0}.'.format(PATH_QUERYS);
		self.new(text);

	def error(selfe, e):
		text = 'error: {0}.'.format(e) 
		self.new(text);		

	def insert_tweets(self, NUM_PER_INSERT):
		text = 'insert: {0} tweets.'.format(NUM_PER_INSERT);
		self.new(text);		

	def streaming_tweets(self, query):
		text = "Streaming retweets for query '{0}'" .format(unicode(query))
		self.new(text);		

	def new(self, text):
		log = {}
		log['date_created'] = string_time_now();
		log['text'] = text;
		self.logs.append(log);

	def write(self):
		json.dump(self.logs,self.file, indent=4)

class Collector(threading.Thread):
	def __init__(self, query, language, key, log=None):
		self.query = query        
		self.language = language
		self.key = key
		self.log = log
		self.count = 0
		self.active = False
		self.connected = False
		self.list_temp_tweets_to_insert = []  
		super(Collector, self).__init__()

	def run(self):        
		global log_system
		## Quebra a lista em uma unica string com codificação utf8
		q = "".join(x for x in self.query + ", ")
		q = q.trim(",")
		q = str(unicode(q).encode('utf-8'))  		

		## Adiciona no log
		log_system.streaming_tweets(q)
		listener = StreamingListener(self)

		## pega as informações da chave de identificação
		consumer_key, consumer_secret, \
			access_token, access_token_secret = self.key

		## cria a autenticação 
		auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
		auth.set_access_token(access_token, access_token_secret)

		#cria uma escuta 
		self.stream = tweepy.streaming.Stream(auth, listener, timeout=60.0)
		self.active = True

		#enqunto estiver ativo
		while (self.active):
			try:
				self.connected = True

				if self.language != "all":
					self.stream.filter(track=q, languages=[self.language])

				else:
					self.stream.filter(track=q, languages=[''])
					self.connected = False
					print "Connection dropped:", self.query
					if self.active:
						time.sleep(60)

			except socket.gaierror:
				self.connected = False
				print "Stream closed"
				time.sleep(60)
			except Exception:
				self.connected = False
				traceback.print_exc(file=sys.stdout)
				sys.stdout.flush()
				time.sleep(60)

		print "Collector stopped."

class StreamingListener(tweepy.StreamListener):
	def __init__(self, collector, *args, **kwargs):
		self.collector = collector
		self.count = 0
		self.list_temp_tweets_to_insert = []
		super(StreamingListener, self).__init__(*args, **kwargs)

	def on_data(self, data):
		try:	    
			status = json.loads(data)
			# modify to ISOdate in mongo
			#created_at of status
			status['created_at'] = parse(status['created_at'])
			# status['created_at'] = string_to_date(status['created_at'])
			
			status['timestamp_ms'] = long(status['timestamp_ms'])
			#created_at of user of status
			tmp_user = status['user']
			tmp_user['created_at'] = parse(tmp_user['created_at'])
			status['user'] = tmp_user

			#created_at of retweeted of status
			tmp_retweeted = status.get('retweeted_status')            
			if tmp_retweeted:
				tmp_retweeted['created_at'] = parse(tmp_retweeted['created_at'])
				#created_at of user of retweeted of status
				tmp_user = tmp_retweeted['user']
				tmp_user['created_at'] = parse(tmp_user['created_at'])
				tmp_retweeted['user'] = tmp_user
				status['retweeted_status'] = tmp_retweeted	            

		except Exception as e:  
			log_system.error(e)                           
			return False

		twitter_obj = {}                
		twitter_obj['status'] = status

		self.collector.list_temp_tweets_to_insert.append(twitter_obj)        
        
		try:
			if((self.collector.count % NUM_PER_INSERT) == 0):             
				# twitter_collection.insert(self.collector.list_temp_tweets_to_insert)
				log_system.insert_tweets(NUM_PER_INSERT)
				self.collector.list_temp_tweets_to_insert = []			
		except Exception as e:
			log_system.error(e)
			return False

		self.collector.count += 1

		super(StreamingListener, self).on_data(data)

	def on_error(self, status_code):
		print (status_code)
		if status_code == 401:
			raise Exception("Authentication error")

	def on_status(self, status):
		print status
		# pass

##################################################################

######################### Funções ################################
# chaves de idenficação
def read_keys():
	global log_system
	# ler arquivo	
	f = open(PATH_KEYS,'r')

	log_system.read_file(PATH_KEYS)
	return json.loads(f.read());	

def get_key(keys):
	global index_key;
	## troca a chave atual por outra.
	key = keys[index_key];
	index_key +=1
	return key

# querys 
def read_querys():
	global log_system
	f = open(PATH_QUERYS,'r')
	log_system.read_file(PATH_QUERYS)
	return json.loads(f.read());

def string_time_now():
	return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

def string_to_date(date_string):
	global log_system    
	return datetime.datetime.fromtimestamp(time.mktime(time.strptime(date_string,"%a %b %d %H:%M:%S +0000 %Y")), None)

###################################################################

######################## Rotina Principal #########################
def main():
	global log_system
	# cria o objeto de log do systema
	log_system = log_collector();

	# ler as chaves
	keys = read_keys();
	key = get_key(keys); 

	#ler as querys
	querys = read_querys();

	log_system.write();

	for query in querys:
		c = Collector(query['palavras'], query['linguagem'], key)
		# active_collectors.append(c)

if __name__ == '__main__':
	main()