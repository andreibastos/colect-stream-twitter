# Collect Twitter 

[![N|Solid](http://www.labic.net/wp-content/themes/labicv16/assets/logo-lg.png)](http://www.labic.net/)

# New Features!
  - send to elasticSearch

### Installation
requires [Python](https://www.python.org/) 2.7 to run.

Install the dependencies and devDependencies and start the service.

```sh
$ git clone  https://github.com/andreibastos/colect-stream-twitter.git
$ cd colect-stream-twitter
$ virtualenv . 
$ source bin/activate
```
install dependencies
```sh
$ pip install -r requirements.txt
$ 
```

For production configs...

First: edit file config.json
```sh
$ nano config.json
```
```json
{
    "flags_enable":{
        "send_telegram":false,
        "send_elastic":false,
        "send_categorie":false,
        "send_mongodb_api":false,
        "send_mongodb_uri":true,
        "blocked_enable":false,
        "get_articles":false,
        "word_split":true
    },

    "collector":{
        "geojson":false,
        "NUM_PER_INSERT":5,
        "categorize_namefield":"keywords",
        "sendTelegram":false,
        "chat_id": 99999999        
    },
    "elasticsearch":{
        "routing":"your-router",
        "index":"YYYY-MM-DD"
    },
    "mongodb":{
        "collection_name":"collection_name",
        "database_name":"database_name"
    },

    "datasource":{
       "es_uri":"https://elastic@elasticsearch.your-domain.net/",
       "mongodb_uri":"mongodb://127.0.0.1:27017"
    },

    "endpoints":{
        "api_database" : "https://your-api/v2/tweets",
        "api_bot_telegram" : "https://api.telegram.org/bot{{id}}:{{token}}/sendMessage",
        "api_categorize" : "http://localhost:5001/twitter?",
        "api_categorize2" : "http://localhost:6002/twitter/?"
    },
    "files":{
        "filename_log" : "your-project-name.log",
        "filename_keys" : "keys.json",
        "filename_querys" : "querys.json"
    }
}

```

Second: edit file keys.json
```sh
$ nano keys.json
```
```json

[
    ["hmUbVS0OEqLMPNUdvpDp3nF9k", "mXmj4ZxMcKNUc9nF8wF8aOUf0KeVGWLTXeBUrtptx55Cj6JwLV", "70141627-IbOIyGNpKgEtMg6dhU84onqyOHodPMOCJ6HUaUKaT", "uus0nbmPprAErWyvXtmxxQXaiSNZEzZImqMOuJo3bMDHO"]
]

```
Order: [consumer_key, consumer_secret, access_token, access_token_secret]

third: edit file querys.json
```sh
$ nano querys.json
```
```json
[
    {
        "track": [
            "oculos"             
        ], 
        "languages": [
            "pt"
        ]
    }
]
```
### Run
```sh
$ source bin/activate
$ export ES_HOST_URI_00='localhost:9200' && python2.7 coletor.py
```
### Stop
```sh
CTRL-C
```
