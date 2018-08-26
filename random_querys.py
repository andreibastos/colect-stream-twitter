#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  Autor Andrei Bastos
#  You can contact me by email (andreibastos@outlook.com) or write to:
#  Via Labic/Ufes - Vitória/ES - Brazil

"""
Voce pode fazer uma lista separados de acordo com o número de chaves
# """



import json, sys, random
filename_keywords = 'keywords.txt'
filename_keys = 'keys'


# chaves de idenficação
def read_len_keys():    
    keys = [] 
    with open(filename_keys) as data_file:    
        keys = json.load(data_file)
    return len(keys);


def ler_arquivo():
    termos = []
    with open(filename_keywords, 'r') as f:
        lines = f.readlines()
        for line in lines:
            line = line.replace("\n","")
            if line:
                termos.append(line)
    return termos

def gerar_tracks(termos,qtd_keys):
    querys = [] # lista de querys vazia
    tracks = [] # lista de tracks vazia
    num_termos_por_chave = int(len(termos)/qtd_keys);
    x=(len(termos)%qtd_keys) #calcula o resto da divisão para saber se precisa vai ter querys com número diferente
    x = -(x-1) #pega o resto e subtrai de 1
    count = 0 #contador de termos, só para conferir se a quantidade de termos  está correta
    for termo in termos:    #para cada termo em termo
        tracks.append(termo)       #vai adicionando os termos no track
        if x>=num_termos_por_chave:    #verifica se tem a quantidade correta    
            search = {"track":tracks,"languages":["pt"], "name":tracks[0],"sizeBytes":len("".join(x for x in tracks))} #cria um objeto do tipo query
            querys.append(search) #adiciona nas querys
            count += len(tracks) #conta 
            tracks = []        #zera o tracks
            x=0 #zera o x
        x +=1

    print ("total",count)

    f = open('querys.json', 'w')
    json.dump(querys, f, indent=4,  ensure_ascii=False)


def gerar_querys():
    termos = ler_arquivo();
    qtd_keys  = read_len_keys();
    random.shuffle(termos) # ordena de forma aleatória    
    gerar_tracks(termos,qtd_keys);