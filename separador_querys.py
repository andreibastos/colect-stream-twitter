#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  Autor Andrei Bastos
#  You can contact me by email (andreibastos@outlook.com) or write to:
#  Via Labic/Ufes - Vitória/ES - Brazil

"""
Voce pode fazer uma lista separados em um número de bytes determinado
"""

filename = 'keywords'

def lerArquivo():
	with open(filename, 'r') as f:
		read_data = f.readLines()
		print read_data

lerArquivo()