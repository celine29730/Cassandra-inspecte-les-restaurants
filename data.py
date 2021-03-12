#connection au cluster cassandra

import itertools
from types import ClassMethodDescriptorType
from cassandra.cluster import Cluster

#Création d'une classe pour l'ensemble des requêtes
class bdd():

    #Connection au cluster de cassandra

    #Le cluster et la session fournissent également des fonctions de gestion de contexte qui gèrent
    #implicitement l'arrêt lorsque vous quittez la portée.
    @classmethod
    def connection(cls):
        cls.cluster = Cluster(['localhost'], port=9047)
        #connection à la bdd resto
        cls.session = cls.cluster.connect('resto', wait_for_all_pools=True)
        cls.session.execute('USE resto')

    #fermeture du cluster
    @classmethod
    def close_connection(cls):
        cls.cluster.shutdown()

    # infos d'un restaurant à partir de son id
    @classmethod
    def info_resto_par_id(cls, resto_id):
        cls.connection()
        data = cls.session.execute(f"SELECT * FROM restaurant WHERE id={resto_id}")
        dictionnaire={}
        for element in data:
            dictionnaire={'id':element[0], 'borough':element[1], 'buildingnum':element[2],
             'cuisinetype':element[3], 'name':element[4], 'phone':element[5], 'street':element[6], 'zipcode':element[7]}

        cls.close_connection()
        return dictionnaire




    #liste des noms de restaurants à partir du type de cuisine
    @classmethod
    def nom_resto_par_type(cls, resto_type):
        cls.connection()
        data = cls.session.execute(f"SELECT * FROM restaurant WHERE cuisinetype='{resto_type}'")
        liste=[]
        for element in data:
            dictionnaire={'id':element[0], 'borough':element[1], 'buildingnum':element[2],
             'cuisinetype':element[3], 'name':element[4], 'phone':element[5], 'street':element[6], 'zipcode':element[7]}
            liste.append(dictionnaire)
        result={'data':liste}
        cls.close_connection()
        return result


    #nombre d'inspection d'un restaurant à partir de son id restaurant
    @classmethod
    def nb_inspection(cls, resto_id):
        cls.connection()
        data = cls.session.execute(f"SELECT * FROM inspection WHERE idrestaurant={resto_id}")
        liste=[]
        for element in data:
            liste.append(element)
        cls.close_connection()
        result={'data':{f"nombre d'inspection du restaurant {resto_id}":len(liste)}}
        return result



    #noms des 10 premiers restaurants d'un grade donné
    @classmethod
    def nom_grade(cls, grade):
        cls.connection()
        data = cls.session.execute(f"SELECT idrestaurant FROM inspection WHERE grade= '{grade}' GROUP BY idrestaurant limit 10 ALLOW FILTERING;")
        listegrade=[]
        for element in data:
            listegrade.append(element[0])

        cls.close_connection()
        return listegrade


    
bdd().info_resto_par_id(50006392)
print('===========================================')
resto = bdd().nom_resto_par_type('Thai')
print(resto)
print('===========================================')
inspect = bdd().nb_inspection(40786914)
print (inspect)
print('===========================================')
grade = bdd().nom_grade('A')
print(grade)
