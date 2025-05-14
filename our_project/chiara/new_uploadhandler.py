import os
import json
import numpy as np
import pandas as pd
from sqlite3 import connect
from rdflib import Graph, URIRef, Literal, RDF 
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore

# Python Objects - Edoardo AM Tarpinelli

"""
bisogna verificare come si definiscono i diversi [0], [0..1], [1..*], ...
"""

class IdentifiableEntity(object):
    def __init__(self, identifiers):
        self.id = set()
        for identifier in identifiers:
            self.id.add(identifier) # string[1..*]
    
    def getIds(self) -> list[str]:
        return sorted(list(self.id)) # Convert set to list and sort 

class Journal(IdentifiableEntity):
    def __init__(self, id, title, languages, publisher, seal, license, apc, hasCategory, hasArea):
        super().__init__(id)
        self.title = title # string[1]
        self.languages = set()
        for language in languages:
            self.languages.add(language) # string[1..*]
        self.publisher = publisher if publisher else None # string[0..1]
        self.seal = seal # boolean[1]
        self.license = license # string[1]
        self.apc = apc # boolean[1]
        self.hasCategory = list(hasCategory) if hasCategory else [] # 0..*
        self.hasArea = list(hasArea) if hasArea else [] # 0..*
    
    def getTitle(self):
        return self.title # string
    
    def getLanguages(self): 
        return sorted(list(self.languages)) # list[string]

    def getPublisher(self):
        return self.publisher 
    
    def hasDOAJSeal(self):
        return self.seal # boolean
    
    def getLicence(self):
        return self.license # string
    
    def hasAPC(self):
        return self.apc # boolean
        
    def getCategories(self):
        return list(self.hasCategory) # list[Category]

    def getAreas(self):
        return list(self.hasArea) # list[Area]
               
class Category(IdentifiableEntity):
    def __init__(self, id, quartile):
        super().__init__(id)
        self.quartile = quartile if quartile else None  # string[0..1]
        
    def getQuartile(self):
        return self.quartile # string or None 

class Area(IdentifiableEntity):
    def __init__(self, id):
        super().__init__(id)

# ------------------------------------------------------------------------------------------------------
# Handler, UploadHandler, CategoryUploadHandler and JournalUploadHandler - Chiara Picardi

class Handler(object): #this is the first class, all the others derive from this one 

    # TODO: check it 
    #creating the class 
    def __init__(self):
        self.dbPathOrUrl = ""

    # #creating the class 
    # def __init__(self, dbPathOrUrl : str):
    #     self.dbPathOrUrl = dbPathOrUrl

    #creating the methods 
    def getDbPathOrUrl(self): 
        return self.dbPathOrUrl 

    def setDbPathOrUrl(self, pathOrUrl : str): #: boolean 
        self.dbPathOrUrl = pathOrUrl
        return True


class UploadHandler(Handler):

    def __init__(self):
        super().__init__() 

    def pushDataToDb(self):
        pass # never accessed here and overriden in child classes

#first case: the path is of the relational database the json file

class CategoryUploadHandler(UploadHandler): 
    # TODO
    # def __init__(self):
    #     self.dbPathOrUrl = ""

    def pushDataToDb(self, path: str): 
        
        #creating the database 
        with connect(self.dbPathOrUrl) as con: 
            con.commit() #commit the current transactions to the database  

        with open(path, "r", encoding="utf-8") as c: 
            json_data = json.load(c) #reading the file 

            identifier_list = []
            categories_list = [] 
            area_list = []

            #internal identifier of all the items 
            for idx, item in enumerate(json_data): 
                item_internal_id = ("item_" + str(idx)) 
            
                #1. creating internal ids for each element: identifiers 
                identifiers = item.get("identifiers", []) #selecting the identifiers and using this method to retrive information from a dictionary and take into consideration the possibility that there is not an id 

                #iterating through the identifiers indise the bigger loop of items
                for identifier in identifiers: #i use the iteration because there are more than one in some cases 
                    identifier_list.append({
                            "item_internal_id": item_internal_id,
                            "identifiers": identifier #which is the single identifier 
                            })  #associating the data, with the internal id of the single category but also to the identifies of the whole item so that it's easier to query 

                #2. creating internal ids for the categories, this is trickier because they have more than one value and they can have same id
                #i have to iterate thourg everything but check if the "id" is the same, so it's useful to use a dictionary 
                categories = item.get("categories", []) #especially for category, quartile and area, that in the UML are noted as optional ([0...*]) it's better to do it this way 

                for category in categories: #appunto per me, scrivere cat_id = category["id"] non ha senso perchè category è una lista di un dizionario, io devo internere come dizionario il singolo item 
                    cat_id = category.get("id")
                    #checking for the quartile, because it's optional in the UML
                    quartile = category.get("quartile", "")

                    categories_list.append({
                        "item_internal_id": item_internal_id,
                        "category_id": cat_id,
                        "category_quartile": quartile
                    })
                
            
                #3. creating internal ids for areas, this is the same but without any more value 
                areas = item.get("areas", [])

                for area in areas:
                    area_list.append({
                        "item_internal_id": item_internal_id, 
                        "area": area
                    })
            
            
            #converting the data in dataframes 
            identifiers_df = pd.DataFrame(identifier_list)
            categories_df = pd.DataFrame(categories_list)
            areas_df = pd.DataFrame(area_list)
            # unirle
            merge_1 = pd.merge(identifiers_df, categories_df, left_on='item_internal_id', right_on='item_internal_id')
            merge_2 = pd.merge(merge_1, areas_df, left_on='item_internal_id', right_on='item_internal_id')
            

        with connect(self.dbPathOrUrl) as con:
            # identifiers_df.to_sql("identifiers", con, if_exists="replace", index=False)
            # categories_df.to_sql("categories", con, if_exists="replace", index=False)
            # areas_df.to_sql("areas", con, if_exists="replace", index=False)
            merge_2.to_sql("info", con, if_exists="replace", index=False)

class JournalUploadHandler(UploadHandler): 
    # TODO:
    def __init__(self):
        self.dbPathOrUrl = ""

    def pushDataToDb(self, path):  
        my_graph = Graph() #creating the database

        #classes
        IdentifiableEntity = URIRef("https://schema.org/Thing") #I made this super generic because id is already an attribute
        Journal = URIRef("https://schema.org/Periodical") 
        Category = URIRef("https://schema.org/category")
        Area = URIRef("https://www.wikidata.org/wiki/Q26256810") #I found the one of the topic because area has a different interpretation as more of a physical meaning 

        #predicate 
        hasCategory = URIRef("http://purl.org/dc/terms/subject")
        hasArea = URIRef("https://schema.org/about")

        #attributes related to classes 
        id = URIRef("https://schema.org/identifier")
        title = URIRef("https://schema.org/title")
        languages = URIRef("https://schema.org/inLanguage") 
        publisher = URIRef("https://schema.org/publisher")
        doajSeal = URIRef("https://schema.org/Certification") 
        licence = URIRef("https://schema.org/license")
        apc = URIRef("https://schema.org/isAccessibleForFree")
        quartile = URIRef("https://schema.org/ratingValue") #to revise is it useful? 
        #the impact of the journal in the respecitive field so i use the ranking attribute
    
        #reading the csv  Journal title,Journal ISSN (print version),Journal EISSN (online version),Languages in which the journal accepts manuscripts,Publisher,DOAJ Seal,Journal license,APC
        
        journals = pd.read_csv(path, 
                            keep_default_na=False, 
                            # TODO: chec it
                            # names=["Journal title", "Journal ISSN", "Journal EISSN", "Languages", "Publisher", "DOAJ Seal", "Journal License", "APC"],
                            dtype={
                                "Journal title": "string",
                                "Journal ISSN (print version)": "string",
                                "Journal EISSN (online version)": "string",
                                "Languages in which the journal accepts manuscripts": "string",
                                "Publisher": "string",
                                "DOAJ Seal": "string",
                                "Journal license" : "string",
                                "APC": "string"
                            })
        
        #Problem: DOAJ Seal and APC are booleans according the UML so we need to transform the Yes and No in True and False 
        #source: https://www.geeksforgeeks.org/replace-the-column-contains-the-values-yes-and-no-with-true-and-false-in-python-pandas/ 
        #method: .replace()
        #making sure we don't have problems with different writings (removing whitespaces str.strip() and making everything lowercare .lower())
        journals["DOAJ Seal"] = journals["DOAJ Seal"].str.strip().str.lower() 
        journals["APC"] = journals["APC"].str.strip().str.lower()

        journals = journals.replace({
            "DOAJ Seal" : {"yes": True, "no": False},
            "APC" : {"yes": True, "no": False}
        })

        #giving unique identifiers 
        base_url = "https://comp-data.github.io/res" 
                    
        for idx, row in journals.iterrows(): 
            local_id = "journal-" + str(idx)
            subj = URIRef(base_url + local_id) #new local identifiers for each item in the graph database 

            my_graph.add(((subj, RDF.type, Journal))) #the subject of the row is a journal 
                
            #checking every category in the row (which is none other than a list of vocabularies)
            if row["Journal title"]: 
                my_graph.add((subj, title, Literal(row["Journal title"])))
            
            if row["Journal ISSN (print version)"]: 
                my_graph.add((subj, id, Literal(row["Journal ISSN (print version)"])))
                
            if row["Journal EISSN (online version)"]: 
                my_graph.add((subj, id, Literal(row["Journal EISSN (online version)"])))
                
            if row["Languages in which the journal accepts manuscripts"]: #there could be more languages so it's better to iterate through each of them 
                language_string = row["Languages in which the journal accepts manuscripts"] #1. taking in consideration the whole row
                language_list = language_string.split(",") #as indicated in the F.A.Q on the github they are separated with a comma but inside quotes of course ",", so I separate each item 
                for language in language_list: 
                    language = language.strip() #to delete whitespaces and facilitate the query 
                    my_graph.add((subj, languages, Literal(language)))
                
            if row["Publisher"]: 
                my_graph.add((subj, publisher, Literal(row["Publisher"])))
            
            if row["DOAJ Seal"]: 
                my_graph.add((subj, doajSeal, Literal(row["DOAJ Seal"])))
                
            if row["Journal license"]: 
                my_graph.add((subj, licence, Literal(row["Journal license"])))
                
            if row["APC"]: 
                my_graph.add((subj, apc, Literal(row["APC"]))) # TODO: apc and seal have to be Boleans, not Yes/No

        #opening the connection to upload the graph 
        store = SPARQLUpdateStore() #initializing it as an object 
        try: 
            #endpoint =  self.dbPathOrUrl the endopoint is the url or path of the database 
            store.open((self.dbPathOrUrl, self.dbPathOrUrl))

            for triple in my_graph.triples((None, None, None)): 
                store.add(triple)

            #closing the connection when we finish 
            store.close()

        except Exception as e: 
            print ("Problems with the Blazegraph connection: ", e) #handling errors in the upload part 
        
        #closing the connection when we finish 
        store.close()     