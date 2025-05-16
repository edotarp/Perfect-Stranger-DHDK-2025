import os
import json
import numpy as np
import pandas as pd
import time
import re
from sqlite3 import connect, Error
from typing import List, Set
from SPARQLWrapper import SPARQLWrapper, JSON
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
    
    def getIds(self) -> List[str]:
        return sorted(list(self.id)) # Convert set to list and sort 

class Journal(IdentifiableEntity):
    def __init__(self, id, title, languages, publisher, seal, license, apc, hasCategory, hasArea):
        super().__init__(id)
        self.title = title # string[1]
        self.languages = set()
        self.languages = languages # string[1..*]
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

    def setDbPathOrUrl(self, pathOrUrl : str) -> bool: #: boolean 
        self.dbPathOrUrl = pathOrUrl
        return self.dbPathOrUrl == pathOrUrl


class UploadHandler(Handler):

    def __init__(self):
        super().__init__()

    # def pushDataToDb(self, path: str):  #self implied 
    #     if path.lower().endswith(".csv"): 
    #         handler = JournalUploadHandler(self.dbPathOrUrl)
    #         return handler.journalUpload(path) #calling the method after I called the subclass
    #     elif path.lower().endswith(".json"): 
    #         handler = CategoryUploadHandler(self.dbPathOrUrl)
    #         return handler.categoryUpload(path)
    #     else: 
    #         return False 

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

            category_tracker = []
            area_tracker = []

            identifier_list = []
            
            category_mapping_dict = {} #using it to keep track of what we have
            categories_list = []

            area_mapping_dict = {}
            area_list = []
            
            #internal identifier of all the items 
            for idx, item in enumerate(json_data): 
                item_internal_id = ("item_" + str(idx)) 
                
                #1. creating internal ids for each element: identifiers 
                identifiers = item.get("identifiers", []) #selecting the identifiers and using this method to retrive information from a dictionary and take into consideration the possibility that there is not an id 

                #iterating through the identifiers indise the bigger loop of items
                for idx, row in enumerate(identifiers): #i use the iteration because there are more than one in some cases 
                    identifiers_internal_id = (item_internal_id) + ("_identifier_internal_id_") + str(idx) #thi is useful even if redundant because the iteration makes the indexes always restart, so we have many internal id which are 0 or 1 


                    identifier_list.append({
                            "item_internal_id": item_internal_id,
                            # "identifier_internal_id": identifiers_internal_id,
                            "identifiers": row #which is the single identifier 
                            })  #associating the data, with the internal id of the single category but also to the identifies of the whole item so that it's easier to query 

                #2. creating internal ids for the categories, this is trickier because they have more than one value and they can have same id
                #i have to iterate thourg everything but check if the "id" is the same, so it's useful to use a dictionary 
                categories = item.get("categories", []) #especially for category, quartile and area, that in the UML are noted as optional ([0...*]) it's better to do it this way 

                for row in categories: #appunto per me, scrivere cat_id = category["id"] non ha senso perchè category è una lista di un dizionario, io devo internere come dizionario il singolo item 
                    cat_id = row.get("id")
                    cat_quart = row.get("quartile")
                    if cat_quart != None:
                        join = (cat_id, cat_quart)
                        id_quartile_combination = "_".join(join)
                    elif cat_quart == None:
                        id_quartile_combination = cat_id
                    if id_quartile_combination not in category_mapping_dict: #checking if the category is not already in the dictionary 
                        category_id = ("category_") + str(len(category_mapping_dict))
                        category_mapping_dict[id_quartile_combination] = (category_id)
                    else: 
                        category_id = category_mapping_dict[id_quartile_combination] #if it's already inside the dict consider the original id 

                    #checking for the quartile, because it's optional in the UML
                    quartile = row.get("quartile", "")

                    categories_list.append({
                        "item_internal_id": item_internal_id,
                        "category_internal_id" : category_id,
                        "category_id": cat_id,
                        "category_quartile": quartile
                    })

                    category_tracker.append({
                        "item_internal_id": item_internal_id,
                        "category_internal_id": category_id
                    })
                
                
                #3. creating internal ids for areas, this is the same but without any more value 
                areas = item.get("areas", [])

                for area in areas: 
                    if area not in area_mapping_dict: 
                        area_id = (("area_") + str(len(area_mapping_dict)))
                        area_mapping_dict[area] = area_id
                    else: 
                        area_id = area_mapping_dict[area]
                
                    area_list.append({
                        "item_internal_id": item_internal_id, 
                        "area_internal_id": area_id,
                        "area": area
                    })

                    area_tracker.append({
                        "item_internal_id": item_internal_id,
                        "area_internal_id": area_id
                    })
            
            
            #converting the data in dataframes 
            identifiers_df = pd.DataFrame(identifier_list)
            categories_df = pd.DataFrame(categories_list)
            areas_df = pd.DataFrame(area_list)
            cat_df = pd.DataFrame(category_tracker)
            ar_df = pd.DataFrame(area_tracker)
            # unirle
            merge_1 = pd.merge(identifiers_df, categories_df, left_on='item_internal_id', right_on='item_internal_id')
            merge_2 = pd.merge(merge_1, areas_df, left_on='item_internal_id', right_on='item_internal_id')

            # create hasCategory table
            hasCategory = pd.merge(identifiers_df, cat_df, left_on='item_internal_id', right_on='item_internal_id')

            # create hasArea table
            hasArea = pd.merge(identifiers_df, ar_df, left_on='item_internal_id', right_on='item_internal_id')
        try:
            with connect(self.dbPathOrUrl) as con:
                # identifiers_df.to_sql("identifiers", con, if_exists="replace", index=False)
                # categories_df.to_sql("categories", con, if_exists="replace", index=False)
                # areas_df.to_sql("areas", con, if_exists="replace", index=False)
                merge_2.to_sql('info', con, if_exists='replace', index=False)
                hasCategory.to_sql('hasCategory', con, if_exists='replace', index=False)
                hasArea.to_sql('hasArea', con, if_exists='replace', index=False)
                    # TODO: why not 'con.commit()'
            return True
        except Exception as e:
            print(f"Error occurred while pushing data to DB: {str(e)}")
            return False 
            
#second case: the path is the one of a graph database, the csv file

# class JournalUploadHandler(UploadHandler): 
#     # TODO:
#     def __init__(self):
#         self.dbPathOrUrl = ""
    
#     def check_if_journal_exists(self, graph, issn):
#         """Controlla se un giornale con un dato ISSN esiste già nel grafo."""
#         query = f"""
#             ASK {{
#                 ?journal <https://schema.org/identifier> "{issn}" .
#             }}
#         """
#         graph.setQuery(query)
#         graph.setReturnFormat(JSON)  # È buona pratica impostare il formato di ritorno
#         try:
#             results = graph.query().convert()
#             return bool(results["boolean"])  # I risultati di ASK hanno un campo "boolean"
#         except Exception as e:
#             print(f"Errore durante la query ASK: {e}")
#         return False

#     def pushDataToDb(self, path):  
#         my_graph = Graph() #creating the database

#         #classes
#         IdentifiableEntity = URIRef("https://schema.org/Thing") #I made this super generic because id is already an attribute
#         Journal = URIRef("https://schema.org/Periodical") 
#         Category = URIRef("https://schema.org/category")
#         Area = URIRef("https://www.wikidata.org/wiki/Q26256810") #I found the one of the topic because area has a different interpretation as more of a physical meaning 

#         #predicate 
#         hasCategory = URIRef("http://purl.org/dc/terms/subject")
#         hasArea = URIRef("https://schema.org/about")

#         #attributes related to classes 
#         id = URIRef("https://schema.org/identifier")
#         title = URIRef("https://schema.org/title")
#         languages = URIRef("https://schema.org/inLanguage") 
#         publisher = URIRef("https://schema.org/publisher")
#         doajSeal = URIRef("https://schema.org/Certification") 
#         licence = URIRef("https://schema.org/license")
#         apc = URIRef("https://schema.org/isAccessibleForFree")
#         quartile = URIRef("https://schema.org/ratingValue") #to revise is it useful? 
#         #the impact of the journal in the respecitive field so i use the ranking attribute
    
#         #reading the csv  Journal title,Journal ISSN (print version),Journal EISSN (online version),Languages in which the journal accepts manuscripts,Publisher,DOAJ Seal,Journal license,APC
#         journals = pd.read_csv(path, keep_default_na=False, dtype={
#             "Journal title": "string",
#             "Journal ISSN (print version)": "string",
#             "Journal EISSN (online version)": "string",
#             "Languages in which the journal accepts manuscripts": "string",
#             "Publisher": "string",
#             "DOAJ Seal": "string",
#             "Journal license" : "string",
#             "APC": "string"
#         })

#         base_url = "https://comp-data.github.io/res/"

#         store = SPARQLWrapper(self.dbPathOrUrl) # Use SPARQLWrapper for querying

#         try:
#             store.setReturnFormat(JSON)
#             for idx, row in journals.iterrows():
#                 local_id = "journal-" + str(idx)
#                 subj = URIRef(base_url + local_id)

#                 issn_print = str(row["Journal ISSN (print version)"])
#                 issn_online = str(row["Journal EISSN (online version)"])

#                 exists = False
#                 if issn_print and self.check_if_journal_exists(store, issn_print):
#                     exists = True
#                 elif issn_online and self.check_if_journal_exists(store, issn_online):
#                     exists = True

#                 if not exists:
#                     my_graph.add((subj, RDF.type, Journal))
#                     if row["Journal title"]:
#                         my_graph.add((subj, title, Literal(row["Journal title"])))
#                     if issn_print:
#                         my_graph.add((subj, id, Literal(issn_print)))
#                     if issn_online:
#                         # Potresti voler gestire il caso in cui ci sono due ISSN diversi
#                         my_graph.add((subj, id, Literal(issn_online)))
#                     if row["Languages in which the journal accepts manuscripts"]:
#                         language_string = row["Languages in which the journal accepts manuscripts"]
#                         language_list = language_string.split(",")
#                         for language in language_list:
#                             language = language.strip()
#                             my_graph.add((subj, languages, Literal(language)))
#                     if row["Publisher"]:
#                         my_graph.add((subj, publisher, Literal(row["Publisher"])))
#                     if row["DOAJ Seal"]:
#                         my_graph.add((subj, doajSeal, Literal(row["DOAJ Seal"])))
#                     if row["Journal license"]:
#                         my_graph.add((subj, licence, Literal(row["Journal license"])))
#                     if row["APC"]:
#                         my_graph.add((subj, apc, Literal(row["APC"])))

#             # Opening the connection to upload the graph
#             update_store = SPARQLUpdateStore()
#             update_store.open((self.dbPathOrUrl, self.dbPathOrUrl))
#             for triple in my_graph.triples((None, None, None)):
#                 update_store.add(triple)
#             update_store.close()
#             return True
#         except Exception as e:
#             print ("Problems with the Blazegraph connection: ", e)
#             return False
#         # finally:
#         #     if 'update_store' in locals() and update_store.is_open():
#         #         update_store.close() 
#         #     return True 

# last update from chiara

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
        # journals["DOAJ Seal"] = journals["DOAJ Seal"].str.strip().str.lower() 
        # journals["APC"] = journals["APC"].str.strip().str.lower()

        # journals = journals.replace({
        #     "DOAJ Seal" : {"yes": True, "no": False},
        #     "APC" : {"yes": True, "no": False}
        # })

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
            return True

        except Exception as e: 
            print ("Problems with the Blazegraph connection: ", e) #handling errors in the upload part 
            
            #closing the connection when we finish 
            store.close()
            return False
           
# ------------------------------------------------------------------------------------------------------
# CategoryQueryHandler and QueryHandler - Cecilia Vesci

class QueryHandler(Handler):
    def __init__(self, dbPathOrUrl=""):  
        super().__init__()
        self.dbPathOrUrl = dbPathOrUrl

    def getById(self, id: str):
        """
        Questo metodo cerca un'entità identificabile per ID nel database.
        """
        # raise NotImplementedError("Questo metodo deve essere implementato nelle sottoclassi.")
        pass

class CategoryQueryHandler(QueryHandler):
  
    def __init__(self, dbPathOrUrl=""):
            super().__init__()
            self.dbPathOrUrl = dbPathOrUrl
            self.db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), self.dbPathOrUrl))
    
    def getById(self, id: str):
        return pd.DataFrame() # return empty dataframe as no IdentifiableEntity in relational db
    
    def _execute_query(self, query: str, params: tuple = None):
        try:
            with connect(self.dbPathOrUrl) as con:
                if params:
                    df = pd.read_sql_query(query, con, params=params)
                else:
                    df = pd.read_sql_query(query, con)
                return df
        except Error as e:
            if "no such table: info" in str(e):
                 print(f"Database error: The table 'info' does not exist in {self.dbPathOrUrl}")
            else:
                 print(f"Database error during query execution: {e}")
           
            # This part might need adjustment based on expected columns for each method caller
            return pd.DataFrame()
        except Exception as e:
            print(f"An unexpected error occurred during query execution: {e}")
            return pd.DataFrame()

    # Prendere tutte le categorie (distinte)
    def getAllCategories(self):
        """
        return all categories included in database with no repetition
        """    
        # Select distinct non-null category IDs from the 'info' table
        query = "SELECT DISTINCT category_id, category_quartile FROM info WHERE category_id IS NOT NULL"
        df = self._execute_query(query)
        # Ensure correct column name if df is empty
        if df.empty and 'category_id' not in df.columns:
             return pd.DataFrame(columns=['category_id'])
        return df

        # Prendere tutte le aree (distinte)
    def getAllAreas(self):
        """
        return all area included in database with no repetition
        """
        # Select distinct non-null areas from the 'info' table
        query = "SELECT DISTINCT area FROM info WHERE area IS NOT NULL"
        df = self._execute_query(query)
        # Ensure correct column name if df is empty
        if df.empty and 'area' not in df.columns:
             return pd.DataFrame(columns=['area'])
        return df

    def getCategoriesWithQuartile(self, quartiles=Set[str]):
        """
        if quartiles is given it returns a df showing all the categories associated to that quartile
        if quartiles is not given, it returns a df with all categories 
        """
        if not quartiles:
            # Return all distinct category/quartile pairs
            query = "SELECT DISTINCT category_id, category_quartile FROM info WHERE category_id IS NOT NULL"
            df = self._execute_query(query)
        else:
            # Build the WHERE clause carefully to handle different types and NULL
            conditions = []
            params = []
            has_null_quartile_request = False

            for q in quartiles:
                if q is None or pd.isna(q):
                    has_null_quartile_request = True
                else:
                    # Add placeholder for non-null quartiles
                    conditions.append("category_quartile = ?")
                     # Convert to string for consistent comparison if quartiles can be numbers/strings
                    params.append(str(q))

            where_clause = ""
            if conditions:
                where_clause = "(" + " OR ".join(conditions) + ")"

            if has_null_quartile_request:
                if where_clause:
                    where_clause += " OR category_quartile IS NULL"
                else:
                    where_clause = "category_quartile IS NULL"
                    # Construct the final query only if there's a valid where_clause
            if not where_clause:
                 # This case should ideally not be reached if quartiles list is not empty,
                 # but as a safeguard return empty df matching schema.
                 return pd.DataFrame(columns=['category_id', 'category_quartile'])

            query = f"""
                SELECT DISTINCT category_id, category_quartile
                FROM info
                WHERE category_id IS NOT NULL AND ({where_clause})
            """
            df = self._execute_query(query, tuple(params))

        # Ensure correct column names if df is empty
        if df.empty and ('category_id' not in df.columns or 'category_quartile' not in df.columns):
             return pd.DataFrame(columns=['category_id', 'category_quartile'])
        return df

    def getCategoriesAssignedToAreas(self, areas=Set[str]):
        """
        if areas is given it returns a df showing all the categories associated to that areas
        if areas is not given, it returns a df with all categories
        """
        if not areas:
            # If no areas specified, get all distinct categories/quartiles
            query = "SELECT DISTINCT category_id, category_quartile FROM info WHERE category_id IS NOT NULL"
            df = self._execute_query(query)
        else:
            # Create placeholders for the areas in the IN clause
            placeholders = ','.join('?' for _ in areas)
            query = f"""
                SELECT DISTINCT category_id, category_quartile
                FROM info
                WHERE area IN ({placeholders}) AND category_id IS NOT NULL
            """
            df = self._execute_query(query, tuple(areas))

        # Ensure correct column names if df is empty
        if df.empty and ('category_id' not in df.columns or 'category_quartile' not in df.columns):
             return pd.DataFrame(columns=['category_id', 'category_quartile'])
        return df

    def getAreasAssignedToCategories(self, categories=Set[str]):
        """
        if category is given it returns a df showing all the areas associated to that category
        if category is not given, it returns a df with all areas
        """
        if not categories:
            # If no categories specified, get all distinct areas
            query = "SELECT DISTINCT area FROM info WHERE area IS NOT NULL"
            df = self._execute_query(query)
        else:
            # Create placeholders for the categories in the IN clause
            placeholders = ','.join('?' for _ in categories)
            query = f"""
                SELECT DISTINCT area
                FROM info
                WHERE category_id IN ({placeholders}) AND area IS NOT NULL
            """
            df = self._execute_query(query, tuple(categories))

        # Ensure correct column name if df is empty
        if df.empty and 'area' not in df.columns:
             return pd.DataFrame(columns=['area'])
        return df


# ------------------------------------------------------------------------------------------------------
# JournalQueryHandler - Faride

class JournalQueryHandler(QueryHandler):
    # TODO: we have to deal with repetition in all the methods
    def __init__(self):
        self.dbPathOrUrl = ""

    def execute_sparql_query(self, query):
        # print("è entrato")
        sparql = SPARQLWrapper(self.dbPathOrUrl)
        sparql.setReturnFormat(JSON)
        sparql.setQuery(query)
        try:
            # print("entrato in try")
            result = sparql.queryAndConvert()
            # print("uscito da try")
        except Exception as e:
            print("SPARQL Error:", e)
            return pd.DataFrame()

        if result and "head" in result and "vars" in result["head"] and "results" in result and "bindings" in result["results"]:
            journals_data = {}
            # counter = 0
            for row in result["results"]["bindings"]:
                # print(counter)
                # counter += 1
                journal_uri = row.get("journal", {}).get("value")
                if not journal_uri:
                    continue

                if journal_uri not in journals_data:
                    journals_data[journal_uri] = {}
                    for var in result["head"]["vars"]:
                        journals_data[journal_uri][var] = row.get(var, {}).get("value", "") if var not in ["identifier", "languages"] else []

                for var in result["head"]["vars"]:
                    value = row.get(var, {}).get("value")
                    if var == "identifier" and value not in journals_data[journal_uri]["identifier"]:
                        journals_data[journal_uri]["identifier"].append(value)
                    elif var == "languages" and value not in journals_data[journal_uri]["languages"]:
                        journals_data[journal_uri]["languages"].append(value)
                    elif var not in ["journal", "identifier", "languages"]:
                        journals_data[journal_uri][var] = value
            # print("end if statement")
        # Converti il dizionario in una lista di dizionari per creare il DataFrame
            list_data = []
            for uri, data in journals_data.items():
                list_data.append(data)

            df = pd.DataFrame(list_data)
            return df.replace(np.nan, "")
        else:
            return pd.DataFrame()

    def getById(self, identifier):
        query = f"""
        SELECT DISTINCT ?journal ?id ?title ?publisher ?license ?apc ?seal ?language ?type
        WHERE {{
            ?journal <https://schema.org/identifier> ?id .
            FILTER(str(?id) = '{identifier}').
           OPTIONAL {{ ?journal <https://schema.org/title> ?title }} .
           OPTIONAL {{ ?journal <https://schema.org/publisher> ?publisher }} .
           OPTIONAL {{ ?journal <https://schema.org/license> ?license }} .
           OPTIONAL {{ ?journal <https://schema.org/isAccessibleForFree> ?apc }} .
           OPTIONAL {{ ?journal <https://schema.org/Certification> ?seal }} .
           OPTIONAL {{ ?journal <https://schema.org/inLanguage> ?language }} .
          OPTIONAL {{ ?journal a ?type }} .
        }}
        """        
        return self.execute_sparql_query(query)

    def getAllJournals(self):
        query =         """
        SELECT DISTINCT ?journal ?title ?identifiers ?languages ?publisher ?license ?apc ?seal
        WHERE {
          ?journal a <https://schema.org/Periodical> ;
                   <https://schema.org/title> ?title ;
                   <https://schema.org/identifier> ?identifiers ;
                   <https://schema.org/inLanguage> ?languages ;
                   <https://schema.org/license> ?license .
          OPTIONAL { ?journal <https://schema.org/publisher> ?publisher }
          OPTIONAL { ?journal <https://schema.org/isAccessibleForFree> ?apc }
          OPTIONAL { ?journal <https://schema.org/Certification> ?seal }
        }
        """
        return self.execute_sparql_query(query)   #if there was an error the issn schema can be altered
                                                  
    def getJournalsWithTitle(self, title: str):
        escaped_title = title.replace('"', '\\"')
        query = f"""
        SELECT DISTINCT ?journal ?title ?identifiers ?languages ?publisher ?license ?apc ?seal
        WHERE {{
            ?journal a <https://schema.org/Periodical> ;
                    <https://schema.org/title> ?title ;
                    <https://schema.org/identifier> ?identifiers ;
                    <https://schema.org/inLanguage> ?languages ;
                    <https://schema.org/license> ?license .
            
            OPTIONAL {{ ?journal <https://schema.org/publisher> ?publisher }}
            OPTIONAL {{ ?journal <https://schema.org/isAccessibleForFree> ?apc }}
            OPTIONAL {{ ?journal <https://schema.org/Certification> ?seal }}
            FILTER(CONTAINS(LCASE(?title), "{escaped_title.lower()}"))
        }}
        """
        return self.execute_sparql_query(query)

    def getJournalsPublishedBy(self, publisher: str):
        publisher = publisher.replace('"', '\\"')
        query = f"""
        SELECT DISTINCT ?journal ?title ?identifiers ?languages ?publisher ?license ?apc ?seal
        WHERE {{
            ?journal a <https://schema.org/Periodical> ;
                    <https://schema.org/title> ?title ;
                    <https://schema.org/identifier> ?identifiers ;
                    <https://schema.org/inLanguage> ?languages ;
                    <https://schema.org/license> ?license .
            
            OPTIONAL {{ ?journal <https://schema.org/publisher> ?publisher }}
            OPTIONAL {{ ?journal <https://schema.org/isAccessibleForFree> ?apc }}
            OPTIONAL {{ ?journal <https://schema.org/Certification> ?seal }}
            FILTER(CONTAINS(LCASE(?publisher), "{publisher.lower()}"))
        }}
        """
        return self.execute_sparql_query(query)

    def getJournalsWithLicense(self, license_set: Set[str]):
        # Sanitize le stringhe nella lista per evitare problemi con le virgolette nella query
        sanitized_licenses = [lic.replace('"', '\\"') for lic in license_set]
        # Costruisci la parte della clausola FILTER con l'operatore IN
        filter_clause = 'FILTER (LCASE(?license) IN (' + ', '.join([f'"{lic.lower()}"' for lic in sanitized_licenses]) + '))'

        query = f"""
        SELECT DISTINCT ?journal ?title ?identifiers ?languages ?publisher ?license ?apc ?seal
        WHERE {{
            ?journal a <https://schema.org/Periodical> ;
                    <https://schema.org/title> ?title ;
                    <https://schema.org/identifier> ?identifiers ;
                    <https://schema.org/inLanguage> ?languages ;
                    <https://schema.org/license> ?license .

            OPTIONAL {{ ?journal <https://schema.org/publisher> ?publisher }}
            OPTIONAL {{ ?journal <https://schema.org/isAccessibleForFree> ?apc }}
            OPTIONAL {{ ?journal <https://schema.org/Certification> ?seal }}
            {filter_clause}
        }}
        """
        return self.execute_sparql_query(query)

    def getJournalsWithAPC(self):
        query = """
        SELECT DISTINCT ?journal ?title ?identifiers ?languages ?publisher ?license ?apc ?seal
        WHERE {{
            ?journal a <https://schema.org/Periodical> ;
                    <https://schema.org/title> ?title ;
                    <https://schema.org/identifier> ?identifiers ;
                    <https://schema.org/inLanguage> ?languages ;
                    <https://schema.org/license> ?license .
            
            OPTIONAL {{ ?journal <https://schema.org/publisher> ?publisher }}
            OPTIONAL {{ ?journal <https://schema.org/isAccessibleForFree> ?apc }}
            OPTIONAL {{ ?journal <https://schema.org/Certification> ?seal }}
          FILTER(LCASE(?apc) = "yes")
        }}
        """
        return self.execute_sparql_query(query)

    def getJournalsWithDOAJSeal(self):
        query = """
        SELECT DISTINCT ?journal ?title ?identifiers ?languages ?publisher ?license ?apc ?seal
        WHERE {{
            ?journal a <https://schema.org/Periodical> ;
                    <https://schema.org/title> ?title ;
                    <https://schema.org/identifier> ?identifiers ;
                    <https://schema.org/inLanguage> ?languages ;
                    <https://schema.org/license> ?license .
            
            OPTIONAL {{ ?journal <https://schema.org/publisher> ?publisher }}
            OPTIONAL {{ ?journal <https://schema.org/isAccessibleForFree> ?apc }}
            OPTIONAL {{ ?journal <https://schema.org/Certification> ?seal }}
          FILTER(LCASE(?seal) = "yes")
        }}
        """
        return self.execute_sparql_query(query)

# ------------------------------------------------------------------------------------------------------
# Basic Query Engine - Edoardo AM Tarpinelli

class BasicQueryEngine(object):
    # TODO: we have to deal with partial input strings
    def __init__(self):
        self.journalQuery = [] # [0..*] - graph
        self.categoryQuery = [] # [0..*] - rdb

    def getCategoryQuartile_mapped(self, all_identifiers):
        identifier_to_category_quartiles = {}
        if not self.categoryQuery or not all_identifiers:
            return identifier_to_category_quartiles

        for handler in self.categoryQuery:
            with connect(handler.dbPathOrUrl) as con:
                placeholders = ', '.join(['?'] * len(all_identifiers))
                query = f"""
                    SELECT identifiers, category_id, category_quartile
                    FROM info
                    WHERE identifiers IN ({placeholders})
                """
                df = pd.read_sql_query(query, con, params=all_identifiers)
                if not df.empty:
                    for index, row in df.iterrows():
                        identifier = row['identifiers']
                        category_id = row['category_id']
                        category_quartile = row['category_quartile']
                        if identifier not in identifier_to_category_quartiles:
                            identifier_to_category_quartiles[identifier] = []
                        # Memorizziamo la coppia (category_id, category_quartile) come una tupla
                        if (category_id, category_quartile) not in identifier_to_category_quartiles[identifier]:
                            identifier_to_category_quartiles[identifier].append((category_id, category_quartile))
        return identifier_to_category_quartiles

    def gethasArea_mapped(self, all_identifiers):
        identifier_to_areas = {}
        if not self.categoryQuery or not all_identifiers:
            return identifier_to_areas

        for handler in self.categoryQuery:
            with connect(handler.dbPathOrUrl) as con:
                placeholders = ', '.join(['?'] * len(all_identifiers))
                query = f"""
                    SELECT identifiers, area
                    FROM info
                    WHERE identifiers IN ({placeholders})
                """
                df = pd.read_sql_query(query, con, params=all_identifiers)
                if not df.empty:
                    for index, row in df.iterrows():
                        identifier = row['identifiers']
                        area_value = row['area']
                        if identifier not in identifier_to_areas:
                            identifier_to_areas[identifier] = []
                        if area_value not in identifier_to_areas[identifier]:
                            identifier_to_areas[identifier].append(area_value)
        return identifier_to_areas
    
    def createJournalObject(self, input_dataframe):
        journal_list = list()
        all_journal_identifiers_set = set()
        for index, row in input_dataframe.iterrows(): # Usa iterrows() per iterare sulle righe
            identifiers = row['identifiers']
            if isinstance(identifiers, list):
                all_journal_identifiers_set.update(identifiers)
            elif isinstance(identifiers, str):
                all_journal_identifiers_set.add(identifiers)

        all_journal_identifiers = list(all_journal_identifiers_set)

        # Recupera la mappatura identifier -> aree
        identifier_to_areas = self.gethasArea_mapped(all_journal_identifiers)
        identifier_to_categories = self.getCategoryQuartile_mapped(all_journal_identifiers)
        # counter = 0
        for row in input_dataframe.itertuples(index=False):
            # Gestisci il caso in cui 'identifier' sia una lista o una stringa
            first_identifier = row.identifiers[0] if isinstance(row.identifiers, list) and len(row.identifiers) > 0 else row.identifiers if isinstance(row.identifiers, str) else None
            has_area = identifier_to_areas.get(first_identifier, [])
            has_category = identifier_to_categories.get(first_identifier, [])
            # if counter < 10:
            #     print(row.journal,
            #         row.title,
            #         list(row.languages) if isinstance(row.languages, list) and len(row.languages) > 0 else [],
            #         row.publisher if pd.notna(row.publisher) else None,
            #         row.seal if pd.notna(row.seal) and str(row.seal).lower() == 'yes' else False,
            #         row.license if pd.notna(row.license) else None,
            #         row.apc if pd.notna(row.apc) and str(row.apc).lower() == 'yes' else False,
            #         has_category,
            #         has_area)
            # counter += 1
            journal = Journal(
                id=row.journal,
                title=row.title,
                languages=list(row.languages) if isinstance(row.languages, list) and len(row.languages) > 0 else [],
                publisher=row.publisher if pd.notna(row.publisher) else None,
                seal=row.seal if pd.notna(row.seal) and str(row.seal).lower() == 'yes' else False,
                license=row.license if pd.notna(row.license) else None,
                apc=row.apc if pd.notna(row.apc) and str(row.apc).lower() == 'yes' else False,
                hasCategory=has_category, # Implementa la logica di recupero efficiente se necessario
                hasArea=has_area
            )
            journal_list.append(journal)

        return journal_list

    def createCategoryObject(self, input_dataframe):
        category_list = list()
        for index, row in input_dataframe.iterrows():
            category = Category(
                id=row['category_id'],
                quartile=row['category_quartile']
            )
            category_list.append(category)
        return category_list

    def createAreaObject(self, input_dataframe):
        area_list = list()
        for index, row in input_dataframe.iterrows():
            area = Area(
                id=row['area'],
            )
            area_list.append(area)
        return area_list
    

    def cleanJournalHandlers(self):
        self.journalQuery.clear() # Boolean
        return True
    
    def cleanCategoryHandlers(self):
        self.categoryQuery.clear() # Boolean
        return True

    def addJournalHandler(self, JournalHandler):
        self.journalQuery.append(JournalHandler) # Boolean
        return True

    def addCategoryHandler(self, CategoryHandler):
        # set category handler
        self.categoryQuery.append(CategoryHandler) # Boolean
        return True
    
    def getEntityById(self, input_identifier: str) -> IdentifiableEntity:
    #     if not self.metadataQuery:
    #         return None
        
    #     handler = self.metadataQuery[0]
    #     df = handler.getById(input_identifier)
        
    #     if df.empty: 
    #         return None
        
    #     if '' in df.columns:
    #         list = self.createObjectList(df)
    #         if list:
    #             return list[0]  
        
    #     if '' in df.columns and 'id' in df.columns: 
    #         return Journal(df.iloc[0]["id"], df.iloc[0]["name"])
        
        return None

    def getAllJournals(self) -> List[Journal]:
        
        if len(self.journalQuery) > 0:
            all_journal_dfs = []
            for handler in self.journalQuery:
                # print(handler)
                new_journal_df = handler.getAllJournals()
                 

        return self.createJournalObject(new_journal_df)
    
    def getJournalsWithTitle(self, partialTitle: str) -> List[Journal]:
        if len(self.journalQuery) > 0:
            all_journal_dfs = []
            for handler in self.journalQuery:
                # print(handler)
                new_journal_df = handler.getJournalsWithTitle(partialTitle)
                # all_journal_dfs.append(new_journal_df) 

        return self.createJournalObject(new_journal_df)


    def getJournalsPublishedBy(self, partialName: str) -> List[Journal]:
        if len(self.journalQuery) > 0:
            all_journal_dfs = []
            for handler in self.journalQuery:
                new_journal_df = handler.getJournalsPublishedBy(partialName)
                # all_journal_dfs.append(new_journal_df) 

        return self.createJournalObject(new_journal_df)

    def getJournalsWithLicense(self, licenses: Set[str]) -> List[Journal]:
        if len(self.journalQuery) > 0:
            all_journal_dfs = []
            for handler in self.journalQuery:
                new_journal_df = handler.getJournalsWithLicense(licenses)
                # all_journal_dfs.append(new_journsal_df) 

        return self.createJournalObject(new_journal_df)

    def getJournalsWithAPC(self) -> List[Journal]:
        if len(self.journalQuery) > 0:
            all_journal_dfs = []
            for handler in self.journalQuery:
                new_journal_df = handler.getJournalsWithAPC()
                # all_journal_dfs.append(new_journal_df) 

        return self.createJournalObject(new_journal_df)

    def getJournalsWithDOAJSeal(self) -> List[Journal]:
        if len(self.journalQuery) > 0:
            all_journal_dfs = []
            for handler in self.journalQuery:
                new_journal_df = handler.getJournalsWithDOAJSeal()
                # all_journal_dfs.append(new_journal_df) 

        return self.createJournalObject(new_journal_df)
    
    def getAllCategories(self) -> List[Category]:
        if len(self.categoryQuery) > 0:
            all_category_dfs = []
            for handler in self.categoryQuery:
                new_category_df = handler.getAllCategories()
                # all_category_dfs.append(new_category_df)
            
        return self.createCategoryObject(new_category_df)

    
    def getAllAreas(self) -> List[Area]:
        if len(self.categoryQuery) > 0:
            all_area_dfs = []
            for handler in self.categoryQuery:
                new_area_df = handler.getAllAreas()
                # all_area_dfs.append(new_area_df)
            
        return self.createAreaObject(new_area_df)
        
    
    def getCategoriesWithQuartile(self, quartiles=None) -> List[Category]:
        if len(self.categoryQuery) > 0:
            all_category_dfs = []
            for handler in self.categoryQuery:
                new_category_df = handler.getCategoriesWithQuartile(quartiles)
                # all_category_dfs.append(new_category_df)
            
        return self.createCategoryObject(new_category_df)   
    
    def getCategoriesAssignedToAreas(self, area_ids=None) -> List[Category]:
        if len(self.categoryQuery) > 0:
            all_category_dfs = []
            for handler in self.categoryQuery:
                new_category_df = handler.getCategoriesAssignedToAreas(area_ids)
                # all_category_dfs.append(new_category_df)
            
        return self.createCategoryObject(new_category_df)
        

    def getAreasAssignedToCategories(self, category_ids=None) -> List[Area]:
        if len(self.categoryQuery) > 0:
            all_area_dfs = []
            for handler in self.categoryQuery:
                new_area_df = handler.getAreasAssignedToCategories(category_ids)
                # all_area_dfs.append(new_area_df)
            
        return self.createAreaObject(new_area_df)
        
# ------------------------------------------------------------------------------------------------------
# Full Query Engine -

class FullQueryEngine(BasicQueryEngine):
    def getJournalsInCategoriesWithQuartile(self, category_id=Set[str], category_quartile=Set[str]) -> List[Journal]:
        def safe_string_to_list(s):
            if isinstance(s, str):
                s = s.strip()
                if s.startswith('[') and s.endswith(']'):
                    s = s[1:-1]
                    items = re.split(r',\s*(?=[^\]"]*(?:\"[^\]\"]*\"[^\]"]*)*[^\]"]*$)', s)
                    items = [item.strip().strip('"').strip("'") for item in items]
                    return items
                else:
                    return [s]
            return s

        # print('len: ', len(category_id))
        if len(category_id) == 0:
            for handler in self.categoryQuery:
                cat_id_df = handler.getAllCategories()
            # print(cat_id_df)
            category_id = cat_id_df['category_id'].unique().tolist()
        if len(category_quartile) == 0:
            for handler in self.categoryQuery:
                cat_id_df = handler.getAllCategories()
                # print(cat_id_df)
            category_quartile = cat_id_df['category_quartile'].unique().tolist()            
        
        category_id_quartile_list = []
        for item in category_id:
            for quart in category_quartile:
                category_id_quartile_list.append((item, quart))
        # print(category_id_quartile_list)
        for handler in self.categoryQuery:
            with connect(handler.dbPathOrUrl) as con:
                placeholders = ', '.join(['(?, ?)'] * len(category_id_quartile_list))
                query = f"""
                    SELECT identifiers
                    FROM info
                    WHERE (category_id, category_quartile) IN ({placeholders});
                """
                params = [item for pair in category_id_quartile_list for item in pair]
                df = pd.read_sql_query(query, con, params=params)
        df = df.drop_duplicates(subset="identifiers", keep='first', inplace=False) # HERE I GET THE DF WITH IDENTIFIERS OF INTEREST
            
        if len(self.journalQuery) > 0:
            all_journal_dfs = []
            for handler in self.journalQuery:
                new_journal_df = handler.getAllJournals()
                all_journal_dfs.append(new_journal_df) # HERE I GET THE DF WITH ALL JOURNALS 
                
            if all_journal_dfs:
                all_journals_df = pd.concat(all_journal_dfs, ignore_index=True)

                # Converti le stringhe delle liste nella colonna 'identifier' in vere liste
                all_journals_df['identifiers'] = all_journals_df['identifiers'].apply(safe_string_to_list)
                # Esplodi la colonna 'identifier' per avere un identifier per riga
                all_journals_exploded_df = all_journals_df.explode('identifiers')

                # Estrai l'identifier stringa (gestendo anche il caso in cui non sia una tupla)
                all_journals_exploded_df['merged_identifier'] = all_journals_exploded_df['identifiers'].apply(lambda x: x[0] if isinstance(x, tuple) else x)

                # Assicurati che 'merged_identifier' sia di tipo stringa
                all_journals_exploded_df['merged_identifier'] = all_journals_exploded_df['merged_identifier'].astype(str)

                # Unisci i due DataFrames
                merged_df = pd.merge(df, all_journals_exploded_df, left_on='identifiers', right_on='merged_identifier', how='inner')
                # print(merged_df.info())
                # Rimuovi le colonne ausiliarie e rinomina
                merged_df = merged_df.drop(columns=['merged_identifier', 'identifiers_y'])
                merged_df = merged_df.rename(columns={'identifiers_x': 'identifiers'})

                # Rimuovi i duplicati basandosi sulla colonna 'identifier'
                final_df = merged_df.drop_duplicates(subset='identifiers')
                df_agg = final_df.groupby('journal')['identifiers'].apply(list).reset_index()

                # Unisci il DataFrame aggregato con le altre colonne del DataFrame originale (prendendo la prima occorrenza per le altre colonne)
                df_merged = pd.merge(df_agg, final_df.drop(columns=['identifiers']).groupby('journal').first().reset_index(), on='journal', how='left')


                return self.createJournalObject(df_merged)
    
    def getJournalsInAreasWithLicense(self, area=Set[str], license=Set[str]) -> List[Journal]:
        def safe_string_to_list(s):
            if isinstance(s, str):
                s = s.strip()
                if s.startswith('[') and s.endswith(']'):
                    s = s[1:-1]
                    items = re.split(r',\s*(?=[^\]"]*(?:\"[^\]\"]*\"[^\]"]*)*[^\]"]*$)', s)
                    items = [item.strip().strip('"').strip("'") for item in items]
                    return items
                else:
                    return [s]
            return s
        
        if len(area) == 0:
            for handler in self.categoryQuery:
                area_df = handler.getAllAreas()
            area = area_df['area'].unique().tolist()
        
        # get journals with at least of of the areas in input
        for handler in self.categoryQuery:
            with connect(handler.dbPathOrUrl) as con:
                placeholders = ', '.join(['?'] * len(area))
                query = f"""
                    SELECT identifiers
                    FROM info
                    WHERE area IN ({placeholders});
                """
                params = list(area)
                df = pd.read_sql_query(query, con, params=params)

        journal_with_area_df = df.drop_duplicates(subset="identifiers", keep='first', inplace=False) # HERE I GET THE DF WITH IDENTIFIERS OF INTEREST
            
        # get journals with licenses
        if len(self.journalQuery) > 0:
            journal_with_licenses_dfs = []
            for handler in self.journalQuery:
                # print(len(license))
                new_journal_with_licenses_df = handler.getJournalsWithLicense(license)
                if new_journal_with_licenses_df.empty:
                    print('trovato df vuoto')
                    new_journal_with_licenses_df = handler.getAllJournals()
                journal_with_licenses_dfs.append(new_journal_with_licenses_df)
            if journal_with_licenses_dfs:
                journal_with_licenses_df = pd.concat(journal_with_licenses_dfs, ignore_index=True)
                # print(journal_with_licenses_df.info())
                # Converti le stringhe delle liste nella colonna 'identifier' in vere liste
                journal_with_licenses_df['identifiers'] = journal_with_licenses_df['identifiers'].apply(safe_string_to_list)


                # Esplodi la colonna 'identifier' per avere un identifier per riga
                journal_with_licenses_df_exploded = journal_with_licenses_df.explode('identifiers')

                # Estrai l'identifier stringa (gestendo anche il caso in cui non sia una tupla)
                journal_with_licenses_df_exploded['merged_identifier'] = journal_with_licenses_df_exploded['identifiers'].apply(lambda x: x[0] if isinstance(x, tuple) else x)

                # Assicurati che 'merged_identifier' sia di tipo stringa
                journal_with_licenses_df_exploded['merged_identifier'] = journal_with_licenses_df_exploded['merged_identifier'].astype(str)

                # Unisci i due DataFrames
                merged_df = pd.merge(journal_with_area_df, journal_with_licenses_df_exploded, left_on='identifiers', right_on='merged_identifier', how='inner')

                # Rimuovi le colonne ausiliarie e rinomina
                merged_df = merged_df.drop(columns=['merged_identifier', 'identifiers_y'])
                merged_df = merged_df.rename(columns={'identifiers_x': 'identifiers'})

                # Rimuovi i duplicati basandosi sulla colonna 'identifier'
                final_df = merged_df.drop_duplicates(subset='identifiers')
                df_agg = final_df.groupby('journal')['identifiers'].apply(list).reset_index()

                # Unisci il DataFrame aggregato con le altre colonne del DataFrame originale (prendendo la prima occorrenza per le altre colonne)
                df_merged = pd.merge(df_agg, final_df.drop(columns=['identifiers']).groupby('journal').first().reset_index(), on='journal', how='left')


                return self.createJournalObject(df_merged)

    
    def getDiamondJournalsInAreasAndCategoriesWithQuartile(self, area=Set[str], category_id=Set[str], category_quartile=Set[str]) -> List[Journal]:
        def safe_string_to_list(s):
            if isinstance(s, str):
                s = s.strip()
                if s.startswith('[') and s.endswith(']'):
                    s = s[1:-1]
                    items = re.split(r',\s*(?=[^\]"]*(?:\"[^\]\"]*\"[^\]"]*)*[^\]"]*$)', s)
                    items = [item.strip().strip('"').strip("'") for item in items]
                    return items
                else:
                    return [s]
            return s
        if len(area) == 0:
            for handler in self.categoryQuery:
                area_df = handler.getAllAreas()
            area = area_df['area'].unique().tolist()
        if len(category_id) == 0:
            for handler in self.categoryQuery:
                cat_id_df = handler.getAllCategories()
            category_id = cat_id_df['category_id'].unique().tolist()
        if len(category_quartile) == 0:
            for handler in self.categoryQuery:
                cat_id_df = handler.getAllCategories()
            category_quartile = cat_id_df['category_quartile'].unique().tolist()
        
        attribute_combination_list = []
        for cat in category_id:
            for quart in category_quartile:
                for ar in area:
                    attribute_combination_list.append((cat, quart, ar))

        for handler in self.categoryQuery:
            with connect(handler.dbPathOrUrl) as con:
                placeholders = ', '.join(['(?, ?, ?)'] * len(attribute_combination_list))
                query = f"""
                    SELECT identifiers
                    FROM info
                    WHERE (category_id, category_quartile, area) IN ({placeholders});
                """
                params = [item for pair in attribute_combination_list for item in pair]
                df = pd.read_sql_query(query, con, params=params)
        df = df.drop_duplicates(subset="identifiers", keep='first', inplace=False) # HERE I GET THE DF WITH IDENTIFIERS OF INTEREST
        
        if len(self.journalQuery) > 0:
            all_journal_dfs = []
            for handler in self.journalQuery:
                new_journal_df = handler.getAllJournals()
                all_journal_dfs.append(new_journal_df) # HERE I GET THE DF WITH ALL JOURNALS 
            # print(all_journal_dfs)   
            if all_journal_dfs:
                all_journals_df = pd.concat(all_journal_dfs, ignore_index=True)

                # Converti le stringhe delle liste nella colonna 'identifier' in vere liste
                all_journals_df['identifiers'] = all_journals_df['identifiers'].apply(safe_string_to_list)

                # Esplodi la colonna 'identifier' per avere un identifier per riga
                all_journals_exploded_df = all_journals_df.explode('identifiers')

                # Estrai l'identifier stringa (gestendo anche il caso in cui non sia una tupla)
                all_journals_exploded_df['merged_identifier'] = all_journals_exploded_df['identifiers'].apply(lambda x: x[0] if isinstance(x, tuple) else x)

                # Assicurati che 'merged_identifier' sia di tipo stringa
                all_journals_exploded_df['merged_identifier'] = all_journals_exploded_df['merged_identifier'].astype(str)

                # Unisci i due DataFrames
                merged_df = pd.merge(df, all_journals_exploded_df, left_on='identifiers', right_on='merged_identifier', how='inner')
                print(merged_df.columns)
                # Rimuovi le colonne ausiliarie e rinomina
                merged_df = merged_df.drop(columns=['merged_identifier', 'identifiers_y'])
                merged_df = merged_df.rename(columns={'identifiers_x': 'identifiers'})

                # Rimuovi i duplicati basandosi sulla colonna 'identifier'
                final_df = merged_df.drop_duplicates(subset='identifiers')
                df_agg = final_df.groupby('journal')['identifiers'].apply(list).reset_index()

                # Unisci il DataFrame aggregato con le altre colonne del DataFrame originale (prendendo la prima occorrenza per le altre colonne)
                df_merged = pd.merge(df_agg, final_df.drop(columns=['identifiers']).groupby('journal').first().reset_index(), on='journal', how='left')

                df_filtered = df_merged[df_merged['apc'].isin(['No', False])]

                return self.createJournalObject(df_filtered) #, len(self.createJournalObject(df_filtered))

