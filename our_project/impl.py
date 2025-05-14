import os
import json
import numpy as np
import pandas as pd
import time
from sqlite3 import connect
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

class JournalUploadHandler(UploadHandler): 
    # TODO:
    def __init__(self):
        self.dbPathOrUrl = ""
    
    def check_if_journal_exists(self, graph, issn):
        """Controlla se un giornale con un dato ISSN esiste già nel grafo."""
        query = f"""
            ASK {{
                ?journal <https://schema.org/identifier> "{issn}" .
            }}
        """
        graph.setQuery(query)
        graph.setReturnFormat(JSON)  # È buona pratica impostare il formato di ritorno
        try:
            results = graph.query().convert()
            return bool(results["boolean"])  # I risultati di ASK hanno un campo "boolean"
        except Exception as e:
            print(f"Errore durante la query ASK: {e}")
        return False

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
        journals = pd.read_csv(path, keep_default_na=False, dtype={
            "Journal title": "string",
            "Journal ISSN (print version)": "string",
            "Journal EISSN (online version)": "string",
            "Languages in which the journal accepts manuscripts": "string",
            "Publisher": "string",
            "DOAJ Seal": "string",
            "Journal license" : "string",
            "APC": "string"
        })

        base_url = "https://comp-data.github.io/res/"

        store = SPARQLWrapper(self.dbPathOrUrl) # Use SPARQLWrapper for querying

        try:
            store.setReturnFormat(JSON)
            for idx, row in journals.iterrows():
                local_id = "journal-" + str(idx)
                subj = URIRef(base_url + local_id)

                issn_print = str(row["Journal ISSN (print version)"])
                issn_online = str(row["Journal EISSN (online version)"])

                exists = False
                if issn_print and self.check_if_journal_exists(store, issn_print):
                    exists = True
                elif issn_online and self.check_if_journal_exists(store, issn_online):
                    exists = True

                if not exists:
                    my_graph.add((subj, RDF.type, Journal))
                    if row["Journal title"]:
                        my_graph.add((subj, title, Literal(row["Journal title"])))
                    if issn_print:
                        my_graph.add((subj, id, Literal(issn_print)))
                    if issn_online:
                        # Potresti voler gestire il caso in cui ci sono due ISSN diversi
                        my_graph.add((subj, id, Literal(issn_online)))
                    if row["Languages in which the journal accepts manuscripts"]:
                        language_string = row["Languages in which the journal accepts manuscripts"]
                        language_list = language_string.split(",")
                        for language in language_list:
                            language = language.strip()
                            my_graph.add((subj, languages, Literal(language)))
                    if row["Publisher"]:
                        my_graph.add((subj, publisher, Literal(row["Publisher"])))
                    if row["DOAJ Seal"]:
                        my_graph.add((subj, doajSeal, Literal(row["DOAJ Seal"])))
                    if row["Journal license"]:
                        my_graph.add((subj, licence, Literal(row["Journal license"])))
                    if row["APC"]:
                        my_graph.add((subj, apc, Literal(row["APC"])))

            # Opening the connection to upload the graph
            update_store = SPARQLUpdateStore()
            update_store.open((self.dbPathOrUrl, self.dbPathOrUrl))
            for triple in my_graph.triples((None, None, None)):
                update_store.add(triple)
            update_store.close()
            return True
        except Exception as e:
            print ("Problems with the Blazegraph connection: ", e)
            return False
        finally:
            if 'update_store' in locals() and update_store.is_open():
                update_store.close() 
            return True 

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
    
    def executeQuery(self, sql_command):
        with connect(self.dbPathOrUrl) as connection:
                cursor = connection.cursor()
                cursor.execute(sql_command)
                df = pd.DataFrame(cursor.fetchall(), columns = [description[0] for description in cursor.description]);
        #df.columns = [description[0] for description in cursor.description]; # setting column names with list comprehension because sqlite lacks a normal reference to column names
        return df

    # Prendere tutte le categorie (distinte)
    def getAllCategories(self):
        """
        return all categories included in database with no repetition
        """
        with connect(self.dbPathOrUrl) as con:
            query = "SELECT DISTINCT category_id, category_quartile FROM info"
            df = pd.read_sql_query(query, con)
        return df

        # Prendere tutte le aree (distinte)
    def getAllAreas(self):
        """
        return all area included in database with no repetition
        """
        with connect(self.dbPathOrUrl) as con:
            query = "SELECT DISTINCT area FROM info"
            df = pd.read_sql_query(query, con)
        return df

    def getCategoriesWithQuartile(self, quartiles=None):
        """
        if quartiles is given it returns a df showing all the categories associated to that quartile
        if quartiles is not given, it returns a df with the categories associated with all the unique quartiles in column category_quartile
        """
        with connect(self.dbPathOrUrl) as con:
            if quartiles:
                # ... (la parte con i quartili specificati rimane invariata)
                quartile_list = list(quartiles)
                placeholders = ','.join('?' * len(quartile_list))
                query = f"SELECT DISTINCT category_id, category_quartile FROM info WHERE category_quartile IN ({placeholders})"
                df = pd.read_sql_query(query, con, params=quartile_list)
            else:
                # # Se quartiles non è specificato, troviamo le category_id che hanno tutti i quartili unici presenti nella colonna
                # subquery = "SELECT DISTINCT category_quartile FROM info"
                # all_unique_quartiles_df = pd.read_sql_query(subquery, con)
                # print('all_unique_quartiles_df: ', all_unique_quartiles_df)
                # print('to_list: ', all_unique_quartiles_df['category_quartile'].tolist())
                # all_unique_quartiles = all_unique_quartiles_df['category_quartile'].tolist()
                # num_unique_quartiles = len(all_unique_quartiles)

                # query = f"""
                #     SELECT category_id, category_quartile
                #     FROM info
                #     WHERE category_quartile IN ({','.join(['?'] * num_unique_quartiles)})
                #     GROUP BY category_id
                #     HAVING COUNT(DISTINCT category_quartile) = {num_unique_quartiles}
                # """
                # df = pd.read_sql_query(query, con, params=all_unique_quartiles)
                df = pd.DataFrame()
        return df

    def getCategoriesAssignedToAreas(self, areas=None):
        """
        if areas is given it returns a df showing all the categories associated to that areas
        if areas is not given, it returns a df with the categories associated with all the unique areas in column category_quartile
        """
        with connect(self.dbPathOrUrl) as con:
            if areas:
                # Converti l'insieme in una lista (o tupla) per l'uso con IN e i placeholder
                area_list = list(areas)
                placeholders = ', '.join('?' * len(area_list))
                query = f"SELECT DISTINCT category_id, category_quartile FROM info WHERE area IN ({placeholders})"
                df = pd.read_sql_query(query, con, params=area_list)
            else:
                # # Se quartiles non è specificato, troviamo le category_id che hanno tutti i quartili
                # subquery = "SELECT DISTINCT area FROM info"
                # all_areas_df = pd.read_sql_query(subquery, con)
                # all_areas = [a[0] for a in all_areas_df.values.tolist()]
                # num_all_areas = len(all_areas)

                # query = f"""
                #     SELECT category_id
                #     FROM info
                #     GROUP BY category_id
                #     HAVING COUNT(DISTINCT category_quartile) = {num_all_areas}
                # """
                # df = pd.read_sql_query(query, con)
                df = pd.DataFrame()
        return df

    def getAreasAssignedToCategories(self, categories=None):
        with connect(self.dbPathOrUrl) as con:
            if categories:
                # Converti l'insieme in una lista (o tupla) per l'uso con IN e i placeholder
                categories_list = list(categories)
                placeholders = ', '.join('?' * len(categories_list))
                query = f"SELECT DISTINCT area FROM info WHERE category_id IN ({placeholders})"
                df = pd.read_sql_query(query, con, params=categories_list)
            else:
                # # Se quartiles non è specificato, troviamo le category_id che hanno tutti i quartili
                # subquery = "SELECT DISTINCT area FROM info"
                # all_categories_df = pd.read_sql_query(subquery, con)
                # all_categories = [a[0] for a in all_categories_df.values.tolist()]
                # num_all_ctaegories = len(all_categories)

                # query = f"""
                #     SELECT category_id
                #     FROM info
                #     GROUP BY category_id
                #     HAVING COUNT(DISTINCT category_quartile) = {num_all_ctaegories}
                # """
                # df = pd.read_sql_query(query, con)
                df = pd.DataFrame()
        return df


# ------------------------------------------------------------------------------------------------------
# JournalQueryHandler - Faride

class JournalQueryHandler(QueryHandler):
    # TODO: we have to deal with repetition in all the methods
    def __init__(self):
        self.dbPathOrUrl = ""

    def execute_sparql_query(self, query):
        sparql = SPARQLWrapper(self.dbPathOrUrl)
        sparql.setReturnFormat(JSON)
        sparql.setQuery(query)
        try:
            result = sparql.queryAndConvert()
        except Exception as e:
            print("SPARQL Error:", e)
            return pd.DataFrame()

        if result and "head" in result and "vars" in result["head"] and "results" in result and "bindings" in result["results"]:
            journals_data = {}
            for row in result["results"]["bindings"]:
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
        SELECT DISTINCT ?journal ?title ?identifier ?languages ?publisher ?license ?apc ?seal
        WHERE {
          ?journal a <https://schema.org/Periodical> ;
                   <https://schema.org/title> ?title ;
                   <https://schema.org/identifier> ?identifier ;
                   <https://schema.org/inLanguage> ?languages ;
                   <https://schema.org/license> ?license .
          OPTIONAL { ?journal <https://schema.org/publisher> ?publisher }
          OPTIONAL { ?journal <https://schema.org/isAccessibleForFree> ?apc }
          OPTIONAL { ?journal <https://schema.org/Certification> ?seal }
        }
        """
        # <https://schema.org/issn> ?issn ;
        #            <https://schema.org/eissn> ?eissn ;
        return self.execute_sparql_query(query)   #if there was an error the issn schema can be altered
                                                  
    def getJournalsWithTitle(self, title: str):
        escaped_title = title.replace('"', '\\"')
        query = f"""
        SELECT DISTINCT ?journal ?title ?identifier ?languages ?publisher ?license ?apc ?seal
        WHERE {{
            ?journal a <https://schema.org/Periodical> ;
                    <https://schema.org/title> ?title ;
                    <https://schema.org/identifier> ?identifier ;
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
        SELECT DISTINCT ?journal ?title ?identifier ?languages ?publisher ?license ?apc ?seal
        WHERE {{
            ?journal a <https://schema.org/Periodical> ;
                    <https://schema.org/title> ?title ;
                    <https://schema.org/identifier> ?identifier ;
                    <https://schema.org/inLanguage> ?languages ;
                    <https://schema.org/license> ?license .
            
            OPTIONAL {{ ?journal <https://schema.org/publisher> ?publisher }}
            OPTIONAL {{ ?journal <https://schema.org/isAccessibleForFree> ?apc }}
            OPTIONAL {{ ?journal <https://schema.org/Certification> ?seal }}
            FILTER(CONTAINS(LCASE(?publisher), "{publisher.lower()}"))
        }}
        """
        return self.execute_sparql_query(query)

    def getJournalsWithLicense(self, license_str: str):
        license_str = license_str.replace('"', '\\"')
        query = f"""
        SELECT DISTINCT ?journal ?title ?identifier ?languages ?publisher ?license ?apc ?seal
        WHERE {{
            ?journal a <https://schema.org/Periodical> ;
                    <https://schema.org/title> ?title ;
                    <https://schema.org/identifier> ?identifier ;
                    <https://schema.org/inLanguage> ?languages ;
                    <https://schema.org/license> ?license .
            
            OPTIONAL {{ ?journal <https://schema.org/publisher> ?publisher }}
            OPTIONAL {{ ?journal <https://schema.org/isAccessibleForFree> ?apc }}
            OPTIONAL {{ ?journal <https://schema.org/Certification> ?seal }}
          FILTER(LCASE(?license) = "{license_str.lower()}")
        }}
        """
        return self.execute_sparql_query(query)

    def getJournalsWithAPC(self):
        query = """
        SELECT DISTINCT ?journal ?title ?identifier ?languages ?publisher ?license ?apc ?seal
        WHERE {{
            ?journal a <https://schema.org/Periodical> ;
                    <https://schema.org/title> ?title ;
                    <https://schema.org/identifier> ?identifier ;
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
        SELECT DISTINCT ?journal ?title ?identifier ?languages ?publisher ?license ?apc ?seal
        WHERE {{
            ?journal a <https://schema.org/Periodical> ;
                    <https://schema.org/title> ?title ;
                    <https://schema.org/identifier> ?identifier ;
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
        # self.identity_category_dict = dict()
        # self.identity_area_dict = dict()
    
    # def gethasCategory(self, identifier):
    #     if len(identifier) > 1:
    #         issn = identifier[0]
    #         eissn = identifier[1]
    #     elif len(identifier) == 1:
    #         issn = identifier[0]
    #         eissn = None
    #     if len(self.categoryQuery) > 0:
    #         conditions = []
    #         params = []
    #         hasCategory_list = []

    #         if issn:
    #             conditions.append("identifiers = ?")
    #             params.append(issn)
    #         if eissn:
    #             conditions.append("identifiers = ?")
    #             params.append(eissn)

    #         where_clause = " OR ".join(conditions)
    #         for handler in self.categoryQuery:
    #             with connect(handler.dbPathOrUrl) as con:
    #                 query = f"SELECT DISTINCT category_internal_id FROM hasCategory WHERE {where_clause}"
    #                 df = pd.read_sql_query(query, con, params=params)
    #         if df.empty:
    #             return []
    #         else:
    #             df = df.drop_duplicates(subset='category_internal_id', keep='first')
    #             hasCategory_list = df['category_internal_id'].to_list()
    #             # print('hasCategory: ', hasCategory_list)
    #             return hasCategory_list 
    #     else:
    #         return 'len(self.categoryQuery) < 0, meaning no database to query'
        
    # def gethasArea(self, identifier):
    #     if len(identifier) > 1:
    #         issn = identifier[0]
    #         eissn = identifier[1]
    #     elif len(identifier) == 1:
    #         issn = identifier[0]
    #         eissn = None
    #     if len(self.categoryQuery) > 0:
    #         conditions = []
    #         params = []
    #         hasArea_list = []

    #         if issn:
    #             conditions.append("identifiers = ?")
    #             params.append(issn)
    #         if eissn:
    #             conditions.append("identifiers = ?")
    #             params.append(eissn)

    #         where_clause = " OR ".join(conditions)
    #         for handler in self.categoryQuery:
    #             with connect(handler.dbPathOrUrl) as con:
    #                 query = f"SELECT DISTINCT area_internal_id FROM hasArea WHERE {where_clause}"
    #                 df = pd.read_sql_query(query, con, params=params)
    #         if df.empty:
    #             return []
    #         else:
    #             df = df.drop_duplicates(subset='area_internal_id', keep='first')
    #             hasArea_list = df['area_internal_id'].to_list()
    #             # print('hasArea: ', hasArea_list)
    #             return hasArea_list 
    #     else:
    #         return 'len(self.categoryQuery) < 0, meaning no database to query'

    # def createJournalObject(self, input_dataframe):
    #     # convert df into list of Python Objects
    #     journal_list = list()

    #     if input_dataframe:
    #         # concatenate all journal df in the list
    #         journal_df = pd.concat(input_dataframe, ignore_index=True)
    #         # aggregate based on values in 'journal' and compress 'languages' into a string (nome than 1 may be associated to the journal)
    #         # journal_df_grouped = journal_df.groupby('journal').agg(languages=('languages', lambda x: ', '.join(list(set(x)))),identifier=('identifier', lambda x: ', '.join(list(set(x))))).reset_index()
    #         # add columns different from 'journal' and 'languages' to the df on 'journal' and drop duplicates on 'journal'
    #         # journal_df_final = pd.merge(journal_df_grouped, journal_df[['journal'] + [col for col in journal_df.columns if col not in ['journal', 'identifier', 'languages']]].drop_duplicates(subset=['journal'], keep='first'), on='journal', how='left')
            
    #         # print(journal_df.info())                
    #     else:
    #         # if all_journal_dfs == False: return empty df
    #         journal_df = pd.DataFrame()
        
    #     for index, row in journal_df.iterrows():
    #         # print('identifier: ... ', row['identifier'])
    #         journal = Journal(
    #             id=row['journal'],  
    #             title=row['title'],
    #             languages=row['languages'] if len(row['languages']) > 0 else [],
    #             publisher=row['publisher'] if pd.notna(row['publisher']) else None,
    #             seal=row['seal'] if pd.notna(row['seal']) and str(row['seal']).lower() == 'yes' else False,
    #             license=row['license'] if pd.notna(row['license']) else None,
    #             apc=row['apc'] if pd.notna(row['apc']) and str(row['apc']).lower() == 'yes' else False,
    #             hasCategory = self.gethasCategory(row['identifier']),
    #             hasArea = self.gethasArea(row['identifier'])
    #         ) 
    #         journal_list.append(journal)

    #     return journal_list

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

        if input_dataframe:
            journal_df = pd.concat(input_dataframe, ignore_index=True)
        else:
            journal_df = pd.DataFrame()

        # Ottieni tutti gli identifier unici da processare
        all_journal_identifiers_set = set()
        for identifiers_list in journal_df['identifier']:
            if isinstance(identifiers_list, list):
                all_journal_identifiers_set.update(identifiers_list)
            elif isinstance(identifiers_list, str):
                all_journal_identifiers_set.add(identifiers_list)

        all_journal_identifiers = list(all_journal_identifiers_set)

        # Recupera la mappatura identifier -> aree
        identifier_to_areas = self.gethasArea_mapped(all_journal_identifiers)
        identifier_to_categories = self.getCategoryQuartile_mapped(all_journal_identifiers)
        counter = 0
        for row in journal_df.itertuples(index=False):
            # Gestisci il caso in cui 'identifier' sia una lista o una stringa
            first_identifier = row.identifier[0] if isinstance(row.identifier, list) and len(row.identifier) > 0 else row.identifier if isinstance(row.identifier, str) else None
            has_area = identifier_to_areas.get(first_identifier, [])
            has_category = identifier_to_categories.get(first_identifier, [])
            if counter < 10:
                print(row.journal,
                    row.title,
                    list(row.languages) if isinstance(row.languages, list) and len(row.languages) > 0 else [],
                    row.publisher if pd.notna(row.publisher) else None,
                    row.seal if pd.notna(row.seal) and str(row.seal).lower() == 'yes' else False,
                    row.license if pd.notna(row.license) else None,
                    row.apc if pd.notna(row.apc) and str(row.apc).lower() == 'yes' else False,
                    has_category,
                    has_area)
            counter += 1
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
        if input_dataframe:
            # concatenate all journal df in the list
            category_df = pd.concat(input_dataframe, ignore_index=True)
                            
        else:
            # if all_journal_dfs == False: return empty df
            category_df = pd.DataFrame()

        for index, row in category_df.iterrows():
            category = Category(
                id=row['category_id'],
                quartile=row['category_quartile']
            )
            category_list.append(category)
        return category_list

    def createAreaObject(self, input_dataframe):
        area_list = list()
        if input_dataframe:
            # concatenate all journal df in the list
            area_df = pd.concat(input_dataframe, ignore_index=True)
                            
        else:
            # if all_journal_dfs == False: return empty df
            area_df = pd.DataFrame()

        for index, row in area_df.iterrows():
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

        # ===============================================================================
        # # Helper code to get hasCategory and hasArea lists when class BasicQueryEngine is initialized

        # # create df with ALL identifiers (there will be repetition) and category_id
        # if len(self.categoryQuery) > 0:
        #     # print('len self.categoryQuery: ', len(self.categoryQuery))
        #     for handler in self.categoryQuery:
        #         with connect(handler.dbPathOrUrl) as con:
        #             query = """
        #             SELECT identifiers, category_id
        #             FROM info
        #             WHERE category_id IS NOT NULL AND category_id != '';
        #             """
        #         df = pd.read_sql_query(query, con)
        #         # print(df)
        # for index, row in df.iterrows():
        #         identifier = row['identifiers']
        #         # in the dictionary there will be no more repetition
        #         if identifier not in self.identity_category_dict:
        #             self.identity_category_dict[identifier] = set()
        #         self.identity_category_dict[identifier].add(row['category_id'])
    
        # # create df with ALL identifiers (there will be repetition) and area
        # if len(self.categoryQuery) > 0:
        #     for handler in self.categoryQuery:
        #         with connect(handler.dbPathOrUrl) as con:
        #             query = """
        #             SELECT identifiers, area
        #             FROM info
        #             WHERE area IS NOT NULL AND area != '';
        #             """
        #         df = pd.read_sql_query(query, con)
        # for index, row in df.iterrows():
        #         identifier = row['identifiers']
        #         # in the dictionary there will be no more repetition
        #         if identifier not in self.identity_area_dict:
        #             self.identity_area_dict[identifier] = set()
        #         self.identity_area_dict[identifier].add(row['area'])
        # return True
    
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
                all_journal_dfs.append(new_journal_df) 

        return self.createJournalObject(all_journal_dfs)
    
    def getJournalsWithTitle(self, partialTitle: str) -> List[Journal]:
        if len(self.journalQuery) > 0:
            all_journal_dfs = []
            for handler in self.journalQuery:
                # print(handler)
                new_journal_df = handler.getJournalsWithTitle(partialTitle)
                all_journal_dfs.append(new_journal_df) 

        return self.createJournalObject(all_journal_dfs)


    def getJournalsPublishedBy(self, partialName: str) -> List[Journal]:
        if len(self.journalQuery) > 0:
            all_journal_dfs = []
            for handler in self.journalQuery:
                new_journal_df = handler.getJournalsPublishedBy(partialName)
                all_journal_dfs.append(new_journal_df) 

        return self.createJournalObject(all_journal_dfs)

    def getJournalsWithLicense(self, licenses: Set[str]) -> List[Journal]:
        if len(self.journalQuery) > 0:
            all_journal_dfs = []
            for handler in self.journalQuery:
                new_journal_df = handler.getJournalsWithLicense(licenses)
                all_journal_dfs.append(new_journal_df) 

        return self.createJournalObject(all_journal_dfs)

    def getJournalsWithAPC(self) -> List[Journal]:
        if len(self.journalQuery) > 0:
            all_journal_dfs = []
            for handler in self.journalQuery:
                new_journal_df = handler.getJournalsWithAPC()
                all_journal_dfs.append(new_journal_df) 

        return self.createJournalObject(all_journal_dfs)

    def getJournalsWithDOAJSeal(self) -> List[Journal]:
        if len(self.journalQuery) > 0:
            all_journal_dfs = []
            for handler in self.journalQuery:
                new_journal_df = handler.getJournalsWithDOAJSeal()
                all_journal_dfs.append(new_journal_df) 

        return self.createJournalObject(all_journal_dfs)
    
    def getAllCategories(self) -> List[Category]:
        if len(self.categoryQuery) > 0:
            all_category_dfs = []
            for handler in self.categoryQuery:
                new_category_df = handler.getAllCategories()
                all_category_dfs.append(new_category_df)
            
        return self.createCategoryObject(all_category_dfs)

    
    def getAllAreas(self) -> List[Area]:
        if len(self.categoryQuery) > 0:
            all_area_dfs = []
            for handler in self.categoryQuery:
                new_area_df = handler.getAllAreas()
                all_area_dfs.append(new_area_df)
            
        return self.createAreaObject(all_area_dfs)
        
    
    def getCategoriesWithQuartile(self, quartiles=None) -> List[Category]:
        if len(self.categoryQuery) > 0:
            all_category_dfs = []
            for handler in self.categoryQuery:
                new_category_df = handler.getCategoriesWithQuartile(quartiles)
                all_category_dfs.append(new_category_df)
            
        return self.createCategoryObject(all_category_dfs)   
    
    def getCategoriesAssignedToAreas(self, area_ids=None) -> List[Category]:
        if len(self.categoryQuery) > 0:
            all_category_dfs = []
            for handler in self.categoryQuery:
                new_category_df = handler.getCategoriesAssignedToAreas(area_ids)
                all_category_dfs.append(new_category_df)
            
        return self.createCategoryObject(all_category_dfs)
        

    def getAreasAssignedToCategories(self, category_ids=None) -> List[Area]:
        if len(self.categoryQuery) > 0:
            all_area_dfs = []
            for handler in self.categoryQuery:
                new_area_df = handler.getAreasAssignedToCategories(category_ids)
                all_area_dfs.append(new_area_df)
            
        return self.createAreaObject(all_area_dfs)
        
# ------------------------------------------------------------------------------------------------------
# Full Query Engine -

class FullQueryEngine(BasicQueryEngine):
    def JournalsInCategoryWithQuartile(self, category_id=Set[str], category_quartile=Set[str]) -> List[Journal]:
        category_id_quartile_list = []
        for item in category_id:
            category_id_quartile_list.append((item, category_quartile))

        params = (category_id_quartile_list,)

        identifiers = []

        if len(self.categoryQuery) > 0:
            for handler in self.categoryQuery:
                with connect(handler.dbPathOrUrl) as con:
                    query = """
                        SELECT T1.internal_item_id, T1.identifiers
                        FROM info AS T1
                        INNER JOIN (
                            SELECT internal_item_id, MIN(ROWID) AS min_rowid
                            FROM info
                            GROUP BY internal_item_id
                        ) AS T2
                        ON T1.internal_item_id = T2.internal_item_id AND T1.ROWID = T2.min_rowid
                        WHERE (T1.category_id, T1.category_quartile) IN (?);
                    """
                    df = pd.read_sql_query(query, con, params=params)

                    if not df.empty:
                        df = df.drop_duplicates(subset='internal_item_id', keep='first') # Changed to internal_item_id
                        hasArea_list = df['internal_item_id'].to_list() # Changed to internal_item_id
                        identifiers.extend(hasArea_list)
        all_journal_dfs = []
        if len(self.journalQuery) > 0:
            for handler in self.journalQuery:
                if not identifiers:
                    return []

                # Costruisci la parte della query con i filtri sugli identifiers
                filter_clauses = " ".join(f'?journal <https://schema.org/identifier> "{identifier}" .' for identifier in identifiers)

                query = f"""
                    SELECT DISTINCT ?journal ?title ?identifier ?languages ?publisher ?license ?apc ?seal
                    WHERE {{
                        ?journal a <https://schema.org/Periodical> ;
                                <https://schema.org/title> ?title ;
                                <https://schema.org/identifier> ?identifier ;
                                <https://schema.org/inLanguage> ?languages ;
                                <https://schema.org/license> ?license .
                        {filter_clauses}
                        OPTIONAL {{ ?journal <https://schema.org/publisher> ?publisher }}
                        OPTIONAL {{ ?journal <https://schema.org/isAccessibleForFree> ?apc }}
                        OPTIONAL {{ ?journal <https://schema.org/Certification> ?seal }}
                    }}
                """
                new_journal_df = handler.execute_sparql_query(query)
                all_journal_dfs.append(new_journal_df) 

        return self.createJournalObject(all_journal_dfs)
    
    def JournalsInAreasWithLicense(self, area=Set[str], license=Set[str]) -> List[Journal]:
        pass
    def DiamondJournalsInAreasAndCategoriesWithQuartile(self, area=Set[str], category_id=Set[str], category_quartile=Set[str]) -> List[Journal]:
        pass
