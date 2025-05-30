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

# ------------------------------------------------------------------------------------------------------
# Python Objects - Edoardo AM Tarpinelli

class IdentifiableEntity(object):
    def __init__(self, identifiers):
        self.id = set()
        for item in identifiers:
            self.id.add(item) # string[1..*]
    
    def getIds(self) -> List[str]:
        return list(self.id) # Convert set to list and sort 

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

    # creating the class 
    def __init__(self):
        self.dbPathOrUrl = ""

    # creating the methods 
    def getDbPathOrUrl(self): 
        return self.dbPathOrUrl 

    def setDbPathOrUrl(self, pathOrUrl : str) -> bool: #: boolean 
        self.dbPathOrUrl = pathOrUrl
        return self.dbPathOrUrl == pathOrUrl


class UploadHandler(Handler):

    def __init__(self):
        super().__init__()

    def pushDataToDb(self):
        pass # never accessed here and overriden in child classes

#first case: the path is of the relational database the json file

class CategoryUploadHandler(UploadHandler): 

    def __init__(self):
        super().__init__()
    
    def pushDataToDb(self, path: str) -> bool: 
        
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

                #iterating through the identifiers inside  the bigger loop of items
                for idx, row in enumerate(identifiers): #i use the iteration because there are more than one in some cases 

                    identifier_list.append({
                            "item_internal_id": item_internal_id,
                            "identifiers": row #which is the single identifier 
                            }) 

                #2. creating internal ids for the categories, this is trickier because they have more than one value and they can have same id
                #i have to iterate through everything but check if the "id" is the same, so it's useful to use a dictionary 
                categories = item.get("categories", []) #especially for category, quartile and area, that in the UML are noted as optional ([0...*]) it's better to do it this way 

                for row in categories: 
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

            # create hasCategory table - Edoardo
            hasCategory = pd.merge(identifiers_df, cat_df, left_on='item_internal_id', right_on='item_internal_id')

            # create hasArea table - Edoardo
            hasArea = pd.merge(identifiers_df, ar_df, left_on='item_internal_id', right_on='item_internal_id')
        try:
            with connect(self.dbPathOrUrl) as con:
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
    def __init__(self):
        self.dbPathOrUrl = ""

    def pushDataToDb(self, path) -> bool:  
        my_graph = Graph() #creating the database

        #classes
        Journal = URIRef("https://schema.org/Periodical") 

        #attributes related to classes 
        id = URIRef("https://schema.org/identifier")
        title = URIRef("https://schema.org/title")
        languages = URIRef("https://schema.org/inLanguage") 
        publisher = URIRef("https://schema.org/publisher")
        doajSeal = URIRef("https://schema.org/Certification") 
        licence = URIRef("https://schema.org/license")
        apc = URIRef("https://schema.org/isAccessibleForFree")
        
        journals = pd.read_csv(path, 
                            keep_default_na=False, 
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
                my_graph.add((subj, apc, Literal(row["APC"]))) 

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
        
    def getById(self, id: str):
        query = """
        SELECT DISTINCT area AS identity, NULL AS category_quartile
        FROM info
        WHERE area = :id

        UNION ALL

        SELECT DISTINCT category_id AS identity, category_quartile
        FROM info
        WHERE category_id = :id;
        """
        df = self._execute_query(query, params={'id': id})
        if df.empty:
             return pd.DataFrame()
        # if df['category_quartile'].isna():
        #     df.drop('category_quartile', axis=1)
        if 'category_quartile' in df.columns and pd.isna(df['category_quartile']).all():
            df = df.drop(columns=['category_quartile'])
        return df # return empty dataframe as no IdentifiableEntity in relational db

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
        
        if isinstance(result, bytes):
            try:
                # Decodifica i bytes in una stringa usando UTF-8 (la più comune)
                result_str = result.decode('utf-8')
                # Parsa la stringa JSON in un dizionario Python
                result = json.loads(result_str)
            except json.JSONDecodeError as jde:
                print(f"Errore JSONDecodeError: Impossibile parsare la stringa come JSON. Dettagli: {jde}")
                print(f"Stringa che ha causato l'errore: {result_str[:200]}...") # Stampa i primi 200 caratteri per debug
                return pd.DataFrame() # Restituisce un DataFrame vuoto in caso di errore
            except UnicodeDecodeError as ude:
                print(f"Errore UnicodeDecodeError: Impossibile decodificare i byte con UTF-8. Dettagli: {ude}")
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
                        journals_data[journal_uri][var] = row.get(var, {}).get("value", "") if var not in ["identifiers", "languages"] else []

                for var in result["head"]["vars"]:
                    value = row.get(var, {}).get("value")
                    if var == "identifiers" and value not in journals_data[journal_uri]["identifiers"]:
                        journals_data[journal_uri]["identifiers"].append(value)
                    elif var == "languages" and value not in journals_data[journal_uri]["languages"]:
                        journals_data[journal_uri]["languages"].append(value)
                    elif var not in ["journal", "identifiers", "languages"]:
                        journals_data[journal_uri][var] = value

        # convert the dictionary in a list of dictionaries to create the Dataframe
            list_data = []
            for uri, data in journals_data.items():
                list_data.append(data)

            df = pd.DataFrame(list_data)
            return df.replace(np.nan, "")
        else:
            return pd.DataFrame()

    def getById(self, identifier):
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
            FILTER(CONTAINS(LCASE(?identifiers), "{identifier}"))
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

    def __init__(self):
        self.journalQuery = [] # [0..*] - graph
        self.categoryQuery = [] # [0..*] - rdb

    def getCategoryQuartile_mapped(self, all_identifiers):
        """
        it returns a dictionary with {'journal_identifier': list['associated_category_quartile']} for each journal in the input list 
        """
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
                        if category_id not in identifier_to_category_quartiles[identifier]:
                            identifier_to_category_quartiles[identifier].append(category_id)
        return identifier_to_category_quartiles

    def gethasArea_mapped(self, all_identifiers):
        """
        it returns a dictionary with {'journal_identifier': list['associated_area']} for each journal in the input list 
        """
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
                    # print(df)
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

        for index, row in input_dataframe.iterrows(): 
            # check whether a journal has one or more identifiers to be used in hasArea/hasCategory methods
            identifiers = row['identifiers']
            if isinstance(identifiers, list):
                all_journal_identifiers_set.update(identifiers)
            elif isinstance(identifiers, str):
                all_journal_identifiers_set.add(identifiers)

        all_journal_identifiers = list(all_journal_identifiers_set)

        # get hasCategory and hasArea for each journal in a dictionary ('journal identifier': list['has...'])
        identifier_to_areas = self.gethasArea_mapped(all_journal_identifiers)
        identifier_to_categories = self.getCategoryQuartile_mapped(all_journal_identifiers)

        for row in input_dataframe.itertuples(index=False):
            
            first_identifier = row.identifiers[0] if isinstance(row.identifiers, list) and len(row.identifiers) > 0 else row.identifiers if isinstance(row.identifiers, str) else None
            has_area = identifier_to_areas.get(first_identifier, [])
            has_category = identifier_to_categories.get(first_identifier, [])
            # print(first_identifier, "\n", 
            #     row.title, "\n", 
            #     list(row.languages) if isinstance(row.languages, list) and len(row.languages) > 0 else [], "\n", 
            #     row.publisher if pd.notna(row.publisher) else None, "\n", 
            #     True if pd.notna(row.seal) and str(row.seal.lower()).lower() == 'yes' else False, "\n", 
            #     row.license if pd.notna(row.license) else None, "\n", 
            #     True if pd.notna(row.apc) and str(row.apc.lower()).lower() == 'yes' else False, "\n", 
                # has_category, # Implementa la logica di recupero efficiente se necessario
                # has_area)
            journal = Journal(
                id=row.identifiers,
                title=row.title,
                languages=list(row.languages) if isinstance(row.languages, list) and len(row.languages) > 0 else [],
                publisher=row.publisher if pd.notna(row.publisher) else None,
                seal=True if pd.notna(row.seal) and str(row.seal.lower()).lower() == 'yes' else False,
                license=row.license if pd.notna(row.license) else None,
                apc=True if pd.notna(row.apc) and str(row.apc.lower()).lower() == 'yes' else False,
                hasCategory=has_category, # Implementa la logica di recupero efficiente se necessario
                hasArea=has_area
            )
            journal_list.append(journal)

        return journal_list

    def createCategoryObject(self, input_dataframe):
        category_list = list()
        input_dataframe = input_dataframe.drop_duplicates(subset=['category_id'], keep='first')
        # print(len(input_dataframe['category_id']))
        id_list = list()
        for index, row in input_dataframe.iterrows():
            # print(row['category_id'], "\n",
                # row['category_quartile'])
            id_list.append(row['category_id'])
            category = Category(
                id=id_list,
                quartile=row['category_quartile']
            )
            category_list.append(category)
        return category_list

    def createAreaObject(self, input_dataframe):
        area_list = list()
        id_list = list()
        for index, row in input_dataframe.iterrows():
            # print(row['area'])
            id_list.append(row['area'])
            area = Area(
                id=id_list,
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
        self.categoryQuery.append(CategoryHandler) # Boolean
        return True
    
    def getEntityById(self, input_identifier: str) -> IdentifiableEntity:
        
        if len(self.journalQuery) > 0:
            for handler in self.journalQuery:
                journal_df = handler.getById(input_identifier)
        
        if len(self.categoryQuery) > 0:
            for handler in self.categoryQuery:
                cat_area_df = handler.getById(input_identifier)
        
        if journal_df.empty and cat_area_df.empty:
            return None

        if journal_df.empty == False and cat_area_df.empty:
            return Journal
        
        if journal_df.empty and cat_area_df.empty == False:
            if len(cat_area_df.columns) == 2:
            
                cat_area_df = cat_area_df.rename(columns={'identity': 'category_id'})
                return Category
            else:
                cat_area_df = cat_area_df.rename(columns={'identity': 'area'})
                return Area

    def getAllJournals(self) -> List[Journal]:
        
        if len(self.journalQuery) > 0:
            for handler in self.journalQuery:
                new_journal_df = handler.getAllJournals()
                 

        return self.createJournalObject(new_journal_df)
    
    def getJournalsWithTitle(self, partialTitle: str) -> List[Journal]:
        if len(self.journalQuery) > 0:
            for handler in self.journalQuery:
                # print(handler)
                new_journal_df = handler.getJournalsWithTitle(partialTitle)

        return self.createJournalObject(new_journal_df)


    def getJournalsPublishedBy(self, partialName: str) -> List[Journal]:
        if len(self.journalQuery) > 0:
            for handler in self.journalQuery:
                new_journal_df = handler.getJournalsPublishedBy(partialName)

        return self.createJournalObject(new_journal_df)

    def getJournalsWithLicense(self, licenses: Set[str]) -> List[Journal]:
        if len(self.journalQuery) > 0:
            for handler in self.journalQuery:
                new_journal_df = handler.getJournalsWithLicense(licenses)

        return self.createJournalObject(new_journal_df)

    def getJournalsWithAPC(self) -> List[Journal]:
        if len(self.journalQuery) > 0:
            for handler in self.journalQuery:
                new_journal_df = handler.getJournalsWithAPC()

        return self.createJournalObject(new_journal_df)

    def getJournalsWithDOAJSeal(self) -> List[Journal]:
        if len(self.journalQuery) > 0:
            for handler in self.journalQuery:
                new_journal_df = handler.getJournalsWithDOAJSeal()

        return self.createJournalObject(new_journal_df)
    
    def getAllCategories(self) -> List[Category]:
        if len(self.categoryQuery) > 0:
            for handler in self.categoryQuery:
                new_category_df = handler.getAllCategories()
            
        return self.createCategoryObject(new_category_df)

    
    def getAllAreas(self) -> List[Area]:
        if len(self.categoryQuery) > 0:
            for handler in self.categoryQuery:
                new_area_df = handler.getAllAreas()
            
        return self.createAreaObject(new_area_df)
        
    
    def getCategoriesWithQuartile(self, quartiles=None) -> List[Category]:
        if len(self.categoryQuery) > 0:
            for handler in self.categoryQuery:
                new_category_df = handler.getCategoriesWithQuartile(quartiles)
            
        return self.createCategoryObject(new_category_df)   
    
    def getCategoriesAssignedToAreas(self, area_ids=None) -> List[Category]:
        if len(self.categoryQuery) > 0:
            for handler in self.categoryQuery:
                new_category_df = handler.getCategoriesAssignedToAreas(area_ids)
            
        return self.createCategoryObject(new_category_df)
        

    def getAreasAssignedToCategories(self, category_ids=None) -> List[Area]:
        if len(self.categoryQuery) > 0:
            for handler in self.categoryQuery:
                new_area_df = handler.getAreasAssignedToCategories(category_ids)
            
        return self.createAreaObject(new_area_df)
        
# ------------------------------------------------------------------------------------------------------
# Full Query Engine - Edoardo AM Tarpinelli

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

        if len(category_id) == 0:
            for handler in self.categoryQuery:
                cat_id_df = handler.getAllCategories()
            category_id = cat_id_df['category_id'].unique().tolist()

        if len(category_quartile) == 0:
            for handler in self.categoryQuery:
                cat_id_df = handler.getAllCategories()
            category_quartile = cat_id_df['category_quartile'].unique().tolist()            
        
        category_id_quartile_list = []

        for item in category_id:
            for quart in category_quartile:
                category_id_quartile_list.append((item, quart))

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
                    # print('trovato df vuoto')
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

                # Rimuovi le colonne ausiliarie e rinomina
                merged_df = merged_df.drop(columns=['merged_identifier', 'identifiers_y'])
                merged_df = merged_df.rename(columns={'identifiers_x': 'identifiers'})

                # Rimuovi i duplicati basandosi sulla colonna 'identifier'
                final_df = merged_df.drop_duplicates(subset='identifiers')
                df_agg = final_df.groupby('journal')['identifiers'].apply(list).reset_index()

                # Unisci il DataFrame aggregato con le altre colonne del DataFrame originale (prendendo la prima occorrenza per le altre colonne)
                df_merged = pd.merge(df_agg, final_df.drop(columns=['identifiers']).groupby('journal').first().reset_index(), on='journal', how='left')

                df_filtered = df_merged[df_merged['apc'].isin(['No', False])]

                return self.createJournalObject(df_filtered) 

