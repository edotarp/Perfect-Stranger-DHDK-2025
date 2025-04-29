import os
import json
import numpy as np
import pandas as pd
from sqlite3 import connect
import SPARQLWrapper
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

    #creating the class 
    def __init__(self, dbPathOrUrl : str):
        self.dbPathOrUrl = dbPathOrUrl

    #creating the methods 
    def getDbPathOrUrl(self): 
        return self.dbPathOrUrl 

    def setDbPathOrUrl(self, pathOrUrl : str): #: boolean 
        self.dbPathOrUrl = pathOrUrl
        return True


class UploadHandler(Handler):

    def pushDataToDb(self, path: str):  #self implied 
        if path.lower().endswith(".csv"): 
            handler = JournalUploadHandler(self.dbPathOrUrl)
            return handler.journalUpload(path) #calling the method after I called the subclass
        elif path.lower().endswith(".json"): 
            handler = CategoryUploadHandler(self.dbPathOrUrl)
            return handler.categoryUpload(path)
        else: 
            print("Unsupported file. Please try with a .csv or .json") #handling edge case: the file type is not right
            return False 


#first case: the path is of the relational database the json file
import json 
from sqlite3 import connect
import pandas as pd
class CategoryUploadHandler(UploadHandler): 
    
    def categoryUpload(self, path: str): 
        
        try: 
            #creating the database 
            with open(path, "r", encoding="utf-8") as c: 
                json_data = json.load(c) #reading the file 

                identifier_list = []

                category_mapping_dict = {} #using it to keep track of what we have
                categories_list = []

                area_mapping_dict = {}
                area_list = []

                #internal identifier of all the items 
                for idx, item in enumerate(json_data): 
                    item_internal_id = ("item-" + str(idx))
                
                    #1. creating internal ids for each element: identifiers 
                    identifiers = item["identifiers"] #selecting the identifiers  

                    #iterating through the identifiers indise the bigger loop of items
                    for idx, row in enumerate(identifiers): #i use the iteration because there are more than one in some cases 
                        identifiers_internal_id = ("internal_id-") +  str(idx)
                        identifier_list.append({
                                "item_internal_id": item_internal_id,
                                "identifier": identifiers_internal_id,
                                "identifiers": identifiers
                                })  #associating the data, with the internal id of the single category but also to the identifies of the whole item so that it's easier to query 

                    #2. creating internal ids for the categories, this is trickier because they have more than one value and they can have same id
                    #i have to iterate thourg everything but check if the "id" is the same, so it's useful to use a dictionary 
                    categories = item["categories"] 
                    #DUBBIO!!!!! should i add some kind of handler if there are present? like item.get("identifiers", [])

                    for idx, row in enumerate(categories): #appunto per me, scrivere cat_id = category["id"] non ha senso perchè category è una lista di un dizionario, io devo internere come dizionario il singolo item 
                        cat_id = row["id"]

                        if cat_id not in category_mapping_dict: #checking if the category is not already in the dictionary 
                            category_id_internal_id = ("category_id-") + str(idx)
                            category_mapping_dict[cat_id] = (category_id_internal_id)
                        else: 
                            category_id_internal_id = category_mapping_dict[cat_id] #if it's already inside the dict consider the original id 

                        categories_list.append({
                            "item_internal_id": item_internal_id,
                            "category_internal_id" : category_id_internal_id,
                            "id": cat_id,
                            "quartile": row["quartile"]
                        })
                
                    #3. creating internal ids for areas, this is the same but without any more value 
                    areas = item["areas"]

                    for idx, row in enumerate(areas): 
                        if row not in area_mapping_dict: 
                            area_id = (("areas-") + str(idx))
                            area_mapping_dict[row] = area_id
                        else: 
                            area_id = area_mapping_dict[row]
                    
                        area_list.append({
                            "item_internal_id": item_internal_id, 
                            "area_internal_id": area_id,
                            "area": row
                        })

                #converting the data in dataframes 
                
                identifiers_df = pd.DataFrame(identifier_list)
                categories_df = pd.DataFrame(categories_list)
                areas_df = pd.DataFrame(area_list)

        except Exception as e: #handling errors in the building dataframe phase 
            print(e)

        #adding them to the database 
        try: 
            with connect(self.dbPathOrUrl) as con:
                identifiers_df.to_sql("identifiers", con, if_exists="replace", index=False)
                categories_df.to_sql("categories", con, if_exists="replace", index=False)
                areas_df.to_sql("areas", con, if_exists="replace", index=False)
        except Exception as e: #handling errors in the pushing data phase
            print(e)

            
#second case: the path is the one of a graph database, the csv file
from rdflib import Graph, URIRef, Literal, RDF 
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore

class JournalUploadHandler(UploadHandler): 

    def journalUpload(self, path: str):  

        try:     
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
                #checking every category in the row (which is none other than a panda Series, so a list of vocabularies)
                if row["Journal title"]: 
                    my_graph.add((subj, title, Literal(row["Journal title"])))
                if row["Journal ISSN (print version)"]: 
                    my_graph.add((subj, id, Literal(row["Journal ISSN (print version)"])))
                    #NEED TO DECIDE IF WE WANT TO CONSIDER BOTH AS ID, OR TO SEPATATE THEM (https://schema.org/issn) 
                if row["Journal EISSN (online version)"]: 
                    my_graph.add((subj, id, Literal(row["Journal EISSN (online version)"])))
                if row["Languages in which the journal accepts manuscripts"]: 
                    my_graph.add((subj, languages, Literal(row["Languages in which the journal accepts manuscripts"])))
                if row["Publisher"]: 
                    my_graph.add((subj, publisher, Literal(row["Publisher"])))
                if row["DOAJ Seal"]: 
                    my_graph.add((subj, doajSeal, Literal(row["DOAJ Seal"])))
                if row["Journal license"]: 
                    my_graph.add((subj, licence, Literal(row["Journal license"])))
                if row["APC"]: 
                    my_graph.add((subj, apc, Literal(row["APC"])))

        except Exception as e: 
            print(e) #handling errors in the building the graph phase 

        try: 
            #opening the connection to upload the graph 
            store = SPARQLUpdateStore() #initializing it as an object 
            #endpoint =  self.dbPathOrUrl the endopoint is the url or path of the database 
            store.open(self.dbPathOrUrl, self.dbPathOrUrl)

            for triple in my_graph.triples(None, None, None): 
                store.add(triple)

            #closing the connection when we finish 
            store.close()

        except Exception as e: 
            print(e) #handling errors in the upload part 
    
# ------------------------------------------------------------------------------------------------------
# CategoryQueryHandler and QueryHandler - Cecilia Vesci

class CategoryQueryHandler(QueryHandler):
  def __init__(self, dbPathOrUrl=""):
        #super().__init__()
        self.dbPathOrUrl = dbPathOrUrl
        self.db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), self.dbPathOrUrl))
  
  def getById(self, id: str):
      return pd.DataFrame() # return empty dataframe as no IdentifiableEntity in relational db
   
  def executeQuery(self, sql_command):
       connection = connect(self.dbPathOrUrl)
       cursor = connection.cursor()
       cursor.execute(sql_command)
       df = pd.DataFrame(cursor.fetchall(), columns = [description[0] for description in cursor.description])
       #df.columns = [description[0] for description in cursor.description]; # setting column names with list comprehension because sqlite lacks a normal reference to column names
       connection.close()
       return df
        
# ------------------------------------------------------------------------------------------------------
# JournalQueryHandler - Faride

class JournalQueryHandler:
    def __init__(self, db_url):
        self.dbPathOrUrl = db_url

    def execute_sparql_query(self, query):
        sparql = SPARQLWrapper.SPARQLWrapper(self.dbPathOrUrl)
        sparql.setReturnFormat(SPARQLWrapper.JSON)
        sparql.setQuery(query)
        try:
            result = sparql.queryAndConvert()
        except Exception as e:
            print("SPARQL Error:", e)
            return pd.DataFrame()

        columns = result["head"]["vars"]
        df = pd.DataFrame(columns=columns)
        for row in result["results"]["bindings"]:
            row_data = {col: row[col]["value"] if col in row else "" for col in columns}
            df.loc[len(df)] = row_data
        return df.replace(np.nan, "")

    def getAllJournals(self):
        query = """
        SELECT DISTINCT ?journal ?title ?issn ?eissn ?languages ?publisher ?license ?apc ?seal
        WHERE {
          ?journal a <https://schema.org/Periodical> ;
                   <https://schema.org/title> ?title ;
                   <https://schema.org/identifier> ?issn ;
                   <https://schema.org/identifier> ?eissn ;
                   <https://schema.org/inLanguage> ?languages ;
                   <https://schema.org/license> ?license .
          OPTIONAL { ?journal schema:publisher ?publisher }
          OPTIONAL { ?journal schema:isAccessibleForFree ?apc }
          OPTIONAL { ?journal schema:Certification ?seal }
        }
        """
        return self.execute_sparql_query(query)   #if there was an error the issn schema can be altered
                                                  
    def getJournalsWithTitle(self, title: str):
        title = title.replace('"', '\\"')
        query = f"""
        SELECT DISTINCT ?journal ?title
        WHERE {{
          ?journal <https://schema.org/title> ?title .
          FILTER(CONTAINS(LCASE(?title), "{title.lower()}"))
        }}
        """
        return self.execute_sparql_query(query)

    def getJournalsPublishedBy(self, publisher: str):
        publisher = publisher.replace('"', '\\"')
        query = f"""
        SELECT DISTINCT ?journal ?publisher
        WHERE {{
          ?journal <https://schema.org/publisher> ?publisher .
          FILTER(CONTAINS(LCASE(?publisher), "{publisher.lower()}"))
        }}
        """
        return self.execute_sparql_query(query)

    def getJournalsWithLicense(self, license_str: str):
        license_str = license_str.replace('"', '\\"')
        query = f"""
        SELECT DISTINCT ?journal ?license
        WHERE {{
          ?journal <https://schema.org/license> ?license .
          FILTER(LCASE(?license) = "{license_str.lower()}")
        }}
        """
        return self.execute_sparql_query(query)

    def getJournalsWithAPC(self):
        query = """
        SELECT DISTINCT ?journal ?apc
        WHERE {
          ?journal <https://schema.org/isAccessibleForFree> ?apc .
          FILTER(LCASE(?apc) = "yes")
        }
        """
        return self.execute_sparql_query(query)

    def getJournalsWithDOAJSeal(self):
        query = """
        SELECT DISTINCT ?journal ?seal
        WHERE {
          ?journal <https://schema.org/Certification> ?seal .
          FILTER(LCASE(?seal) = "yes")
        }
        """
        return self.execute_sparql_query(query)

    def getCategoriesForJournal(self, journal_uri):
        journal_uri = journal_uri.replace('"', '\\"')
        query = f"""
        SELECT DISTINCT ?category
        WHERE {{
          <{journal_uri}> <https://schema.org/category> ?category .
        }}
        """
        return self.execute_sparql_query(query)

    def getAreasForJournal(self, journal_uri):
        journal_uri = journal_uri.replace('"', '\\"')
        query = f"""
        SELECT DISTINCT ?area
        WHERE {{
          <{journal_uri}> <https://schema.org/about> ?area .
        }}
        """
        return self.execute_sparql_query(query)

# ------------------------------------------------------------------------------------------------------
# Basic Query Engine - Edoardo AM Tarpinelli

class BasicQueryEngine(object):
    def __init__(self):
        self.journalQuery = [] # [0..*] - graph
        self.categoryQuery = [] # [0..*] - rdb

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

    
    def getAllJournals(self) -> list[Journal]:
        journal_list = list()
        if len(self.journalQuery) > 0:
            journal_df = pd.DataFrame() # df dove verranno aggregati i dati di tutti i giornali
            new_journal_df_list = list() # conterrè i df restituiti dai vari handler
            for handler in self.journalQuery:
                new_journal_df = handler.getAllJournals() # .getAllJournals() metodo di JournalQueryHandler - restituisce un df contenente tutti i journals presenti nel df
                new_journal_df_list.append(new_journal_df); 
                # alla fine si raccolgono i df in tutti gli handler    
                #   
            journal_df = new_journal_df_list[0] # prendo il df in posizione[0] nella lista new_journal_df_list al quale aggiungerò tutti i journals negli altri df
            for item in new_journal_df_list[1:]:
                journal_df = journal_df.merge(item, on=['journal_id'], how='inner').drop_duplicates(subset=['journal_id'], keep='first', inplace=True, ignore_index=True) 
            for idx, row in journal_df.iterrows():
                if row['journal_id'] != " " and row['journal_title'] != " ": # ISSN e EISSN --- guardare altre rows??
                    person = Journal(row['journal_id'], row['journal_title'])
                    journal_list.append(person)
        return journal_list
    
    def getJournalsWithTitle(self, partialTitle: str) -> list[Journal]:
        journal_list = list()
        if len(self.journalQuery) > 0:
            journal_df = pd.DataFrame() 
            new_journal_df_list = list() 
            for handler in self.journalQuery:
                new_journal_df = handler.getJournalsWithTitle(partialTitle)
                new_journal_df_list.append(new_journal_df); 
                 
            journal_df = new_journal_df_list[0]
            for item in new_journal_df_list[1:]:
                #
        return journal_list

    def getJournalsPublishedBy(self, partialName: str) -> list[Journal]:
        journal_list = list()
        if len(self.journalQuery) > 0:
            journal_df = pd.DataFrame() 
            new_journal_df_list = list() 
            for handler in self.journalQuery:
                new_journal_df = handler.getJournalsPublishedBy(partialName)
                new_journal_df_list.append(new_journal_df); 
                 
            journal_df = new_journal_df_list[0]
            for item in new_journal_df_list[1:]:
                #
        return journal_list

    def getJournalsWithLicense(self, licenses: set[str]) -> list[Journal]:
        journal_list = list()
        if len(self.journalQuery) > 0:
            journal_df = pd.DataFrame() 
            new_journal_df_list = list() 
            for handler in self.journalQuery:
                new_journal_df = handler.getJournalsWithLicense(licenses)
                new_journal_df_list.append(new_journal_df); 
                 
            journal_df = new_journal_df_list[0]
            for item in new_journal_df_list[1:]:
                #
        return journal_list

    def getJournalsWithAPC(self) -> list[Journal]:
        journal_list = list()
        if len(self.journalQuery) > 0:
            journal_df = pd.DataFrame() 
            new_journal_df_list = list() 
            for handler in self.journalQuery:
                new_journal_df = handler.getJournalsWithAPC()
                new_journal_df_list.append(new_journal_df); 
                 
            journal_df = new_journal_df_list[0]
            for item in new_journal_df_list[1:]:
                #
        return journal_list

    def getJournalsWithDOAJSeal(self) -> list[Journal]:
        journal_list = list()
        if len(self.journalQuery) > 0:
            journal_df = pd.DataFrame() 
            new_journal_df_list = list() 
            for handler in self.journalQuery:
                new_journal_df = handler.getJournalsWithDOAJSeal()
                new_journal_df_list.append(new_journal_df); 
                 
            journal_df = new_journal_df_list[0]
            for item in new_journal_df_list[1:]:
                # 
        return journal_list
    
    def getAllCategories(self) -> list[Category]:
        category_list = list()
        if len(self.categoryQuery) > 0:
            category_df = pd.DataFrame()
            new_category_df_list = list()
            for handler in self.categoryQuery:
                new_category_df = handler.getAllCategories()
                new_category_df_list.append(new_category_df)
            
            category_df = new_category_df_list[0]
            for item in new_category_df_list[1:]:
                #
        return category_list
    
    def getAllAreas(self) -> list[Area]:
        area_list = list()
        if len(self.categoryQuery) > 0:
            area_df = pd.DataFrame()
            new_area_df_list = list()
            for handler in self.categoryQuery:
                new_area_df = handler.getAllCategories()
                new_area_df_list.append(new_area_df)
            
            area_df = new_area_df_list[0]
            for item in new_area_df_list[1:]:
                #
        return area_list
        
    
    def getCategoriesWithQuartile(self, quartiles: set[str]) -> list[Category]:
        category_list = list()
        if len(self.categoryQuery) > 0:
            category_df = pd.DataFrame()
            new_category_df_list = list()
            for handler in self.categoryQuery:
                new_category_df = handler.getCategoriesWithQuartile(quartiles)
                new_category_df_list.append(new_category_df)
            
            category_df = new_category_df_list[0]
            for item in new_category_df_list[1:]:
                #
        return category_list    
    
    def getCategoriesAssignedToAreas(self, area_ids: set[str]) -> list[Category]:
        category_list = list()
        if len(self.categoryQuery) > 0:
            category_df = pd.DataFrame()
            new_category_df_list = list()
            for handler in self.categoryQuery:
                new_category_df = handler.getCategoriesAssignedToAreas(area_ids)
                new_category_df_list.append(new_category_df)
            
            category_df = new_category_df_list[0]
            for item in new_category_df_list[1:]:
                #
        return category_list 
        

    def getAreasAssignedToCategories(self, category_ids: set[str]) -> list[Area]:
        area_list = list()
        if len(self.categoryQuery) > 0:
            area_df = pd.DataFrame()
            new_area_df_list = list()
            for handler in self.categoryQuery:
                new_area_df = handler.getAreasAssignedToCategories(category_ids)
                new_area_df_list.append(new_area_df)
            
            area_df = new_area_df_list[0]
            for item in new_area_df_list[1:]:
                #
        return area_list
        
# ------------------------------------------------------------------------------------------------------
# Full Query Engine -
