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

            identifier_list = []

            category_mapping_dict = {} #using it to keep track of what we have
            categories_list = []

            area_mapping_dict = {}
            area_list = []

            #internal identifier of all the items 
            for idx, item in enumerate(json_data): 
                item_internal_id = ("item-" + str(idx)) # TODO: edo comment, i would use 'item_' instead of 'item-'
            
                #1. creating internal ids for each element: identifiers 
                identifiers = item.get("identifiers", []) #selecting the identifiers and using this method to retrive information from a dictionary and take into consideration the possibility that there is not an id 

                #iterating through the identifiers indise the bigger loop of items
                for idx, row in enumerate(identifiers): #i use the iteration because there are more than one in some cases 
                    identifiers_internal_id = ("internal_id-") + str(idx) #thi is useful even if redundant because the iteration makes the indexes always restart, so we have many internal id which are 0 or 1 

                    identifier_list.append({
                            "item_internal_id": item_internal_id,
                            "identifier_internal_id": identifiers_internal_id,
                            "identifiers": row #which is the single identifier 
                            })  #associating the data, with the internal id of the single category but also to the identifies of the whole item so that it's easier to query 

                #2. creating internal ids for the categories, this is trickier because they have more than one value and they can have same id
                #i have to iterate thourg everything but check if the "id" is the same, so it's useful to use a dictionary 
                categories = item.get("categories", []) #especially for category, quartile and area, that in the UML are noted as optional ([0...*]) it's better to do it this way 

                for row in categories: #appunto per me, scrivere cat_id = category["id"] non ha senso perchè category è una lista di un dizionario, io devo internere come dizionario il singolo item 

                    cat_id = row.get("id")

                    if cat_id not in category_mapping_dict: #checking if the category is not already in the dictionary 
                        
                        category_id_internal_id = ("category_id-") + str(len(category_mapping_dict))
                        category_mapping_dict[cat_id] = (category_id_internal_id)
                    else: 
                        category_id_internal_id = category_mapping_dict[cat_id] #if it's already inside the dict consider the original id 

                    #checking for the quartile, because it's optional in the UML
                    quartile = row.get("quartile", "")

                    categories_list.append({
                        "item_internal_id": item_internal_id,
                        "category_internal_id" : category_id_internal_id,
                        "id": cat_id,
                        "quartile": quartile
                    })
                
            
                #3. creating internal ids for areas, this is the same but without any more value 
                areas = item.get("areas", [])

                for row in areas: 
                    area_section = areas[0]

                    if row not in area_mapping_dict: 

                        area_id = (("areas-") + str(len(area_mapping_dict)))
                        area_mapping_dict[area_section] = area_id
                    else: 
                        area_id = area_mapping_dict[area_section]
                
                    area_list.append({
                        "item_internal_id": item_internal_id, 
                        "area_internal_id": area_id,
                        "area": area_section
                    })
            # print(category_mapping_dict)
            
            #converting the data in dataframes 
            identifiers_df = pd.DataFrame(identifier_list)
            categories_df = pd.DataFrame(categories_list)
            areas_df = pd.DataFrame(area_list)

        with connect(self.dbPathOrUrl) as con:
            identifiers_df.to_sql("identifiers", con, if_exists="replace", index=False)
            categories_df.to_sql("categories", con, if_exists="replace", index=False)
            areas_df.to_sql("areas", con, if_exists="replace", index=False)

                # TODO: why not 'con.commit()'
            
#second case: the path is the one of a graph database, the csv file

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
        #giving unique identifiers 
        base_url = "https://comp-data.github.io/res" 
                    
        for idx, row in journals.iterrows(): 
            local_id = "journal-" + str(idx)
            subj = URIRef(base_url + local_id) #new local identifiers for each item in the graph database 

            my_graph.add(((subj, RDF.type, Journal))) #the subject of the row is a journal 
                
            #checking every category in the row (which is none other than a list of vocabularies)
            if row["Journal title"]: 
                my_graph.add((subj, title, Literal(row["Journal title"])))
            # TODO: what is Literal?
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



# ------------------------------------------------------------------------------------------------------
# CategoryQueryHandler and QueryHandler - Cecilia Vesci

class QueryHandler:
    def __init__(self):
        self.dbPathOrUrl = ""

    def getDbPathOrUrl(self):
        return self.dbPathOrUrl

    def setDbPathOrUrl(self, path):
        self.dbPathOrUrl = path

    def getById(self, id):
        """
        Questo metodo cerca un'entità identificabile per ID nel database.
        """
        raise NotImplementedError("Questo metodo deve essere implementato nelle sottoclassi.")
    

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
            with connect(self.dbPathOrUrl) as con:
                query = "SELECT DISTINCT id FROM categories"
                df = pd.read_sql_query(query, con)
            return df

        # Prendere tutte le aree (distinte)
    def getAllAreas(self):
            with connect(self.dbPathOrUrl) as con:
                query = "SELECT DISTINCT area FROM areas"
                df = pd.read_sql_query(query, con)
            return df

        # Prendere tutte le categorie che hanno un certo quartile
    def getCategoriesWithQuartile(self, quartiles: set):
            placeholders = ', '.join(['?'] * len(quartiles))
            with connect(self.dbPathOrUrl) as con:
                query = f"""
                SELECT DISTINCT id, quartile 
                FROM categories 
                WHERE quartile IN ({placeholders})
                """
                df = pd.read_sql_query(query, con, params=list(quartiles))
            return df

        # Prendere tutte le categorie associate a una lista di aree
    def getCategoriesByAreas(self, areas: set):
            placeholders = ', '.join(['?'] * len(areas))
            with connect(self.dbPathOrUrl) as con:
                query = f"""
                SELECT DISTINCT c.id, a.area
                FROM categories c
                JOIN areas a ON c.item_internal_id = a.item_internal_id
                WHERE a.area IN ({placeholders})
                """
                df = pd.read_sql_query(query, con, params=list(areas))
            return df

        # Prendere tutti gli item (journal, ecc) associati a una categoria specifica
    def getItemsByCategory(self, category_id: str):
            with connect(self.dbPathOrUrl) as con:
                query = """
                SELECT DISTINCT item_internal_id
                FROM categories
                WHERE id = ?
                """
                df = pd.read_sql_query(query, con, params=(category_id,))
            return df

        # Prendere tutti gli item associati a una area specifica
    def getItemsByArea(self, area_name: str):
            with connect(self.dbPathOrUrl) as con:
                query = """
                SELECT DISTINCT item_internal_id
                FROM areas
                WHERE area = ?
                """
                df = pd.read_sql_query(query, con, params=(area_name,))
            return df


# ------------------------------------------------------------------------------------------------------
# JournalQueryHandler - Faride

class JournalQueryHandler(QueryHandler):
    # TODO: we have to deal with repetition in all the methods
    def __init__(self):
        self.dbPathOrUrl = ""

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
        SELECT DISTINCT ?journal ?title ?issn ?eissn ?languages ?publisher ?license ?apc ?seal
        WHERE {
          ?journal a <https://schema.org/Periodical> ;
                   <https://schema.org/title> ?title ;
                   <https://schema.org/identifier> ?issn ;
                   <https://schema.org/identifier> ?eissn ;
                   <https://schema.org/inLanguage> ?languages ;
                   <https://schema.org/license> ?license .
          OPTIONAL { ?journal <https://schema.org/publisher> ?publisher }
          OPTIONAL { ?journal <https://schema.org/isAccessibleForFree> ?apc }
          OPTIONAL { ?journal <https://schema.org/Certification> ?seal }
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
    # TODO: we have to deal with partial input strings
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
            all_journal_dfs = []
            for handler in self.journalQuery:
                new_journal_df = handler.getAllJournals()
                all_journal_dfs.append(new_journal_df)

            if all_journal_dfs:
                # concatenate all journal df in the list
                journal_df = pd.concat(all_journal_dfs, ignore_index=True)
                # remove duplicates based on 'journal' name
                journal_df.drop_duplicates(subset=['journal'], keep='first', inplace=True, ignore_index=True)
                
            else:
                # if all_journal_dfs == False: return empty df
                journal_df = pd.DataFrame() 

        # convert df into list of Python Objects
        for index, row in journal_df.iterrows():
            journal = Journal(
                id=[row['journal']],  
                title=row['title'],
                languages=[row['languages']] if pd.notna(row['languages']) else [],
                publisher=row['publisher'] if pd.notna(row['publisher']) else None,
                seal=row['seal'] if pd.notna(row['seal']) and str(row['seal']).lower() == 'yes' else False,
                license=row['license'] if pd.notna(row['license']) else None,
                apc=row['apc'] if pd.notna(row['apc']) and str(row['apc']).lower() == 'yes' else False,
                hasCategory=[],  # Dovrai recuperare le categorie separatamente se necessario
                hasArea=[]      # Dovrai recuperare le aree separatamente se necessario
            )
            journal_list.append(journal)

        return journal_list, len(journal_list)
    
    def getJournalsWithTitle(self, partialTitle: str) -> list[Journal]:
        journal_list = list()
        if len(self.journalQuery) > 0:
            all_journal_dfs = []
            for handler in self.journalQuery:
                new_journal_df = handler.getJournalsWithTitle(partialTitle)
                all_journal_dfs.append(new_journal_df)

            if all_journal_dfs:
                # concatenate all journal df in the list
                journal_df = pd.concat(all_journal_dfs, ignore_index=True)
                # remove duplicates based on 'journal' name
                journal_df.drop_duplicates(subset=['journal'], keep='first', inplace=True, ignore_index=True)
                
            else:
                # if all_journal_dfs == False: return empty df
                journal_df = pd.DataFrame()
                
        # convert df into list of Python Objects
        for index, row in journal_df.iterrows():
            if row['title'] == partialTitle:
                journal = Journal(
                    id=[row['journal']],  
                    title=row['title'],
                    languages=[row['languages']] if pd.notna(row['languages']) else [],
                    publisher=row['publisher'] if pd.notna(row['publisher']) else None,
                    seal=row['seal'] if pd.notna(row['seal']) and str(row['seal']).lower() == 'yes' else False,
                    license=row['license'] if pd.notna(row['license']) else None,
                    apc=row['apc'] if pd.notna(row['apc']) and str(row['apc']).lower() == 'yes' else False,
                    hasCategory=[],  # Dovrai recuperare le categorie separatamente se necessario
                    hasArea=[]      # Dovrai recuperare le aree separatamente se necessario
                )
                journal_list.append(journal)

        return journal_list, len(journal_list)

    def getJournalsPublishedBy(self, partialName: str) -> list[Journal]:
        journal_list = list()
        if len(self.journalQuery) > 0:
            all_journal_dfs = []
            for handler in self.journalQuery:
                new_journal_df = handler.getJournalsPublishedBy(partialName)
                all_journal_dfs.append(new_journal_df)

            if all_journal_dfs:
                # concatenate all journal df in the list
                journal_df = pd.concat(all_journal_dfs, ignore_index=True)
                # remove duplicates based on 'journal' name
                journal_df.drop_duplicates(subset=['journal'], keep='first', inplace=True, ignore_index=True)
                
            else:
                # if all_journal_dfs == False: return empty df
                journal_df = pd.DataFrame()

        # convert df into list of Python Objects
        for index, row in journal_df.iterrows():
            if row['publisher'] == partialName:
                journal = Journal(
                    id=[row['journal']],  
                    title=row['title'],
                    languages=[row['languages']] if pd.notna(row['languages']) else [],
                    publisher=row['publisher'] if pd.notna(row['publisher']) else None,
                    seal=row['seal'] if pd.notna(row['seal']) and str(row['seal']).lower() == 'yes' else False,
                    license=row['license'] if pd.notna(row['license']) else None,
                    apc=row['apc'] if pd.notna(row['apc']) and str(row['apc']).lower() == 'yes' else False,
                    hasCategory=[],  # Dovrai recuperare le categorie separatamente se necessario
                    hasArea=[]      # Dovrai recuperare le aree separatamente se necessario
                )
                journal_list.append(journal)

        return journal_list, len(journal_list)

    def getJournalsWithLicense(self, licenses: set[str]) -> list[Journal]:
        journal_list = list()
        if len(self.journalQuery) > 0:
            all_journal_dfs = []
            for handler in self.journalQuery:
                new_journal_df = handler.getJournalsWithLicense(licenses)
                all_journal_dfs.append(new_journal_df)

            if all_journal_dfs:
                # concatenate all journal df in the list
                journal_df = pd.concat(all_journal_dfs, ignore_index=True)
                # remove duplicates based on 'journal' name
                journal_df.drop_duplicates(subset=['journal'], keep='first', inplace=True, ignore_index=True)
                
            else:
                # if all_journal_dfs == False: return empty df
                journal_df = pd.DataFrame()
        # convert df into list of Python Objects
        for index, row in journal_df.iterrows():
            if row['license'] == licenses:
                journal = Journal(
                    id=[row['journal']],  
                    title=row['title'],
                    languages=[row['languages']] if pd.notna(row['languages']) else [],
                    publisher=row['publisher'] if pd.notna(row['publisher']) else None,
                    seal=row['seal'] if pd.notna(row['seal']) and str(row['seal']).lower() == 'yes' else False,
                    license=row['license'] if pd.notna(row['license']) else None,
                    apc=row['apc'] if pd.notna(row['apc']) and str(row['apc']).lower() == 'yes' else False,
                    hasCategory=[],  # Dovrai recuperare le categorie separatamente se necessario
                    hasArea=[]      # Dovrai recuperare le aree separatamente se necessario
                )
                journal_list.append(journal)

        return journal_list, len(journal_list)

    def getJournalsWithAPC(self) -> list[Journal]:
        journal_list = list()
        if len(self.journalQuery) > 0:
            all_journal_dfs = []
            for handler in self.journalQuery:
                new_journal_df = handler.getJournalsWithAPC()
                all_journal_dfs.append(new_journal_df)

            if all_journal_dfs:
                # concatenate all journal df in the list
                journal_df = pd.concat(all_journal_dfs, ignore_index=True)
                # remove duplicates based on 'journal' name
                journal_df.drop_duplicates(subset=['journal'], keep='first', inplace=True, ignore_index=True)
                
            else:
                # if all_journal_dfs == False: return empty df
                journal_df = pd.DataFrame()
        # convert df into list of Python Objects
        for index, row in journal_df.iterrows():
            if row['apc'] == 'Yes':
                journal = Journal(
                    id=[row['journal']],  
                    title=row['title'],
                    languages=[row['languages']] if pd.notna(row['languages']) else [],
                    publisher=row['publisher'] if pd.notna(row['publisher']) else None,
                    seal=row['seal'] if pd.notna(row['seal']) and str(row['seal']).lower() == 'yes' else False,
                    license=row['license'] if pd.notna(row['license']) else None,
                    apc=row['apc'] if pd.notna(row['apc']) and str(row['apc']).lower() == 'yes' else False,
                    hasCategory=[],  # Dovrai recuperare le categorie separatamente se necessario
                    hasArea=[]      # Dovrai recuperare le aree separatamente se necessario
                )
                journal_list.append(journal)

        return journal_list, len(journal_list)

    def getJournalsWithDOAJSeal(self) -> list[Journal]:
        journal_list = list()
        if len(self.journalQuery) > 0:
            all_journal_dfs = []
            for handler in self.journalQuery:
                new_journal_df = handler.getJournalsWithDOAJSeal()
                all_journal_dfs.append(new_journal_df)

            if all_journal_dfs:
                # concatenate all journal df in the list
                journal_df = pd.concat(all_journal_dfs, ignore_index=True)
                # remove duplicates based on 'journal' name
                journal_df.drop_duplicates(subset=['journal'], keep='first', inplace=True, ignore_index=True)
                
            else:
                # if all_journal_dfs == False: return empty df
                journal_df = pd.DataFrame()
        # convert df into list of Python Objects
        for index, row in journal_df.iterrows():
            if row['seal'] == 'Yes':
                journal = Journal(
                    id=[row['journal']],  
                    title=row['title'],
                    languages=[row['languages']] if pd.notna(row['languages']) else [],
                    publisher=row['publisher'] if pd.notna(row['publisher']) else None,
                    seal=row['seal'] if pd.notna(row['seal']) and str(row['seal']).lower() == 'yes' else False,
                    license=row['license'] if pd.notna(row['license']) else None,
                    apc=row['apc'] if pd.notna(row['apc']) and str(row['apc']).lower() == 'yes' else False,
                    hasCategory=[],  # Dovrai recuperare le categorie separatamente se necessario
                    hasArea=[]      # Dovrai recuperare le aree separatamente se necessario
                )
                journal_list.append(journal)

        return journal_list, len(journal_list)
    
    # def getAllCategories(self) -> list[Category]:
    #     category_list = list()
    #     if len(self.categoryQuery) > 0:
    #         category_df = pd.DataFrame()
    #         new_category_df_list = list()
    #         for handler in self.categoryQuery:
    #             new_category_df = handler.getAllCategories()
    #             new_category_df_list.append(new_category_df)
            
    #         category_df = new_category_df_list[0]
    #         for item in new_category_df_list[1:]:
    #             #
    #     return category_list
    
    # def getAllAreas(self) -> list[Area]:
    #     area_list = list()
    #     if len(self.categoryQuery) > 0:
    #         area_df = pd.DataFrame()
    #         new_area_df_list = list()
    #         for handler in self.categoryQuery:
    #             new_area_df = handler.getAllCategories()
    #             new_area_df_list.append(new_area_df)
            
    #         area_df = new_area_df_list[0]
    #         for item in new_area_df_list[1:]:
    #             area_df = area_df.merge(item, on=['category_id'], how='inner').drop_duplicates(subset=['author_id'], keep='first', inplace=True, ignore_index=True);
            
    #         for idx, row in person_df.iterrows():
    #             if row["author_id"] != " " and row["author_name"] != " ":
    #                 person = Person(row["author_id"], row["author_name"]);
    #                 person_list.append(person);
    #     return area_list
        
    
    # def getCategoriesWithQuartile(self, quartiles: set[str]) -> list[Category]:
    #     category_list = list()
    #     if len(self.categoryQuery) > 0:
    #         category_df = pd.DataFrame()
    #         new_category_df_list = list()
    #         for handler in self.categoryQuery:
    #             new_category_df = handler.getCategoriesWithQuartile(quartiles)
    #             new_category_df_list.append(new_category_df)
            
    #         category_df = new_category_df_list[0]
    #         for item in new_category_df_list[1:]:
    #             #
    #     return category_list    
    
    # def getCategoriesAssignedToAreas(self, area_ids: set[str]) -> list[Category]:
    #     category_list = list()
    #     if len(self.categoryQuery) > 0:
    #         category_df = pd.DataFrame()
    #         new_category_df_list = list()
    #         for handler in self.categoryQuery:
    #             new_category_df = handler.getCategoriesAssignedToAreas(area_ids)
    #             new_category_df_list.append(new_category_df)
            
    #         category_df = new_category_df_list[0]
    #         for item in new_category_df_list[1:]:
    #             #
    #     return category_list 
        

    # def getAreasAssignedToCategories(self, category_ids: set[str]) -> list[Area]:
    #     area_list = list()
    #     if len(self.categoryQuery) > 0:
    #         area_df = pd.DataFrame()
    #         new_area_df_list = list()
    #         for handler in self.categoryQuery:
    #             new_area_df = handler.getAreasAssignedToCategories(category_ids)
    #             new_area_df_list.append(new_area_df)
            
    #         area_df = new_area_df_list[0]
    #         for item in new_area_df_list[1:]:
    #             #
    #     return area_list
        
# ------------------------------------------------------------------------------------------------------
# Full Query Engine -
