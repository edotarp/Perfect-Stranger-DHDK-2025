

import pandas as pd
import numpy as np
import SPARQLWrapper

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