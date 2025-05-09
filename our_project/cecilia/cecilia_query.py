
import os
import select
import pandas as pd
from sqlite3 import connect
from pandas import read_sql_query

# === Classe base QueryHandler ===
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
            df = read_sql_query(query, con)
        return df

    # Prendere tutte le aree (distinte)
  def getAllAreas(self):
        with connect(self.dbPathOrUrl) as con:
            query = "SELECT DISTINCT area FROM areas"
            df = read_sql_query(query, con)
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
            df = read_sql_query(query, con, params=list(quartiles))
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
            df = read_sql_query(query, con, params=list(areas))
        return df

    # Prendere tutti gli item (journal, ecc) associati a una categoria specifica
  def getItemsByCategory(self, category_id: str):
        with connect(self.dbPathOrUrl) as con:
            query = """
            SELECT DISTINCT item_internal_id
            FROM categories
            WHERE id = ?
            """
            df = read_sql_query(query, con, params=(category_id,))
        return df

    # Prendere tutti gli item associati a una area specifica
  def getItemsByArea(self, area_name: str):
        with connect(self.dbPathOrUrl) as con:
            query = """
            SELECT DISTINCT item_internal_id
            FROM areas
            WHERE area = ?
            """
            df = read_sql_query(query, con, params=(area_name,))
        return df

