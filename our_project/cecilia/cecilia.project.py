# Class to interact with SQLite database 
import os
import select
import pandas as pd
from sqlite3 import connect

class CategoryQueryHandler(QueryHandler):
  def __init__(self, dbPathOrUrl=""):
        #super().__init__()
        self.dbPathOrUrl = dbPathOrUrl
        self.db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), self.dbPathOrUrl))
  
  def getById(self, id: str):
      return pd.DataFrame() # return empty dataframe as no IdentifiableEntity in relational db
   
  def executeQuery(self, sql_command):
       connection = connect(self.dbPathOrUrl);
       cursor = connection.cursor();
       cursor.execute(sql_command);
       df = pd.DataFrame(cursor.fetchall(), columns = [description[0] for description in cursor.description]);
       #df.columns = [description[0] for description in cursor.description]; # setting column names with list comprehension because sqlite lacks a normal reference to column names
       connection.close()
       return df

def getAllActivities(self):
    sql_command = ""
    SELECT
        Id AS Activity_internal_id,
        Title AS [Refers To],
        Owner AS [Responsible Institute],
        Author AS [Responsible Person],
        Type AS Technique,
        Date AS [Start Date],
        Date AS [End Date],
        Place AS Tool
    FROM activities
    """
    return self.executeQuery(sql_command)