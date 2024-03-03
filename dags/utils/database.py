from typing import List

from pandas import DataFrame
import mysql.connector as mysql
from airflow.models import Variable


class MySQLHandler:
    def __init__(self):
        self.cxn = None
    
    def connect(self):
        self.cxn = mysql.connect(host=Variable.get("MYSQL_HOST"), user=Variable.get("MYSQL_USER"), 
                        password=Variable.get("MYSQL_PSWD"), database=Variable.get("MYSQL_DB"))
        
    def execute(self, query: str) -> DataFrame:
        cur = self.cxn.cursor()
        cur.execute(query)

        headers = [tup[0] for tup in cur.description]
        df = DataFrame.from_records(cur.fetchall(), columns=headers)
        cur.close()
        return df

    def execute_batch(self, queries: List[str]):
        cur = self.cxn.cursor()
        try:
            for query in queries:
                cur.execute(query)
            cur.close()
        except mysql.Error:
            self.cxn.rollback()
        else:
            self.commit()
    
    def commit(self):
        self.cxn.commit()

    def close(self):
        self.cxn.close()