import luigi
import pandas as pd
from helper.db_connector import postgres_engine

class ExtractData(luigi.Task):
    
    def requires(self):
        pass

    def run(self):
        # read data
        marketing_data = pd.read_csv("marketing_data.csv")
        
        marketing_data.to_csv(self.output().path, index = False)

    def output(self):
        return luigi.LocalTarget("data/raw/extracted_data.csv")

class TransformData(luigi.Task):
    
    def requires(self):
        return ExtractData()
    
    def run(self):
        # read data from previous process
        extracted_data = pd.read_csv(self.input().path)

        # initialize dictionary for rename columns
        RENAME_COLS = {
                        "CustomerID": "customer_id",
                        "Genre": "gender",
                        "Age": "age",
                        "Annual_Income_(k$)": "annual_income",
                        "Spending_Score": "spending_score"
                    }
        
        extracted_data = extracted_data.rename(columns = RENAME_COLS)

        # filter data based on condition
        extracted_data = extracted_data[extracted_data["spending_score"] >= 50]

        extracted_data.to_csv(self.output().path, index = False)

    def output(self):
        return luigi.LocalTarget("data/transform/transformed_data.csv")
    
    
class LoadData(luigi.Task):

    def requires(self):
        return TransformData()
    
    def run(self):
        
        # read data from transformed task
        transformed_data = pd.read_csv(self.input().path)

        # create engine
        engine = postgres_engine()

        # insert to database
        transformed_data.to_sql(name = "mall_customer",
                              con = engine, 
                              if_exists = "append",
                              index = False)

    def output(self):
        pass

if __name__ == "__main__":
    luigi.build([ExtractData(),
                 TransformData(),
                 LoadData()], local_scheduler=True)
