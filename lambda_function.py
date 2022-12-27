import awswrangler as wr
import pandas as pd
import urllib.parse
import os

"""
Temporary hard-coded AWS setting; i.e to be set as OS variable in Lambda | output location to store file
1. use environ function from os module to get the environment variables which are save in the E 

"""

os_input_s3_cleansed_layer = os.environ['s3_cleansed_layer']
os_input_glue_catalog_db_name = os.environ['glue_catalog_db_name']
os_input_glue_catalog_table_name = os.environ['glue_catalog_table_name']
os.environ['write_data_operation'] = 'append'
os_input_write_data_operation = os.environ['write_data_operation']

#create lambda handler function

def lambda_handler(event, context):
    #get object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    #decode bucket name and assign to variable key
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    try:
        
        """
        create Dataframe from content
        1. call read_json function from wr module - this reads json file from s3 bucket
        2. url of json file is formated usign format function
        3. string template has two place holders bucket variable contains name of s3 bucket and key contains name of json file
        
        """ 
        
        df_raw = wr.s3.read_json('s3://{}/{}'.format(bucket, key))
        
        """
        extract the required columns from json file
        use json_normalize function to flatten list of dictionaries from columns 'item' into a dataframe
        
        """
        
        df_step_1 = pd.json_normalize(df_raw['items'])
        
        """
        write to s3
        1. use to_parquet function from wr module to write a pandas DF to parquet file in the s3 
        
        """
        
        wr_reponse = wr.s3.to_parquet(
            df = df_step_1,
            path = os_input_s3_cleansed_layer,
            dataset=True,
            database = os_input_glue_catalog_db_name,
            table = os_input_glue_catalog_table_name,
            mode = os_input_write_data_operation
            )
        
        return wr_reponse
        
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
