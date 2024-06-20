#########################################################################################################
# Author Name : Kshitij Ingawale
# Create Date : 22-12-2023
# Update Date : 22-12-2023
# Description : this script generate the bronze layer of the input Dataset
#######################################################################################################
import logging

from pyspark.sql import SparkSession
from datetime import datetime,timedelta
import argparse
from utils.comman_utils import CommonUtils
from pyspark.sql.functions import *
if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO, filename='logging_inf_airline',
                        filemode='w', format='%(asctime)s - %(levelname)s - %(message)s')
    # create spark session
    spark = SparkSession.builder.master('local[*]').getOrCreate()

    # create parser
    parser = argparse.ArgumentParser()

    # add arguments
    parser.add_argument('-s', '--source_name', default='airline', help='please provide source name/ dataset name')
    parser.add_argument('-sbc','--schema_base_path',default=r'C:\Users\kshit\PycharmProjects\Airline_project\source_schema',help= 'please provide schema base path')
    parser.add_argument('-gdp','--result_data_path',default=r'D:\airline_project_input\airline\output')


    # parse the arguments
    args = parser.parse_args()
    source_name = args.source_name
    schema_base_path = args.schema_base_path
    result_data_path = args.result_data_path

    # get yesterday's date
    yesderdays_date = datetime.now() - timedelta(days=10)
    yesterdays_date = yesderdays_date.strftime('%Y%m%d')

    # get or generate  schema
    base_path = r"D:\airline_project_input"

    # initilize the bronze schema of the file
    # D:\airline_input\<source_name>\yyyyMMdd
    input_file_path = fr'{base_path}\{source_name}\{yesterdays_date}\*.csv'

    # created instance of the class CommonUtils
    cmutils = CommonUtils()

    # generate the schema of the input file
    input_schema = cmutils.generate_bronze_schema(schema_base_path, source_name)

    # create a dataframe 1 object of the file
    df = spark.read.csv(input_file_path,input_schema)


    # created the list of the column name which require null check
    null_columns  = cmutils.column_null_check(schema_base_path,source_name)

    # check the null value
    df = cmutils.null_check(df,null_columns)
    print('the regular dataset with null check is as follows')
    df.show()

    # seperated good data from the dataframe
    good_df = df.filter((col('null_check') == '') & (col('airline_id') > 0 ))
    # good_df.show(100)

    # seperated bad data from th dataframe
    bad_df = df.filter((col('null_check')!= '') | (col('airline_id') < 1))
    # bad_df.show(100)




    # saved good data as a parquet file
    good_df.write.parquet(fr'{result_data_path}\good_data',mode='overwrite')
    logging.info(f'the good data saved at {result_data_path}\good_data with {good_df.count()} records and {good_df.columns} columns')


    # save the bad date as a parquet file
    bad_df.write.csv(fr'{result_data_path}\bad_data',mode='overwrite')
    logging.info(f'the bad data saved at {result_data_path}\bad_data with {bad_df.count()} records and {bad_df.columns} columns')



