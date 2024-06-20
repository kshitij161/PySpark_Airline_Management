######################################################################################################
# Author Name : Kshitij Ingawale
# Create Date : 16-12-2023
# Last update : 18-12-2023
# Description : This script is generating bronze layer based on input dataset
################################################################################################################
from pyspark.sql import SparkSession
import argparse
from datetime import *
from utils.comman_utils import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import logging
if __name__ == '__main__':
    # Initializing Spark Session
    spark = SparkSession.builder.master('local[*]').getOrCreate()

    # Creating logging file
    logging.basicConfig(level=logging.INFO,filename='logging_info_airport',filemode='a'
                        ,format='%(asctime)s - %(levelname)s - %(message)s')

    # Creating Dataframe on input file.
    parser = argparse.ArgumentParser()
    parser.add_argument('--source_name', default='airport',
                        help='please provide source name/dataset name')
    parser.add_argument('-sbp', '--schema_base_path',
                        default=r'C:\Users\kshit\PycharmProjects\Airline_project\source_schema',
                        help='please provide schema path/dataset path')
    parser.add_argument('-gdp','--data_path',default=r"D:\Airline_project_input\output")

    # Initilizing the parser argument
    args = parser.parse_args()
    source_name = args.source_name
    schema_base_path = args.schema_base_path
    data_path = args.data_path


    # Get yesterday's date
    yesterdays_date = datetime.now() - timedelta(days=33)
    yesterdays_date = yesterdays_date.strftime('%Y%m%d')
    print(yesterdays_date)
    #Initializing variable
    #D:\Airline_project_input\<source_name>\yyyyMMdd\+.csv
    base_path = r"D:\Airline_project_input"
    input_file_path = fr"{base_path}\{source_name}\{yesterdays_date}\*.csv"

    output_file_path = fr"{base_path}\{source_name}\{yesterdays_date}\output"

    # get or generate  schema
    commanutils = CommonUtils()

    input_schema = commanutils.generate_bronze_schema(schema_base_path, source_name)

    # create a dataframe 1 object of the file
    df = spark.read.csv(input_file_path, input_schema)
    df.show()

    #Initilize the list of the columns that need to special charater check
    special_character_column_list = commanutils.column_special_character(schema_base_path, source_name)
    print(special_character_column_list)

    #  check the special character of the file
    df = commanutils.special_character_check(df, special_character_column_list)

    # Seperate the good data  from data frame
    good_data = df.select("*").filter(col('special_char') == '').drop('special_char')
    good_data.show()

    # seperate bad data from data
    bad_data = df.select('*').filter(col('special_char') != '')
    bad_data.show()
    #adding one column into the dataframe with value time and date
    good_data = good_data.withColumn('current_timestamp',current_timestamp())

    # write a good data at the good_data path
    good_data.write.csv(fr'{data_path}\good_data_csv',mode='append')

    # saved the information of the number of records and columns saved in the csv file
    logging.info(f'The good data of {yesterdays_date} saved at the path {data_path} '
                 f'with {good_data.count()} records and {df.columns} columns')

    # write a bad data at the bad_data path
    bad_data.write.csv(fr'{data_path}\bad_data_csv',mode='append')


    # saved the information of the number of records and columns saved in the csv file
    logging.info(f'The bad data of {yesterdays_date} saved at the path {data_path} with'
                 f' {bad_data.count()} records and {df.columns} columns')

    # write a bad data at the good_data path

    final_good_data = spark.read.csv(fr"{data_path}\good_data_csv\*.csv",input_schema)


    # create a window specification
    windowSpec = Window.partitionBy('airport_id').orderBy(col('current_timestamp').desc())

    # add a row number to the dataframe and filter the row number to be 1
    final_good_data = final_good_data.withColumn('row_number',row_number().over(windowSpec)).filter('row_number == 1')

    final_good_data =final_good_data.drop('current_timestamp')
    final_good_data = final_good_data.drop('row_number')
    final_good_data = final_good_data.drop('special_char')
    final_good_data.show(100)
    # write a good data at the good_data path
    final_good_data.repartition(1).write.parquet(fr'{data_path}\good_data_parquet',mode='overwrite')

    # saved the information of the number of records and columns saved in the parquet file
    logging.info(f'the good data(parquet) saved at the path {data_path} with {final_good_data.count()} records and {df.columns} columns')


