#########################################################################################################################
# Author Name: Ingawale Kshitij
# create Date : 27-12-2023
# last update : 27-12-2023
# Description : this script generate the bronce schema of the route dataset
################################################################################################
from pyspark.sql import SparkSession
import argparse
from datetime import datetime,timedelta
from utils.comman_utils import *



if __name__ == '__main__':

    # create spark session
    spark = SparkSession.builder.master('local[*]').getOrCreate()

    # create parser
    parser = argparse.ArgumentParser()

    # add arguments
    parser.add_argument('-s', '--source_name', default='route', help='please provide source name/ dataset name')
    parser.add_argument('-sbc', '--schema_base_path',
                        default=r'C:\Users\kshit\PycharmProjects\Airline_project\source_schema',
                        help='please provide schema base path')
    parser.add_argument('-gdp', '--good_data_path', default=r'D:\airline_project_input\route\output')

    # parse the arguments
    args = parser.parse_args()
    source_name = args.source_name
    schema_base_path = args.schema_base_path
    good_data_path = args.good_data_path

    # get yesterday's date
    yesderdays_date = datetime.now() - timedelta(days=4)
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
    print(input_schema)

    # create a dataframe 1 object of the file
    df = spark.read.csv(input_file_path, input_schema)

    # get the list of the columns which has null values
    replace_col_list = cmutils.null_replace_columns(schema_base_path, source_name)

    # replace the null values with empty string ''
    df = cmutils.replace_null(df,replace_col_list)

    # write the replaced data to the good_data_path
    df.write.csv(fr'{good_data_path}',mode='append')

    # read the saved csv files into to write it into the parquet file
    parqurt_df = spark.read.csv(fr'{good_data_path}\*.csv',input_schema)

    #    write the final dataframe into the parquet file
    parqurt_df.repartition(1).write.parquet(fr'{good_data_path}\parquet_output',mode='overwrite')
