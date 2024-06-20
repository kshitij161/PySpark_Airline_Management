from pyspark.sql import SparkSession
import argparse
from utils.comman_utils import CommonUtils
import logging

if __name__ == '__main__':
    # create spark session
    spark = SparkSession.builder.master('local[*]').getOrCreate()

    # crete the logging file  in the write mode with message format
    logging.basicConfig(level=logging.INFO,filename='logging_info_plane',filemode='w',
                        format='%(asctime)s - %(levelname)s - %(message)s')

    # create parser
    parser = argparse.ArgumentParser()

    # add arguments
    parser.add_argument('--source_name', default='plane', help='please provide source name/dataset name')
    parser.add_argument('-sbp', '--schema_base_path',
                        default=r'C:\Users\kshit\PycharmProjects\Airline_project\source_schema',
                        help='please provide schema path/dataset path')
    parser.add_argument('-gdp', '--good_data_path', default=r"D:\Airline_project_input\plane\output")

    # parse the arguments
    args=parser.parse_args()

    # get arguments
    source_name = args.source_name
    schema_base_path = args.schema_base_path
    good_data_path = args.good_data_path



    # get or generate  schema
    base_path = r"D:\Airline_project_input"
    input_file_path = fr"{base_path}\{source_name}\{source_name}\*.csv"




    # created instance of the class CommonUtils
    commanutils = CommonUtils()

    # generate the schema of the input file
    input_schema = commanutils.generate_bronze_schema(schema_base_path, source_name)
    # create a dataframe 1 object of the file

    df = spark.read.csv(input_file_path, input_schema)
    # df.show()
        # write the data to parquet format
    df.write.parquet(good_data_path,mode='overwrite')
    print('file saved successfully')

    logging.info(f'the file saved successfully with {df.count()} rows and {df.columns} columns')




    







