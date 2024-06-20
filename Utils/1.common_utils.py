from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import *


class CommonUtils:
    def generate_bronze_schema(self, schema_base_path, source_name):
        ''' This method will generate  pyspark schema or a bronze layer dataframe
        :param source_name,base_path
        :return StructType schema '''

        # Initiallize variable
        struct_field_lst = []
        try:
            # Reading input schema files
            file = open(fr"{schema_base_path}\{source_name}_schema.csv", "r")

            # Iterating over each element in input files
            for element in file.readlines():
                column_name = element.split(',')[0]
                # datatype = element.split(',')[1]

                # Generating StructType schema values
                struct_field_lst.append(StructField(column_name, StringType()))

            input_schema = StructType(struct_field_lst)

        except Exception as e:
            raise Exception(f"ERROR : While running generate_bronze_schema method Failed with an  exception {e}")

        return input_schema


    def special_character_check(self, df, list):

        """This method retrieves the special characters from the dataframe.
        :param df: Input dataframe.
        :param column_list: List of columns in the dataframe.
        :return df: Dataframe with additional 'special_char' column."""
        try:
            special = "[^A-za-z-0-9|\|/|\s*]"
            df = df.withColumn('special_char', lit(''))
            for i in list:
                # it checks the special character of the column if special character is present it will extract and insert into the new column
                df = df.withColumn('special_char',
                                   when(col('special_char') == '', regexp_extract(i, special, 0)).otherwise(
                                       col('special_char')))

            # return the df which have additional special character column
            return df
        except Exception as e:
            print(f"ERROR: error occurred while running  special_character_check method failed. Exception :  {e}")

    def column_special_character(self, schema_base_path, source_name):

        '''
        this function retrieves the column names of the schema which requires the special character check
        :param schema_base_path : The path of the schema
        :param source_name: The name of the source name
        :return list of the columns
        '''
        try:
            # Open the schema file for the given source name
            file = open(fr'{schema_base_path}\{source_name}_schema.csv', 'r')

            # Initialize an empty list to store the column names
            columns = []

            # Iterate over each line in the file
            for j in file.readlines():
                # Check if the line contains the special character
                if 'special_character' in j:
                    # Extract the column name from the line and append ot to the list
                    columns.append(j[0:j.find(',')])

        except Exception as e:
            raise Exception(f"ERROR : While running column_special_character  method Failed with an  exception {e}")


        # Return the list of column names
        return columns

    def column_null_check(self, schema_base_path:str, source_name:str):

        ''' this function retrieves the column names of the schema which requires the special character check
        :param shema_base_path : The base path of the schema
        :param source_name: The name of the source
        :return A list of the column names '''
        try:
            # Open the schema file for the given source name
            file = open(fr'{schema_base_path}\{source_name}_schema.csv', 'r')
            columns = []
            for line in file.readlines():
                if 'null_check' in line:
                    columns.append(line[0:line.find(',')])
        except Exception as e:
            raise Exception(f"ERROR : While running column_null_check method Failed with an  exception {e}")

        return columns

    def null_check(self, df, columns):

        '''this function checks for the null values in the dataframe
        :param df: The input dataframe
        :param columns: list of the columns in which the null character are present
        :return df : The with an additional column "null_check" that indicates if a null value is present'''
        try:
            # Add a new column 'null_check' with an empty string as the initial value
            df = df.withColumn("null_check", lit(''))

            # Loop through each collumn and update the 'null_check' column based onn null values in the column
            for column in columns:
                df = df.withColumn("null_check", when(df[column].isNull(), 'null').otherwise(df["null_check"]))

        except Exception as e:
            raise Exception(f"ERROR : While running null_check method Failed with an  exception {e}")
        return df

    def null_replace_columns(self, schema_base_path, source_name):
        '''Find and returns a list of null columns in a given source file
        :param schema_base_path: The base path of the schema files
        :param source_name: The name of the source file
        :returns null_columns: A list of null columns in the source file'''

        # Initialize an empty list to store the null columns
        null_columns = []

        # Open the schema file for the given source name
        file = open(fr"{schema_base_path}\{source_name}_schema.csv")
        # Iterating over each element in input files

        for line in file.readlines():
            # Split the line by comma
            splited_line = line.split(',')
            # Check if the line contains the string 'replace\n'
            if 'replace\n' in splited_line:
                # Append the first element of the line to the null_columns list
                null_columns.append(splited_line[0])

        # Return the list of null columns
        return null_columns



   # line =  ['airline,string,replace']
   # splited_line = ['airline','string','replace\n']

    def replace_null(self, df, column_list):

        '''This function replaces occurances of the words null,none or \\N with an empty string
         in each column specified in colummn list
         :param df: dataframe
         :param column list: column list specified the list of the column name
         :returns df: it returns  existing dataframe with the modification '''
        try:

            # Updated pattern to match 'null', 'none', or '\N'
            pattern = r'(null|none|\\N)'

            for column in column_list:
                # Update the DataFrame with replacements for each column
                df = df.withColumn(column, regexp_replace(df[column], pattern, ''))
        except Exception as e :
            raise Exception(f"ERROR : While running replace_null method Failed with an  exception {e}")
        return df

    def datatype(self, schema_base_path, source_name):
        try:
            struct_field_lst = []

            with open(fr'{schema_base_path}\{source_name}_schema.csv', 'r') as file:
                for line in file.readlines():
                    column_name = line.split(',')[0]
                    data_type = line.split(',')[1]

                    # Mapping data types to Spark SQL types
                    spark_data_type = None
                    if data_type.lower() == 'string':
                        spark_data_type = StringType()
                    elif data_type.lower() == 'integer':
                        spark_data_type = IntegerType()
                    elif data_type.lower()=='decimal':
                        spark_data_type = DecimalType()
                    # Add more elif conditions for other data types as needed

                    if spark_data_type:
                        struct_field_lst.append(StructField(column_name, spark_data_type))

            input_schema = StructType(struct_field_lst)

        except Exception as e:
            raise Exception(f"ERROR: Failed to generate schema. Exception: {e}")

        return input_schema





from pyspark.sql.window import Window

window_spec = Window.partitionBy(col('city')).orderBy(col('salary').desc)
df = df.withColumn('rank',rank().over(window_spec)).filter(col('rank')== 1 )
