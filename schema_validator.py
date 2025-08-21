import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import StructType, StructField, StringType
from snowflake.snowpark.functions import col
import logging
import tempfile
import os
# Configure logging
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

# List of columns to exclude from comparison
EXCLUDED_COLUMNS = {
    "CREATED_AT",
    "UPDATED_AT",
    "CREATED_BY",
    "UPDATED_BY",
    "LOAD_TIMESTAMP"
}

def main(session: snowpark.Session):
    
    logger.info("Starting table and schema comparison process.")
    
    # Set up the database and schemas
    db1 = "source_database"
    schema1 = "source_schema"
    
    db2 = "target_database"
    schema2 = "target_schema"

    # Explicitly set the database context
    session.sql(f"USE DATABASE {db1}").collect()

    # Get list of tables in both schemas (Remove t_s_ prefix for tables in db2)
    logger.info(f"Fetching table list from {db1}.{schema1}.")
    table1_list = session.table(f"{db1}.information_schema.tables") \
                         .filter(col("TABLE_SCHEMA") == schema1) \
                         .select("TABLE_NAME")

    logger.info(f"Fetching table list from {db2}.{schema2}.")
    table2_list = session.table(f"{db2}.information_schema.tables") \
                         .filter(col("TABLE_SCHEMA") == schema2) \
                         .select("TABLE_NAME")

    # Convert table lists to sets for easier comparison
    table1_set = set(row["TABLE_NAME"] for row in table1_list.collect())
    table2_set = set(row["TABLE_NAME"] for row in table2_list.collect())

    # Log table counts
    logger.info(f"Table count in {db1}.{schema1}: {len(table1_set)}")
    logger.info(f"Table count in {db2}.{schema2}: {len(table2_set)}")

    # Find all unique tables (both matched and unmatched)
    all_tables = table1_set.union(table2_set)

    # Log additional and missing tables
    additional_tables_in_schema1 = table1_set - table2_set
    additional_tables_in_schema2 = table2_set - table1_set

    logger.info(f"Comparing the Tables in {db2}.{schema2} with {db1}.{schema1}")
    if additional_tables_in_schema1:
        logger.info(f"Additional tables in {db1}.{schema1}: {', '.join(sorted(additional_tables_in_schema1))}")
    else:
        logger.info(f"No additional tables found in {db1}.{schema1}.")

    if additional_tables_in_schema2:
        logger.info(f"Additional tables in {db2}.{schema2}: {', '.join(sorted(additional_tables_in_schema2))}")
    else:
        logger.info(f"No additional tables found in {db2}.{schema2}.")

    # Schema for the result DataFrame
    result_schema = StructType([ 
        StructField("SOURCE_TABLE_NAME", StringType()), 
        StructField("TARGET_TABLE_NAME", StringType()),
        StructField("STATUS_TABLE_NAME", StringType()),  # Status for table name
        StructField("SOURCE_COLUMN_NAME", StringType()), 
        StructField("TARGET_COLUMN_NAME", StringType()), 
        StructField("STATUS_COLUMN_NAME", StringType()),  # Status for column name
        StructField("SOURCE_DATA_TYPE", StringType()), 
        StructField("TARGET_DATA_TYPE", StringType()), 
        StructField("STATUS_DATA_TYPE", StringType()),  # Status for data type
        StructField("SOURCE_CHARACTER_MAXIMUM_LENGTH", StringType()),  
        StructField("TARGET_CHARACTER_MAXIMUM_LENGTH", StringType()),
        StructField("STATUS_CHARACTER_MAXIMUM_LENGTH", StringType()),  # Status for character length
        StructField("SOURCE_SCALE", StringType()), 
        StructField("TARGET_SCALE", StringType()), 
        StructField("STATUS_SCALE", StringType()),  # Status for scale
        StructField("SOURCE_PRECISION", StringType()),  
        StructField("TARGET_PRECISION", StringType()),
        StructField("STATUS_PRECISION", StringType())  # Status for precision
    ])
    
    # List to store comparison results
    results = []
    
    # Iterate through all tables (including unmatched ones)
    for table_name in sorted(all_tables):

        logger.info(f"Processing table: {table_name}")
        # Check if the table exists in schema1
        if table_name in table1_set:
            table1 = table_name
        else:
            table1 = None  # Table does not exist in schema1

        # Check if the table exists in schema2 (after removing prefix)
        if table_name in table2_set:
            # Find the actual table name with prefix in schema2
            table2_query = session.table(f"{db2}.information_schema.tables") \
                                 .filter(col("TABLE_SCHEMA") == schema2) \
                                 .filter(col("TABLE_NAME") == table_name) \
                                 .select("TABLE_NAME")
            
            table2_rows = table2_query.collect()
            if table2_rows:
                table2_name = table2_rows[0]["TABLE_NAME"]
        else:
            table2_name = None  # Table does not exist in schema2

        # Determine STATUS_TABLE_NAME
        if table1 != table2_name:
            status_table_name = "FAIL"
        else:
            status_table_name = "PASS"

        # Process columns for the table
        if table1 is not None:
            logger.info(f"Fetching columns for table {table1} in {db1}.{schema1}.")
            # Get columns from the first table
            table1_columns = session.table(f"{db1}.information_schema.columns") \
                                   .filter(col("TABLE_SCHEMA") == schema1) \
                                   .filter(col("TABLE_NAME") == table1) \
                                   .filter(~col("COLUMN_NAME").isin(EXCLUDED_COLUMNS)) \
                                   .select("COLUMN_NAME", 
                                           "DATA_TYPE", 
                                           "NUMERIC_SCALE", 
                                           "NUMERIC_PRECISION",
                                           "CHARACTER_MAXIMUM_LENGTH") \
                                   .collect()
            table1_columns_dict = {row["COLUMN_NAME"]: row for row in table1_columns}
        else:
            table1_columns_dict = {}  # No columns in schema1

        if table2_name is not None:
            logger.info(f"Fetching columns for table {table2_name} in {db2}.{schema2}.")
            # Get columns from the second table
            table2_columns = session.table(f"{db2}.information_schema.columns") \
                                   .filter(col("TABLE_SCHEMA") == schema2) \
                                   .filter(col("TABLE_NAME") == table2_name) \
                                   .filter(~col("COLUMN_NAME").isin(EXCLUDED_COLUMNS)) \
                                   .select("COLUMN_NAME", 
                                           "DATA_TYPE", 
                                           "NUMERIC_SCALE", 
                                           "NUMERIC_PRECISION",
                                           "CHARACTER_MAXIMUM_LENGTH") \
                                   .collect()
            table2_columns_dict = {row["COLUMN_NAME"]: row for row in table2_columns}
        else:
            table2_columns_dict = {}  # No columns in schema2

        # Get all unique column names from both tables
        all_columns = set(table1_columns_dict.keys()).union(table2_columns_dict.keys())

        # Collect comparison results
        for column_name in sorted(all_columns):
            # Check if the column exists in table1
            if column_name in table1_columns_dict:
                row1 = table1_columns_dict[column_name]
                data_type1 = row1["DATA_TYPE"] if row1["DATA_TYPE"] else "NULL"
                scale1 = str(row1["NUMERIC_SCALE"]) if row1["NUMERIC_SCALE"] is not None else "NULL"
                precision1 = str(row1["NUMERIC_PRECISION"]) if row1["NUMERIC_PRECISION"] is not None else "NULL"
                char_max_length1 = str(row1["CHARACTER_MAXIMUM_LENGTH"]) if row1["CHARACTER_MAXIMUM_LENGTH"] is not None else "NULL"
                table1_column_name = row1["COLUMN_NAME"]
            else:
                data_type1 = "NULL"
                scale1 = "NULL"
                precision1 = "NULL"
                char_max_length1 = "NULL"
                table1_column_name = "NULL"

            # Check if the column exists in table2
            if column_name in table2_columns_dict:
                row2 = table2_columns_dict[column_name]
                data_type2 = row2["DATA_TYPE"] if row2["DATA_TYPE"] else "NULL"
                scale2 = str(row2["NUMERIC_SCALE"]) if row2["NUMERIC_SCALE"] is not None else "NULL"
                precision2 = str(row2["NUMERIC_PRECISION"]) if row2["NUMERIC_PRECISION"] is not None else "NULL"
                char_max_length2 = str(row2["CHARACTER_MAXIMUM_LENGTH"]) if row2["CHARACTER_MAXIMUM_LENGTH"] is not None else "NULL"
                table2_column_name = row2["COLUMN_NAME"]
            else:
                data_type2 = "NULL"
                scale2 = "NULL"
                precision2 = "NULL"
                char_max_length2 = "NULL"
                table2_column_name = "NULL"

            # Determine STATUS_COLUMN_NAME
            if table1_column_name != "NULL" and table2_column_name != "NULL" :
                if table1_column_name == table2_column_name:
                    status_column_name = "PASS"
                else:
                    status_column_name = "FAIL"
            else:
                status_column_name = "FAIL"

            # Determine STATUS_DATA_TYPE
            if data_type1 != "NULL" and data_type2 != "NULL" :
                if data_type1 == data_type2:
                    status_data_type = "PASS"
                else:
                    status_data_type = "FAIL"
            else:
                status_data_type = "FAIL"

            # Determine STATUS_CHARACTER_MAXIMUM_LENGTH
            if char_max_length1 != "NULL" and char_max_length2 != "NULL":
                if char_max_length1 == char_max_length2:
                    status_char_max_length = "PASS"
                elif char_max_length1 == '16777216' and char_max_length2 == '8388607':
                    status_char_max_length = "PASS"
                else:
                    status_char_max_length = "FAIL"
            elif char_max_length1 == "NULL" and char_max_length2 == "NULL":
                status_char_max_length = "PASS"
            else:
                status_char_max_length = "FAIL"
      

            # Determine STATUS_SCALE
            if scale1 != "NULL" and scale2 != "NULL":
                if scale1 == scale2:
                    status_scale = "PASS"
                else:
                    status_scale = "FAIL"
            elif scale1 == "NULL" and scale2 == "NULL":
                status_scale = "PASS"    
            else:
                status_scale = "FAIL"

            # Determine STATUS_PRECISION
            if precision1 != "NULL" and precision2 != "NULL":
                if precision1 == precision2:
                    status_precision = "PASS"
                else:
                    status_precision = "FAIL"
            elif precision1 == "NULL" and precision2 == "NULL":
                status_precision = "PASS"
            else:
                status_precision = "FAIL"

            # Append result to the list
            results.append((
                table1 if table1 is not None else "NULL", 
                table2_name if table2_name is not None else "NULL", 
                status_table_name,  # STATUS_TABLE_NAME
                table1_column_name, 
                table2_column_name, 
                status_column_name,  # STATUS_COLUMN_NAME
                data_type1, data_type2, 
                status_data_type,  # STATUS_DATA_TYPE
                char_max_length1, char_max_length2, 
                status_char_max_length,  # STATUS_CHARACTER_MAXIMUM_LENGTH
                scale1, scale2, 
                status_scale,  # STATUS_SCALE
                precision1, precision2, 
                status_precision  # STATUS_PRECISION
            ))
    
    # Create a Snowpark DataFrame from the results
    logger.info("Creating result DataFrame.")
    result_df = session.create_dataframe(results, schema=result_schema)

    # Order the DataFrame by TABLE1_NAME for a structured comparison
    result_df = result_df.order_by("SOURCE_TABLE_NAME", "TARGET_TABLE_NAME")

   # Apply filtering with multiple conditions
    logger.info("Filtering results for mismatches.")
    filtered_df = result_df[
        (result_df['STATUS_TABLE_NAME'] == 'FAIL') |
        (result_df['STATUS_COLUMN_NAME'] == 'FAIL') |
        (result_df['STATUS_DATA_TYPE'] == 'FAIL') |
        (result_df['STATUS_CHARACTER_MAXIMUM_LENGTH'] == 'FAIL') |
        (result_df['STATUS_SCALE'] == 'FAIL') |
        (result_df['STATUS_PRECISION'] == 'FAIL')
        ]
    
    logger.info("Comparison process completed.")
    
    return filtered_df
