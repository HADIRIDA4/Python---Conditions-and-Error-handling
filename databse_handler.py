import psycopg2
import pandas as pd
from lookups import ErrorHandling, InputTypes, FileType
import os
import csv

## configuration parameters for database connection
config_param = {
    "database": "DVD_RENTAL",
    "host": "localhost",
    "port": 5432,
    "user": "postgres",
    "password": "fsd1234",
}


def create_connection():
    db_session = None
    try:
        db_session = psycopg2.connect(
            database=config_param["database"],
            user=config_param["user"],
            password=config_param["password"],
            host=config_param["host"],
            port=config_param["port"],
        )
    except Exception as e:
        error_message = f"{ErrorHandling.CONNECTION_ERROR.value} - {str(e)}"
        show_error_message(error_message)
        suggest_fix(e)
    finally:
        return db_session


def suggest_fix(exception):
    if "password authentication failed" in str(exception):
        print("Suggestion: Check the database password in the configuration.")
    elif "Connection refused" in str(exception):
        print("Suggestion: Check the database host and port in the configuration.")


def return_query(db_session, query):
    results = None
    try:
        cursor = db_session.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        db_session.commit()
    except Exception as e:
        error_message = f"{ErrorHandling.ERROR_IN_RETURNING_QUERY.value} - {str(e)}"
        show_error_message(error_message)
    finally:
        return results


# def return_query(db_session, query):
#     try:
#         with db_session.cursor() as cursor:
#             cursor.execute(query)
#             results = cursor.fetchall()
#         db_session.commit()
#         return results
#     except Exception as e:
#         error_message = f"{ErrorHandling.ERROR_IN_RETURNING_QUERY.value} - {str(e)}"
#         show_error_message(error_message)
#         return None


def return_data_as_df(file_executor, input_type, db_session=None):
    try:
        if input_type == InputTypes.CSV:
            return_dataframe = pd.read_csv(file_executor)
        elif input_type == InputTypes.EXCEL:
            return_dataframe = pd.read_excel(file_executor)
        elif input_type == InputTypes.SQL:
            if db_session is None:
                raise ValueError("db_session is required for SQL input type")
            return_dataframe = pd.read_sql_query(con=db_session, sql=file_executor)
        else:
            raise ValueError("The file type does not exist, please check main function")
        return return_dataframe
    except Exception as e:
        error_prefix = (
            ErrorHandling.RETURN_DATA_CSV_ERROR.value
            if input_type == InputTypes.CSV
            else ErrorHandling.RETURN_DATA_EXCEL_ERROR.value
            if input_type == InputTypes.EXCEL
            else ErrorHandling.RETURN_DATA_SQL_ERROR.value
            if input_type == InputTypes.SQL
            else ErrorHandling.RETURN_DATA_UNDEFINED_ERROR.value
        )
        show_error_message(error_prefix, str(e))
        return None


def execute_query(db_session, query):
    try:
        cursor = db_session.cursor()
        cursor.execute(query)
        db_session.commit()
    except Exception as e:
        error_prefix = ErrorHandling.EXECUTE_QUERY_ERROR.value
        show_error_message(error_prefix, str(e))


def show_schema_information(db_session):
    try:
        cursor = db_session.cursor()
        cursor.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
        )
        tables = cursor.fetchall()
        if tables:
            print("Tables in the 'public' schema:")
            for table in tables:
                print(table[0])
        else:
            print("No tables found in the 'public' schema.")
    except Exception as e:
        error_prefix = ErrorHandling.Schema_RETRIEVAL_ERROR.value

        show_error_message(error_prefix, str(e))


def handle_pandas_functions(dataframe, function_choice):
    try:
        if function_choice == "remove_duplicates":
            dataframe = dataframe.drop_duplicates()
        elif function_choice == "remove_nulls":
            dataframe = dataframe.dropna()
        elif function_choice == "get_blanks":
            blanks = dataframe.isnull().sum()
            print(blanks)
        elif function_choice == "get_shape":
            shape = dataframe.shape
            print(f"Number of rows: {shape[0]}, Number of columns: {shape[1]}")
        elif function_choice == "get_length":
            length = len(dataframe)
            print(f"Length of DataFrame: {length}")
        else:
            print("Invalid function choice.")
            return None
    except Exception as e:
        error_prefix = ErrorHandling.PANDAS_FUNCTION_ERROR.value
        show_error_message(f"{error_prefix} - {str(e)}")

    return dataframe


def list_files_by_type(folder_path, file_type):
    """Lists the filenames of files in a folder based on the specified file type."""
    valid_file_types = set(FileType)
    files_of_type = []

    try:
        for file in os.listdir(folder_path):
            _, extension = os.path.splitext(file)
            file_extension = extension[1:].upper()

            if file_extension == file_type and FileType[file_type] in valid_file_types:
                files_of_type.append(file)
    except OSError as e:
        print(f"Error while listing files in folder: {str(e)}")

    return files_of_type


def show_error_message(error_message, string):
    print(f"{error_message}:{string}")
