from enum import Enum


class ErrorHandling(Enum):
    CONNECTION_ERROR = "Unable to Connect to the Database"
    ERROR_IN_RETURNING_QUERY = "Error Retrieving Data from the Database"
    RETURN_DATA_CSV_ERROR = "Error reading CSV data"
    RETURN_DATA_EXCEL_ERROR = "Error reading Excel data"
    RETURN_DATA_SQL_ERROR = "Error executing SQL query"
    RETURN_DATA_UNDEFINED_ERROR = "Unsupported input type"
    EXECUTE_QUERY_ERROR = "Error executing the query"
    Schema_RETRIEVAL_ERROR = "ERROR RETRIEVING SCHEMA  "
    PANDAS_FUNCTION_ERROR = " Pandas function operation failed"


class InputTypes(Enum):
    SQL = "SQL"
    CSV = "CSV"
    EXCEL = "xlsx"


class FileType(Enum):
    CSV = "CSV"
    SQL = "SQL"
    EXCEL = "xlsx"
