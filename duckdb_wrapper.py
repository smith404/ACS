import duckdb
from tabulate import tabulate
import os

class DuckDBWrapper:
    def __init__(self, database_path: str, auto_commit: bool = True):
        """
        Initialize the DuckDBWrapper with the path to the database file and auto_commit setting.
        """
        self.database_path = database_path
        self.connection = None
        self.auto_commit = auto_commit

    def connect(self):
        """
        Establish a connection to the DuckDB database.
        """
        self.connection = duckdb.connect(self.database_path)

    def execute_query(self, query: str):
        """
        Execute a SQL script on the connected DuckDB database and return the results.
        
        :param query: The SQL script to execute.
        :return: The results of the query.
        :raises ConnectionError: If the database is not connected.
        """
        if self.connection is None:
            raise ConnectionError("Database is not connected.")
        result = self.connection.execute(query).fetchall()
        if self.auto_commit:
            self.commit()
        return result

    def execute_select_query(self, query: str):
        """
        Execute a SELECT SQL query on the connected DuckDB database and return the results in a tabulated format.
        
        :param query: The SELECT SQL query to execute.
        :return: The results of the query in a tabulated format.
        :raises ConnectionError: If the database is not connected.
        :raises ValueError: If the query is not a SELECT query.
        """
        if self.connection is None:
            raise ConnectionError("Database is not connected.")
        if not query.strip().lower().startswith("select"):
            raise ValueError("Only SELECT queries are allowed.")
        result = self.connection.execute(query).fetchall()
        headers = [desc[0] for desc in self.connection.description]
        return tabulate(result, headers, tablefmt="grid")

    def execute_prepared_statement(self, query: str, params: list):
        """
        Execute a prepared statement with the given parameters on the connected DuckDB database.
        
        :param query: The SQL query to execute.
        :param params: The list of parameters to use in the query.
        :return: The results of the query.
        :raises ConnectionError: If the database is not connected.
        :raises ValueError: If the number of parameters does not match the query.
        """
        if self.connection is None:
            raise ConnectionError("Database is not connected.")
        
        # Check if the number of parameters matches the number of placeholders in the query
        placeholder_count = query.count('?')
        if placeholder_count != len(params):
            raise ValueError(f"Expected {placeholder_count} parameters, but got {len(params)}.")
        
        result = self.connection.execute(query, params).fetchall()
        if self.auto_commit:
            self.commit()
        return result

    def close(self):
        """
        Close the connection to the DuckDB database.
        """
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def begin_transaction(self):
        """
        Begin a new transaction.
        """
        if self.connection is None:
            raise ConnectionError("Database is not connected.")
        self.connection.begin()

    def commit(self):
        """
        Commit the current transaction.
        """
        if self.connection is None:
            raise ConnectionError("Database is not connected.")
        self.connection.commit()

    def rollback(self):
        """
        Rollback the current transaction.
        """
        if self.connection is None:
            raise ConnectionError("Database is not connected.")
        self.connection.rollback()

    def interactive_query(self):
        """
        Interactively take user input to execute either a SELECT or other query, then output the result.
        The method terminates if the user enters 'exit'.
        """
        while True:
            user_input = input("Enter SQL query (or 'exit' to quit): ").strip()
            if user_input.lower() == 'exit':
                print("Exiting interactive query mode.")
                break
            try:
                if (user_input.lower().startswith("select")):
                    result = self.execute_select_query(user_input)
                else:
                    result = self.execute_query(user_input)
                print(result)
            except Exception as e:
                print(f"Error: {e}")

    def execute_script_from_file(self, script_name: str, directory: str = "scripts"):
        """
        Execute a SQL script from a file located in the specified directory.
        
        :param script_name: The name of the script file to execute.
        :param directory: The directory where the script file is located. Defaults to 'scripts'.
        :raises FileNotFoundError: If the script file does not exist.
        :raises ConnectionError: If the database is not connected.
        """
        if not script_name.endswith(".sql"):
            script_name += ".sql"
        
        script_path = os.path.join(directory, script_name)
        if not os.path.isfile(script_path):
            raise FileNotFoundError(f"Script file '{script_path}' does not exist.")
        
        with open(script_path, 'r') as file:
            script = file.read()
        
        self.execute_query(script)

    def convert_csv_to_parquet(self, csv_file_path: str, parquet_file_path: str):
        """
        Convert a CSV file to a Parquet file using DuckDB.
        
        :param csv_file_path: The path to the input CSV file.
        :param parquet_file_path: The path to the output Parquet file.
        :raises FileNotFoundError: If the CSV file does not exist.
        :raises ConnectionError: If the database is not connected.
        """
        if not os.path.isfile(csv_file_path):
            raise FileNotFoundError(f"CSV file '{csv_file_path}' does not exist.")
        if self.connection is None:
            raise ConnectionError("Database is not connected.")
        
        query = f"COPY (SELECT * FROM read_csv_auto('{csv_file_path}')) TO '{parquet_file_path}' (FORMAT 'parquet');"
        self.execute_query(query)

    def load_parquet_to_table(self, parquet_file_path: str, table_name: str):
        """
        Load data from a Parquet file into a table after checking if the table exists and has the same structure.
        
        :param parquet_file_path: The path to the input Parquet file.
        :param table_name: The name of the table to load data into.
        :raises FileNotFoundError: If the Parquet file does not exist.
        :raises ConnectionError: If the database is not connected.
        :raises ValueError: If the table does not exist or the structures do not match.
        """
        if not os.path.isfile(parquet_file_path):
            raise FileNotFoundError(f"Parquet file '{parquet_file_path}' does not exist.")
        if self.connection is None:
            raise ConnectionError("Database is not connected.")
        
        # Check if the table exists
        table_exists_query = f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}';"
        table_exists_result = self.execute_query(table_exists_query)
        if table_exists_result[0][0] == 0:
            raise ValueError(f"Table '{table_name}' does not exist.")
        
        # Get the structure of the table
        table_structure_query = f"DESCRIBE {table_name};"
        table_structure = self.execute_query(table_structure_query)
        table_columns = [row[0] for row in table_structure]
        
        # Get the structure of the Parquet file
        parquet_structure_query = f"DESCRIBE SELECT * FROM read_parquet('{parquet_file_path}');"
        parquet_structure = self.execute_query(parquet_structure_query)
        parquet_columns = [row[0] for row in parquet_structure]
        
        # Check if the structures match
        if table_columns != parquet_columns:
            raise ValueError("The structure of the table and the Parquet file do not match.")
        
        # Load data from the Parquet file into the table
        load_data_query = f"COPY {table_name} FROM '{parquet_file_path}' (FORMAT 'parquet');"
        self.execute_query(load_data_query)

    def extract_table_to_csv(self, table: str, csv_file_path: str):
        """
        Execute a SELECT SQL query and save the table to a CSV file.
        
        :param table: The table to extract.
        :param csv_file_path: The path to the output CSV file.
        :raises ConnectionError: If the database is not connected.
        :raises ValueError: If the query is not a SELECT query.
        """
        self.extract_select_query_to_csv(f"SELECT * FROM {table}", csv_file_path)

    def extract_select_query_to_csv(self, query: str, csv_file_path: str):
        """
        Execute a SELECT SQL query and save the results to a CSV file.
        
        :param query: The SELECT SQL query to execute.
        :param csv_file_path: The path to the output CSV file.
        :raises ConnectionError: If the database is not connected.
        :raises ValueError: If the query is not a SELECT query.
        """
        if self.connection is None:
            raise ConnectionError("Database is not connected.")
        if not query.strip().lower().startswith("select"):
            raise ValueError("Only SELECT queries are allowed.")
        
        query = f"COPY ({query}) TO '{csv_file_path}' (FORMAT 'csv', HEADER);"
        self.execute_query(query)
