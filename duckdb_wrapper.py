import duckdb
from tabulate import tabulate
import os
import json  # Add import for json
import glob  # Add import for glob

class DuckDBWrapper:
    DATABASE_NOT_CONNECTED_ERROR = "Database is not connected."
    ONLY_SELECT_QUERIES_ALLOWED_ERROR = "Only SELECT queries are allowed."
    PARQUET_EXTENSION = ".parquet"
    PARQUET_WILDCARD = "*.parquet"

    def __init__(self, database_path: str, auto_commit: bool = True):
        """
        Initialize the DuckDBWrapper with the path to the database file and auto_commit setting.
        """
        self.database_path = database_path
        self.connection = None
        self.auto_commit = auto_commit

    def check_parquet_file(self, parquet_file_path: str):
        """
        Check if the Parquet file exists.
        
        :param parquet_file_path: The path to the Parquet file.
        :raises FileNotFoundError: If the Parquet file does not exist.
        """
        # Check if the path ends with '.parquet'
        if not parquet_file_path.endswith(self.PARQUET_EXTENSION):
            parquet_file_path += self.PARQUET_EXTENSION
        
        # Check if the path is a directory
        if os.path.isdir(parquet_file_path):
            parquet_file_path = os.path.join(parquet_file_path, self.PARQUET_WILDCARD)
        
        # Check if the path is a wildcard
        if '*' in parquet_file_path or '?' in parquet_file_path:
            parquet_files = glob.glob(parquet_file_path)
            if not parquet_files:
                raise FileNotFoundError(f"No Parquet files match the pattern '{parquet_file_path}'.")
        elif not os.path.isfile(parquet_file_path):
            raise FileNotFoundError(f"Parquet file '{parquet_file_path}' does not exist.")
        
        return parquet_file_path

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
            raise ConnectionError(self.DATABASE_NOT_CONNECTED_ERROR)
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
            raise ConnectionError(self.DATABASE_NOT_CONNECTED_ERROR)
        if not query.strip().lower().startswith("select"):
            raise ValueError(self.ONLY_SELECT_QUERIES_ALLOWED_ERROR)
        result = self.connection.execute(query).fetchall()
        headers = [desc[0] for desc in self.connection.description]
        return tabulate(result, headers, tablefmt="grid")

    def execute_select_query_to_json(self, query: str):
        """
        Execute a SELECT SQL query on the connected DuckDB database and return the results as a JSON list.
        
        :param query: The SELECT SQL query to execute.
        :return: The results of the query as a JSON list.
        :raises ConnectionError: If the database is not connected.
        :raises ValueError: If the query is not a SELECT query.
        """
        if self.connection is None:
            raise ConnectionError(self.DATABASE_NOT_CONNECTED_ERROR)
        if not query.strip().lower().startswith("select"):
            raise ValueError(self.ONLY_SELECT_QUERIES_ALLOWED_ERROR)
        
        result = self.connection.execute(query).fetchall()
        headers = [desc[0] for desc in self.connection.description]
        json_result = [dict(zip(headers, row)) for row in result]
        return json.dumps(json_result)

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
            raise ConnectionError(self.DATABASE_NOT_CONNECTED_ERROR)
        
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
            raise ConnectionError(self.DATABASE_NOT_CONNECTED_ERROR)
        self.connection.begin()

    def commit(self):
        """
        Commit the current transaction.
        """
        if self.connection is None:
            raise ConnectionError(self.DATABASE_NOT_CONNECTED_ERROR)
        self.connection.commit()

    def rollback(self):
        """
        Rollback the current transaction.
        """
        if self.connection is None:
            raise ConnectionError(self.DATABASE_NOT_CONNECTED_ERROR)
        self.connection.rollback()

    def interactive_query(self):
        """
        Interactively take user input to execute either a SELECT or other query, then output the result.
        The method terminates if the user enters 'exit' or 'quit'.
        """
        while True:
            user_input = input("Enter SQL query (or 'exit' to quit): ").strip()
            if user_input.lower() in ['exit', 'quit']:
                print("Exiting interactive query mode.")
                break
            try:
                if user_input.lower().startswith("!exe"):
                    tokens = user_input.split(maxsplit=2)
                    query_name = tokens[1].strip()
                    parameters = json.loads(tokens[2]) if len(tokens) > 2 else None
                    print(query_name, parameters)
                    user_input = self.read_named_query(query_name, parameters=parameters)
                if user_input.lower().startswith("select"):
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
        
        if self.connection is None:
            raise ConnectionError(self.DATABASE_NOT_CONNECTED_ERROR)
        
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
            raise ConnectionError(self.DATABASE_NOT_CONNECTED_ERROR)
        
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

        # Check if the Parquet file exists
        parquet_file_path = self.check_parquet_file(parquet_file_path)

        if self.connection is None:
            raise ConnectionError(self.DATABASE_NOT_CONNECTED_ERROR)
        
        # Split the table name if it contains a period
        schema_name, table_name = table_name.split('.') if '.' in table_name else (None, table_name)
        
        # Check if the table exists
        table_exists_query = f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
        if schema_name:
            table_exists_query += f" AND table_schema = '{schema_name}'"
        table_exists_query += ";"
        
        table_exists_result = self.execute_query(table_exists_query)
        if table_exists_result[0][0] == 0:
            raise ValueError(f"Table '{table_name}' does not exist.")
        
        # Get the structure of the table
        table_structure_query = f"DESCRIBE {table_name};"
        if schema_name:
            table_structure_query = f"DESCRIBE {schema_name}.{table_name};"
        
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
        if schema_name:
            load_data_query = f"COPY {schema_name}.{table_name} FROM '{parquet_file_path}' (FORMAT 'parquet');"
        
        self.execute_query(load_data_query)

    def extract_table_to_csv(self, table: str, csv_file_path: str, delimiter: str = ";"):
        """
        Execute a SELECT SQL query and save the table to a CSV file.
        
        :param table: The table to extract.
        :param csv_file_path: The path to the output CSV file.
        :param delimiter: The delimiter to use in the CSV file.
        :raises ConnectionError: If the database is not connected.
        :raises ValueError: If the query is not a SELECT query.
        """
        self.extract_select_query_to_csv(f"SELECT * FROM {table}", csv_file_path, delimiter)

    def extract_select_query_to_csv(self, query: str, csv_file_path: str, delimiter: str = ";"):
        """
        Execute a SELECT SQL query and save the results to a CSV file.
        
        :param query: The SELECT SQL query to execute.
        :param csv_file_path: The path to the output CSV file.
        :param delimiter: The delimiter to use in the CSV file.
        :raises ConnectionError: If the database is not connected.
        :raises ValueError: If the query is not a SELECT query.
        """
        if self.connection is None:
            raise ConnectionError(self.DATABASE_NOT_CONNECTED_ERROR)
        if not query.strip().lower().startswith("select"):
            raise ValueError(self.ONLY_SELECT_QUERIES_ALLOWED_ERROR)
        
        query = f"COPY ({query}) TO '{csv_file_path}' (FORMAT 'csv', DELIMITER '{delimiter}', HEADER);"
        self.execute_query(query)

    def dump_tables(self, schema_name: str, output_directory: str = None, delimiter: str = ";"):
        """
        Export all tables in the specified schema to CSV files in the given directory.
        
        :param schema_name: The name of the schema containing the tables to export.
        :param output_directory: The directory to save the CSV files. Defaults to the schema name.
        :raises ConnectionError: If the database is not connected.
        """
        if self.connection is None:
            raise ConnectionError(self.DATABASE_NOT_CONNECTED_ERROR)
        
        if output_directory is None:
            output_directory = schema_name
        
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
        
        # Get the list of tables in the schema
        tables_query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_name}';"
        tables = self.execute_query(tables_query)
        
        for table in tables:
            table_name = table[0]
            csv_file_path = os.path.join(output_directory, f"{table_name}.csv")
            self.extract_table_to_csv(f"{schema_name}.{table_name}", csv_file_path, delimiter)

    def load_csv_files(self, schema_name: str, input_directory: str = None, truncate: bool = False, delimiter: str = ";"):
        """
        Load CSV files from the specified directory into the database, ensuring foreign key dependencies are handled.
        
        :param schema_name: The schema where the tables should be loaded.
        :param input_directory: The directory containing the CSV files. Defaults to the schema name.
        :param truncate: Whether to truncate the tables before loading the data. Defaults to False.
        :raises ConnectionError: If the database is not connected.
        :raises FileNotFoundError: If the directory does not exist.
        """
        if self.connection is None:
            raise ConnectionError(self.DATABASE_NOT_CONNECTED_ERROR)
        
        if input_directory is None:
            input_directory = schema_name

        if not os.path.exists(input_directory):
            raise FileNotFoundError(f"Directory '{input_directory}' does not exist.")
        
        # Get the list of CSV files in the directory and sort them alphabetically
        csv_files = sorted([f for f in os.listdir(input_directory) if f.endswith('.csv')])
        
        # Load the tables in the sorted order
        for csv_file in csv_files:
            # Remove leading numbers followed by an underscore from the file name
            table_name = os.path.splitext(csv_file)[0]
            table_name = table_name.split('_', 1)[-1] if table_name[0].isdigit() else table_name
            if truncate:
                truncate_query = f"TRUNCATE TABLE {schema_name}.{table_name};"
                self.execute_query(truncate_query)
            load_query = f"COPY {schema_name}.{table_name} FROM '{os.path.join(input_directory, csv_file)}' (FORMAT 'csv', DELIMITER '{delimiter}', HEADER);"
            self.execute_query(load_query)

    def execute_named_query_to_json(self, query_name: str, directory: str = "queries", parameters: dict = None):
        """
        Execute a SQL script from a file located in the specified directory and return the result in JSON format.
        
        :param query_name: The name of the query file to execute.
        :param directory: The directory where the query file is located. Defaults to 'scripts'.
        :param parameters: A dictionary of optional parameters to use in query string formatting.
        :return: The results of the query as a JSON list.
        :raises FileNotFoundError: If the query file does not exist.
        :raises ConnectionError: If the database is not connected.
        """
        if not query_name.endswith(".sql"):
            query_name += ".sql"
        
        query_path = os.path.join(directory, query_name)
        if not os.path.isfile(query_path):
            raise FileNotFoundError(f"Query file '{query_path}' does not exist.")
        
        with open(query_path, 'r') as file:
            query = file.read()
        
        if parameters:
            query = query.format(**parameters)

        if self.connection is None:
            raise ConnectionError(self.DATABASE_NOT_CONNECTED_ERROR)

        return self.execute_select_query_to_json(query)

    def read_named_query(self, query_name: str, directory: str = "queries", parameters: dict = None):
        """
        Execute a SQL script from a file located in the specified directory and return the result in JSON format.
        
        :param query_name: The name of the query file to execute.
        :param directory: The directory where the query file is located. Defaults to 'scripts'.
        :param parameters: A dictionary of optional parameters to use in query string formatting.
        :return: The expanded query.
        :raises FileNotFoundError: If the query file does not exist.
        :raises ConnectionError: If the database is not connected.
        """
        if not query_name.endswith(".sql"):
            query_name += ".sql"
        
        query_path = os.path.join(directory, query_name)
        if not os.path.isfile(query_path):
            raise FileNotFoundError(f"Query file '{query_path}' does not exist.")
        
        with open(query_path, 'r') as file:
            query = file.read()
        
        if parameters:
            query = query.format(**parameters)

        return query

    def describe_parquet_file(self, parquet_file_path: str, n: int = 5):
        """
        Describe the structure of a Parquet file and print the first n rows in a tabulated format.
        
        :param parquet_file_path: The path to the Parquet file.
        :param n: The number of rows to display. Defaults to 5.
        :raises FileNotFoundError: If the Parquet file does not exist.
        :raises ConnectionError: If the database is not connected.
        """

        # Check if the Parquet file exists
        parquet_file_path = self.check_parquet_file(parquet_file_path)

        if self.connection is None:
            raise ConnectionError(self.DATABASE_NOT_CONNECTED_ERROR)

        # Describe the structure of the Parquet file
        describe_query = f"DESCRIBE SELECT * FROM read_parquet('{parquet_file_path}');"
        structure = self.execute_query(describe_query)
        print("Table Structure:")
        print(tabulate(structure, headers=["Column", "Type"], tablefmt="grid"))
        
        # Fetch the first n rows
        select_query = f"SELECT * FROM read_parquet('{parquet_file_path}') LIMIT {n};"
        rows = self.execute_query(select_query)
        headers = [desc[0] for desc in self.connection.description]
        print(f"\nFirst {n} rows:")
        print(tabulate(rows, headers, tablefmt="grid"))

