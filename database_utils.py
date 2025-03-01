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
        
        self.execute_query(script)

def main():
    """
    Main method to demonstrate the usage of DuckDBWrapper class.
    """
    database_path = "example.duckdb"
    db_wrapper = DuckDBWrapper(database_path)
    db_wrapper.connect()
    db_wrapper.execute_script_from_file("create_database")
    try:
        db_wrapper.interactive_query()
    finally:
        db_wrapper.close()

if __name__ == "__main__":
    main()
