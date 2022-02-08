# This class is used to handle requests for data

__author__ = "William Gazeley"
__email__ = "william.gazeley@loratechai.com"

import yaml
import psycopg2


class DbConn:
    """
        This is an interface object for several functions that handle different db service types
        Used to query for various data without need to know what the underlying service providers are
        Allows for decoupling functional code and underlying DB tech/service

        Args:
            dataName (str): Name of the data to access
            *args: The arguments to be passed to the worker function
            **kwargs: The arguments to be passed to the worker function
    """

    def __init__(self, creds_file_path):
        """
        Initialises querier object using provided credentials yaml file. Holds a credentials file and a connection obj.

        Args:
            creds_file_path: Path to the credentials yaml file
        """
        # Load Creds
        try:
            with open(creds_file_path, 'r') as f:
                self.creds = yaml.safe_load(f)
        except Exception as e:
            raise Exception(e)

        service_type = self.creds['service_type']
        # Connect to DB
        if service_type == 'postgres':
            self.__postgresGetConnection()
        else:
            raise TypeError(__name__, "No such type")


    def __repr__(self):
        repString = "Interface for the following data:"
        for data in self.creds:
            repString += f"\n   - {data} <{self.creds[data]['service_type']}>"
        return(repString)


    def __call__(self, *args, **kwargs):
        """
        Interface function

        Args:
            dataName (str): Name of the data to access
            *args: The arguments to be passed to the worker function
            **kwargs: The arguments to be passed to the worker function
        """
        # Switch case
        if self.creds['service_type'] == 'postgres':
            return(self.__postgresCall(*args, **kwargs))
        elif False:
            # Other services go here
            pass
        else:
            # Add some logging here
            raise Exception(f"There is no valid service type '{self.creds['service_type']}'")


    def __postgresGetConnection(self):
        """
        Sets self.conn for postgres dbs

        Raises:
            Exception: Failure to connect
        """
        try:
            self.conn = psycopg2.connect(**self.creds["credentials"])
        except Exception as e:
            raise Exception(e)


    def __postgresCall(self, query, num_to_fetch=0):
        """
        Queries a postgres database and returns the results

        Args:
            query (str): SQL Query
            num_to_fetch (int, optional): Number of rows to fetch, fetch all if 0. Defaults to 0.

        Returns:
            list[tuple]: returned rows of SQL query
        """
        # Check that there was a query
        if query == None:
            raise Exception(__name__, "No query was passed")
            return

        # Reconnect if connection is closed
        if self.conn.closed:
            self.__postgresGetConnection()

        # Initialise output list
        sql_data = []

        # Query the DB
        try:
            # Create a cursor
            print("Creating a cursor obj..")
            cur = self.conn.cursor()

            # Execute the query
            print("Executing Query..")
            cur.execute(query)

            # Fetch first x results, all if x=0
            print("Fetch")
            if num_to_fetch == 0:
                sql_data = cur.fetchall()
            else:
                sql_data = cur.fetchmany(size=num_to_fetch)

        except Exception as e:
            raise Exception(e)

        finally:
            return sql_data
