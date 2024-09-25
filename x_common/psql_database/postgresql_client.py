import os
import json

import sqlalchemy
from sqlalchemy import Table, Column, Integer, String, MetaData, exc, text, insert, inspect, update, select

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from google.cloud.sql.connector import Connector, IPTypes
from x_common.psql_database.schemas import get_schema, get_json_to_sql_column_mapping
import pg8000
from pg8000.exceptions import DatabaseError


from concurrent.futures import ThreadPoolExecutor
from sqlalchemy.dialects.postgresql import insert as pg_insert

import logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


# is_test_mode = False  #! test db_chema
# is_test_mode = True   #! dev  db_chema


class PostgresqlClient:
    def __init__(self, db_name, db_user, db_password, is_test_mode):
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password

        self.db_schema = self.set_db_schema(is_test_mode)
        self.engine = self.connect_with_gcp_connector(db_schema=self.db_schema)

    def set_db_schema(self, db_schema):
        if db_schema:
            db_schema = "test"
        else:
            db_schema = "dev"

        return db_schema

    def connect_with_gcp_connector(self, db_schema) -> sqlalchemy.engine.base.Engine:
        """
        Initializes a connection pool for a Cloud SQL instance of Postgres.

        Uses the Cloud SQL Python Connector package.

                Args:
        - db_schema:  set default schema for this connection
        """
        instance_connection_name = "alpha-signal-prototype:europe-west1:asignal-psql-dev"


        ip_type = IPTypes.PRIVATE if os.environ.get("PRIVATE_IP") else IPTypes.PUBLIC
        # initialize Cloud SQL Python Connector object
        connector = Connector()

        def getconn() -> pg8000.dbapi.Connection:
            conn: pg8000.dbapi.Connection = connector.connect(
                instance_connection_name,
                "pg8000",
                user=self.db_user,
                password=self.db_password,
                db=self.db_name,
                ip_type=ip_type,
            )
            return conn

        pool = sqlalchemy.create_engine(
            "postgresql+pg8000://",
            creator=getconn,
        )
        # ! Set the default schema for the session
        pool = pool.execution_options(schema_translate_map={None: db_schema})
        logger.info(f"Connected to db_schema: {db_schema}")
        return pool


    def create_schema_if_not_exists(self, schema_name):
        """
        Creates a schema in the database if it does not already exist.

        Args:
        - schema_name: The name of the schema to be created.
        """
        try:
            logger.info("Creating schema", schema_name)
            with self.engine.connect() as conn:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
        except exc.SQLAlchemyError as e:
            logger.info(f"An error occurred while creating the schema '{schema_name}': {e}")


    def create_table_if_not_exists(self, table, db_schema):
        """
        Creates a table in the database if it does not already exist.

        Args:
        - table: A SQLAlchemy Table object representing the table to be created.
        """
        table_name = table.name
        metadata = MetaData(schema=db_schema)

        # Reflect existing tables from the target database into the metadata
        metadata.reflect(bind=self.engine)

        # Check and create schema if it doesn't exist
        if table.schema:
            self.create_schema_if_not_exists(table.schema)

        if table_name not in metadata.tables:
            try:
                # Table does not exist, attempt to create it
                table.create(self.engine)
                logger.info(f"Created table '{table_name}' successfully.")
            except exc.SQLAlchemyError as e:
                logger.info("Error while trying to create table")
                #logger.info(e)
        else:
            logger.info(f"Table '{table_name}' already exists.")

        return table


    def insert_data_not_bulk(self, table, data_list, db_schema):
        """
        Inserts a list of data into the specified table.

        Args:
        - table: A SQLAlchemy Table object where the data will be inserted.
        - data_list: A list of dictionaries, where each dictionary represents a row of data to be inserted.

        The function inserts each dictionary from the list into the table and commits the transaction.
        If an error occurs during insertion, the transaction is rolled back and an error message is logger.infoed.
        """
        logger.info("Inserting data into table: one connection per row")

        with self.engine.connect() as conn:
            try:
                length_data = len(data_list)
                logger.info(f"elements of data: {length_data}")
                for i, data in enumerate(data_list):
                    logger.info(f"adding element {i}/{length_data}")
                    insert_statement = table.insert().values(**data)
                    conn.execute(insert_statement)
                    if db_schema:
                        if i >= 20:
                            break
                logger.info("#--insert_data_not_bulk--  commiting insert")
                conn.commit()  # Explicitly commit the transaction
            except Exception as e:
                logger.info(f"An error occurred: {e}")
                conn.rollback()  # Rollback in case of error


    def insert_data_bulk(self, table, data_list, overwrite, db_schema, update_on_conflict=False, no_underscore_id=False, chunk_size=1000, index_elements=["_id"]):
        """
        Performs a bulk insert of a list of data into the specified table in chunks.

        Args:
        - table: A SQLAlchemy Table object where the data will be inserted.
        - data_list: A list of dictionaries, where each dictionary represents a row of data to be inserted.
        - chunk_size: The number of records to insert per transaction. Default is 1000.
        """
        total_length = len(data_list)
        logger.info(f"Inserting into table number of elements: {total_length} using a chunk size of: {chunk_size}")
        ids = []
        inserted_count = 0
        rejected_count = 0


        with self.engine.connect() as conn:

            try:
                if overwrite:
                    # Truncate the table to remove all existing data
                    truncate_stmt = text(f"TRUNCATE TABLE {db_schema}.{table.name} RESTART IDENTITY CASCADE")
                    conn.execute(truncate_stmt)
                    logger.info(f"Table {db_schema}.{table.name} truncated.")

                if update_on_conflict:
                    for i in range(0, total_length, chunk_size):
                        chunk = data_list[i:i + chunk_size]
                        stmt = insert(table).values(
                            chunk
                        )

                        do_update_stmt = stmt.on_conflict_do_nothing()
                        ids_before = [row[0] for row in conn.execute(table.select()).fetchall()]
                        conn.execute(do_update_stmt)

                        ids_after = [row[0] for row in conn.execute(table.select()).fetchall()]

                        tried_to_insert_ids = [row['_id'] for row in chunk]

                        inserted_count = len(ids_after) - len(ids_before)
                        updated_count = len(tried_to_insert_ids) - inserted_count

                        # Calculate the number of inserted rows

                        conn.commit()  # Commit after each chunk
                        logger.info(
                            f"Chunk {i // chunk_size + 1}/{total_length // chunk_size + 1}: Updated {updated_count}, Inserted {inserted_count}")

                        logger.info(
                            f"Data inserted successfully. Total Inserted: {inserted_count}, Total Updated: {updated_count}")

                else:
                    if no_underscore_id:
                        ids = []  # List to store the IDs or None
                        total_length = len(data_list)
                        for i in range(0, total_length, chunk_size):
                            logger.info(
                                f"Chunk {i // chunk_size + 1}/{total_length // chunk_size + 1}")
                            chunk = data_list[i:i + chunk_size]
                            insert_stmt = insert(table).returning(table.c.id)
                            result = conn.execute(insert_stmt, chunk)
                            conn.commit()  # Commit after each insert
                            inserted_id = result.fetchone()[0]  # Get the inserted ID
                            ids.append(inserted_id)

                    else:
                        # Get the existing _IDs from the table
                        stmt = select(table.c._id)
                        result = conn.execute(stmt)
                        if result:
                            existing_ids = {row['_id'] for row in result.mappings()}
                        else:
                            existing_ids = None
                        # Filter out duplicates from data_list
                        if existing_ids:
                            unique_data = [d for d in data_list if d['_id'] not in existing_ids]
                        else:
                            unique_data = data_list

                        for i in range(0, len(unique_data), chunk_size):
                            chunk = unique_data[i:i + chunk_size]
                            insert_stmt = insert(table).on_conflict_do_nothing().returning(
                                table.c._id)

                            result = conn.execute(insert_stmt, chunk)
                            inserted_ids = result.fetchall()
                            inserted_chunk_count = len(inserted_ids)
                            rejected_chunk_count = len(chunk) - inserted_chunk_count

                            inserted_count += inserted_chunk_count
                            rejected_count += rejected_chunk_count

                            conn.commit()  # Commit after each chunk
                            logger.info(
                                f"Chunk {i // chunk_size + 1}/{total_length // chunk_size + 1}: Inserted {inserted_chunk_count}, Rejected {rejected_chunk_count}")

                            logger.info(
                                f"Data inserted successfully. Total Inserted: {inserted_count}, Total Rejected: {rejected_count}")

            except SQLAlchemyError as e:
                conn.rollback()  # Rollback in case of error
                logger.info(f"An error occurred: {e}")
                logger.error(f"#--insert_data_bulk-- An error occurred: {e}")
                raise

            if ids:
                return ids, inserted_count, rejected_count
            else:
                return None, inserted_count, rejected_count


    def read_from_table_with_json_values(self, table_name, where_condition, db_schema):
        with self.engine.connect() as connection:

            logger.info(f"start: reading data from table: {table_name}")

            # Wrap the query string in `text()` to create an executable object
            query = text(f"SELECT * FROM \"{db_schema}\".\"{table_name}\" {where_condition};")
            # Execute the query and fetch the result
            result = connection.execute(query)
            # Fetch column names from the result
            column_names = result.keys()
            # Initialize an empty list to store the data
            data = []

            # Convert the rows to a list of dicts using column names
            for row in result:
                row_dict = {}
                for column, value in zip(column_names, row):
                    # Check if the column value is a string that could be JSON
                    if isinstance(value, str):
                        try:
                            # Attempt to parse the string as JSON
                            row_dict[column] = json.loads(value)
                        except json.JSONDecodeError:
                            # If parsing fails, keep the original string
                            row_dict[column] = value
                    else:
                        # If the value is not a string, add it as is
                        row_dict[column] = value
                data.append(row_dict)
            logger.info(f"finished: reading data from table: {table_name}")
            return data


    def read_column_from_table_as_list(self, table_name, column_name, where_condition, is_test_mode=False):
        # set up db_schema
        db_schema = self.set_db_schema(is_test_mode)

        with self.engine.connect() as connection:
            logger.info(f"reading data from table: {db_schema}.{table_name} to check for duplicates")

            query = text(f"SELECT {column_name} FROM {db_schema}.{table_name} {where_condition};")
            result = connection.execute(query)
            # Fetch all rows from the result set
            data = [row[0] for row in result.fetchall()]
            return data

    def read_columns_from_table_as_list(self, table_name, column_names, where_condition, is_test_mode=False):
        # set up db_schema
        db_schema = self.set_db_schema(is_test_mode)

        with self.engine.connect() as connection:
            logger.info(f"reading data from table: {table_name} to check for duplicates")

            # Join the column names with a comma for the SELECT query
            columns = ', '.join(column_names)
            query = text(f"SELECT {columns} FROM {db_schema}.{table_name} {where_condition};")
            result = connection.execute(query)
            # Fetch all rows from the result set
            data = [tuple(row) for row in result.fetchall()]
            return data



    def read_from_table(self, table_name, where_condition, db_schema, column_names=None):
        with self.engine.connect() as connection:
            logger.info(f"reading data from table {table_name}")

            # Wrap the query string in `text()` to create an executable object
            if not column_names:
                query = text(f"SELECT * FROM \"{db_schema}\".\"{table_name}\" {where_condition};")
            else:
                columns = ', '.join(column_names)
                logger.info(f"reading columns {columns}")
                query = text(f"SELECT {columns} FROM \"{db_schema}\".\"{table_name}\" {where_condition};")

            # Execute the query and fetch the result
            result = connection.execute(query)
            # Fetch column names from the result
            column_names = result.keys()
            # Convert the rows to a list of dicts using column names
            data = [{column: value for column, value in zip(column_names, row)} for row in result]
            return data


    def rename_keys_in_list_of_dicts(self, data_list, mapping_dict, revert=False):
        """
        Rename the keys in a list of JSON objects based on a provided mapping dictionary.

        :param list_of_json_objs: List of JSON objects with the original keys.
        :param mapping_dict: A dictionary where keys are the original JSON keys and values are the new keys.
        :return: A list of new JSON objects with the keys renamed.
        """
        if revert:
            mapping_dict = {v: k for k, v in mapping_dict.items()}
        renamed_list = []
        for json_obj in data_list:
            renamed_json = {}
            for old_key, new_key in mapping_dict.items():
                if old_key in json_obj:
                    renamed_json[new_key] = json_obj[old_key]
            renamed_list.append(renamed_json)
        return renamed_list


    # Example usa
    def write_list_into_postgresql(self, data_list, table_name, is_test_mode=False, rename_columns=True,
                                   overwrite=False, update_on_conflict=False, no_underscore_id=False, index_elements=["_id"]):
        """
        Writes a list of data into a PostgreSQL table.

        This function connects to a PostgreSQL database, creates the specified table (if it does not exist),
        and then inserts the provided list of data into the table in bulk.

        Args:
        - data_list: A list of dictionaries to be inserted into the table, with keys matching the table's column names.
        """
        # set up db_schema
        logger.info("")
        logger.info("### Starting psql connection ###")
        db_schema = self.set_db_schema(is_test_mode)

        # Getting schema and mapping table
        table_schema = get_schema(table_name)
        mapping_dict = get_json_to_sql_column_mapping(table_name)

        # Create the table if it doesn't exist
        table = self.create_table_if_not_exists(table_schema, db_schema)

        # preparing data: put into sql schema
        if rename_columns:
            final_data = self.rename_keys_in_list_of_dicts(data_list, mapping_dict)
        else:
            final_data = data_list

        # Insert data into the table
        ids, inserted_count, rejected_count = self.insert_data_bulk(table, final_data, overwrite, db_schema, update_on_conflict,  no_underscore_id, index_elements=index_elements)
        logger.info("### Ending psql connection ###")
        logger.info("")

        return ids, inserted_count, rejected_count


    def read_from_psql_and_return_json_list(self, table_name, is_test_mode=True, where_condition="",
                                            table_with_json_data=False, rename_columns=True, column_names=None):
        # set up db_schema
        logger.info("")
        logger.info("### Starting psql connection ###")
        db_schema = self.set_db_schema(is_test_mode)

        if table_with_json_data:
            data_list = self.read_from_table_with_json_values(table_name, where_condition, db_schema)
        else:
            data_list = self.read_from_table(table_name, where_condition, db_schema, column_names)
        if rename_columns:
            mapping_dict = get_json_to_sql_column_mapping(table_name)
            final_data = self.rename_keys_in_list_of_dicts(data_list, mapping_dict, revert=True)
        else:
            final_data = data_list
        logger.info("### Ending psql connection ###")
        logger.info("")
        return final_data


    def update_table(self, table_name, column_name, new_value, id_list, where_condition_table, is_test_mode=True):
        logger.info(f"Updating column: {column_name} in table: {table_name}")
        db_schema = self.set_db_schema(is_test_mode)

        # Reflect the table
        metadata = MetaData(schema=db_schema)
        metadata.reflect(self.engine, schema=db_schema)  #! reflect the tables with schema.
        table = Table(table_name, metadata, autoload_with=self.engine, schema=db_schema)
        # Define the new values
        new_values = {column_name: new_value}
        try:
            stmt = (
                update(table)
                .where(getattr(table.c, where_condition_table).in_(id_list))
                .values(new_values)
            )
            with self.engine.connect() as connection:
                result = connection.execute(stmt)
                connection.commit()
            logger.info(f"Expected to update {len(id_list)} rows")
            logger.info(f"Actually updated {result.rowcount} rows")
        except Exception as e:
            logger.error(f"An error occurred while updating the table: {e}")
        connection.close()


    def update_table_id_value_pairs(self, table_name, column_name, id_value_pairs, where_condition_column, is_test_mode=True):
        logger.info(f"Updating column: {column_name} in table: {table_name}")
        db_schema = self.set_db_schema(is_test_mode)

        metadata = MetaData(schema=db_schema)
        metadata.reflect(self.engine, schema=db_schema)
        table = Table(table_name, metadata, autoload_with=self.engine, schema=db_schema)
        try:
            updated_rows = 0
            with self.engine.connect() as connection:
                for id_value, new_value in id_value_pairs:
                    stmt = (
                        update(table)
                        .where(getattr(table.c, where_condition_column) == id_value)
                        .values({column_name: new_value})
                    )
                    result = connection.execute(stmt)
                    updated_rows += result.rowcount
                connection.commit()

            logger.info(f"Expected to update {len(id_value_pairs)} rows")
            logger.info(f"Actually updated {updated_rows} rows")
        except Exception as e:
            logger.error(f"An error occurred while updating the table: {e}")
        connection.close()

    def update_table_many_columns_in_chunks(self, table_name, id_value_pairs, where_condition_column, chunk_size=100,
                                            is_test_mode=True):
        logger.info(f"Updating table: {table_name}")
        logger.info(f"Number of rows to update: {len(id_value_pairs)}")
        db_schema = self.set_db_schema(is_test_mode)

        metadata = MetaData(schema=db_schema)
        metadata.reflect(self.engine, schema=db_schema)
        table = Table(table_name, metadata, autoload_with=self.engine, schema=db_schema)

        try:
            with self.engine.connect() as connection:
                # Split id_value_pairs into chunks
                #chunks = [id_value_pairs[i:i + chunk_size] for i in range(0, len(id_value_pairs), chunk_size)]
                total_length = len(id_value_pairs)
                for i in range(0, total_length, chunk_size):
                    chunk = id_value_pairs[i:i + chunk_size]
                    logger.info(
                        f"Chunk {i // chunk_size + 1}/{total_length // chunk_size + 1}")
                    transaction = connection.begin()
                    updated_rows = 0
                    for id_value, updates in chunk:
                        stmt = (
                            update(table)
                            .where(getattr(table.c, where_condition_column) == id_value)
                            .values(updates)  # 'updates' is a dict of column names and their new values
                        )
                        result = connection.execute(stmt)
                        updated_rows += result.rowcount
                    transaction.commit()
        except Exception as e:
            logger.error(f"An error occurred while updating the table: {e}")
            raise


    def update_table_with_condition(self, table_name, column_name, new_value, where_condition, is_test_mode=True):
        logger.info(f"Updating column: {column_name} in table: {table_name}")
        db_schema = self.set_db_schema(is_test_mode)

        metadata = MetaData(schema=db_schema)
        metadata.reflect(self.engine, schema=db_schema)  
        table = Table(table_name, metadata, autoload_with=self.engine, schema=db_schema)

        try:
            sql_query = text(f"UPDATE {table} SET {column_name} = {new_value} {where_condition}")
            with self.engine.connect() as conn:
                result = conn.execute(sql_query)
                conn.commit()
            logger.info(f"Actually updated {result.rowcount} rows")

        except Exception as e:
            logger.error(f"An error occurred while updating the table: {e}")



    def update_with_thread_pool(self, table_name, data_list, is_test_mode=True):
        """
        Updates a table in the database with a list of dictionaries using a thread pool.
            For big, high memory cost updates,  using a thread pool to update the table in parallel.

        table_name: str                 - name of the table to update
        data_list: list of dicts        - list of dictionaries with the data to update

        default variables:
        index_elements=['_id']          - list of columns to check for conflicts
        """
        try:
            logger.info(f"Updating: {table_name}, rows to update: {len(data_list)}")
            db_schema = self.set_db_schema(is_test_mode)
            
            metadata = MetaData(schema=db_schema)
            metadata.reflect(self.engine, schema=db_schema) 
            table = Table(table_name, metadata, autoload_with=self.engine, schema=db_schema)
            chunk_size = 1
            updated_rows = 0

            def update_chunk(chunk):
                nonlocal updated_rows
                with self.engine.begin() as connection:
                    stmt = pg_insert(table).values(chunk)
                    do_update_stmt = stmt.on_conflict_do_update(
                        index_elements=['_id'],
                        set_={col: getattr(stmt.excluded, col) for col in chunk[0].keys()}
                    )
                    result = connection.execute(do_update_stmt)
                    updated_rows += result.rowcount

            with ThreadPoolExecutor(max_workers=5) as executor:
                for i in range(0, len(data_list), chunk_size):
                    chunk = data_list[i:i + chunk_size]
                    executor.submit(update_chunk, chunk)

            logger.info(f"Updated rows: {updated_rows}")

        except Exception as e:
            logger.error(f"An error occurred while updating: {e}")

    def connect_tcp_socket_from_local(self, db_schema) -> sqlalchemy.engine.base.Engine:
        """Initializes a TCP connection pool for a Cloud SQL instance of Postgres.

            Args:
        - db_schema:  set default schema for this connection
        """
        db_host = "127.0.0.1"
        db_port = "5432"
        db_name = self.db_user
        db_user = self.db_user

        pool = sqlalchemy.create_engine(
            sqlalchemy.engine.url.URL.create(
                drivername="postgresql+pg8000",
                username=db_user,
                host=db_host,
                port=db_port,
                database=db_name,
            ),
        )
        # ! Set the default schema for the session
        pool = pool.execution_options(schema_translate_map={None: db_schema})
        logger.info(f"Connected to db_schema: {db_schema}")

        return pool

