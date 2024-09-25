import os
import logging
from sqlalchemy import create_engine, text, MetaData
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine.url import URL
from google.cloud.sql.connector import Connector, IPTypes
import pg8000
from sqlalchemy.dialects.postgresql import insert
from data_storage.schemas import get_schema
from datetime import datetime
from sqlalchemy import inspect  

class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class PostgresqlClient(metaclass=Singleton):
    def __init__(self, db_name, db_user, db_password, is_test_mode=False, use_local=True):
        self.logger = logging.getLogger(__name__)
        
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.is_test_mode = is_test_mode
        self.db_schema = "test" if is_test_mode else "dev"
        self.use_local = use_local
        self.engine = self.connect_with_local() if use_local else self.connect_with_gcp_connector()
        self.metadata = MetaData(schema=self.db_schema)
        self.tables = {}





    def connect_with_local(self):
        try:
            db_url = URL.create(
                drivername="postgresql",
                username=self.db_user,
                password=self.db_password,
                host="localhost",
                port=5432,  
                database=self.db_name
            )
            self.logger.info(f"Attempting to connect to: {db_url}")
            engine = create_engine(db_url)
            engine = engine.execution_options(schema_translate_map={None: self.db_schema})
            
            # Test the connection
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                self.logger.info(f"Connection test result: {result.fetchone()}")
            
            self.logger.info(f"Connected to local PostgreSQL database: {self.db_name}, schema: {self.db_schema}")
            return engine
        except Exception as e:
            self.logger.error(f"Failed to connect to PostgreSQL: {str(e)}")
            raise
    



    def connect_with_gcp_connector(self):
        instance_connection_name = "alpha-signal-prototype:europe-west1:asignal-psql-dev"
        ip_type = IPTypes.PRIVATE if os.environ.get("PRIVATE_IP") else IPTypes.PUBLIC
        connector = Connector()

        def getconn():
            return connector.connect(
                instance_connection_name,
                "pg8000",
                user=self.db_user,
                password=self.db_password,
                db=self.db_name,
                ip_type=ip_type,
            )

        engine = create_engine(
            "postgresql+pg8000://",
            creator=getconn,
        )
        engine = engine.execution_options(schema_translate_map={None: self.db_schema})
        self.logger.info(f"Connected to db_schema: {self.db_schema}")
        return engine








    def create_schema_if_not_exists(self):
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.db_schema}"))
                self.logger.info(f"Schema '{self.db_schema}' created or already exists.")
                
        except SQLAlchemyError as e:
            self.logger.error(f"Error creating schema '{self.db_schema}': {e}")
            raise  # Reraise the exception to handle it upstream

    def get_or_create_table(self, table_name):
        if table_name not in self.tables:
            table = get_schema(table_name=table_name)
            if table is None:  # Check if the schema was not found
                raise ValueError(f"No schema defined for table '{table_name}'")
            
            table.schema = self.db_schema
            self.create_schema_if_not_exists()
            
            # Check if the table exists in the database using inspect
            inspector = inspect(self.engine)  # Create an inspector object
            if not inspector.has_table(table_name, schema=self.db_schema):
                try:
                    table.create(self.engine)
                    self.logger.info(f"Table '{table_name}' created in schema '{self.db_schema}'")
                except SQLAlchemyError as e:
                    self.logger.error(f"Error creating table '{table_name}': {e}")
                    raise  # Reraise the exception to handle it upstream
            else:
                self.logger.info(f"Table '{table_name}' already exists in schema '{self.db_schema}'")
            
            self.tables[table_name] = table
        
        return self.tables[table_name]







    def insert_data(self, table_name, data_list, chunk_size=1000):
        table = self.get_or_create_table(table_name)
        total_length = len(data_list)
        inserted_count = 0
        
        with self.engine.connect() as conn:
            try:
                for i in range(0, total_length, chunk_size):
                    chunk = data_list[i:i + chunk_size]
                    stmt = insert(table).values(chunk).on_conflict_do_nothing()
                    result = conn.execute(stmt)
                    inserted_count += result.rowcount
                    conn.commit()
                    self.logger.info(f"Inserted chunk {i // chunk_size + 1}/{(total_length - 1) // chunk_size + 1}: {result.rowcount} rows")
                
                self.logger.info(f"Total inserted: {inserted_count} rows")
                return inserted_count
            
            
            except SQLAlchemyError as e:
                conn.rollback()
                self.logger.error(f"Error inserting data into '{table_name}': {e}")
                raise



    def stream_data(self, table_name, data_generator, chunk_size=1000):
        table = self.get_or_create_table(table_name)
        inserted_count = 0
        chunk = []

        with self.engine.connect() as conn:
            try:
                for item in data_generator:
                    chunk.append(item)
                    if len(chunk) >= chunk_size:
                        stmt = insert(table).values(chunk).on_conflict_do_nothing()
                        result = conn.execute(stmt)
                        inserted_count += result.rowcount
                        conn.commit()
                        self.logger.info(f"Streamed and inserted {inserted_count} rows so far")
                        chunk = []

                # Insert any remaining data
                if chunk:
                    stmt = insert(table).values(chunk).on_conflict_do_nothing()
                    result = conn.execute(stmt)
                    inserted_count += result.rowcount
                    conn.commit()

                self.logger.info(f"Total streamed and inserted: {inserted_count} rows")
                return inserted_count
            except SQLAlchemyError as e:
                conn.rollback()
                self.logger.error(f"Error streaming data into '{table_name}': {e}")
                raise





    async def close(self):
        if self.engine:
            self.engine.dispose()
            self.logger.info("Database connection closed")








    async def store_account_summary(self, storage_data):
        self.logger.info(f"Storing batch of {len(storage_data)} records")
        table = self.get_or_create_table("account_summary")
        current_time = datetime.now().isoformat()

        with self.engine.connect() as conn:
            if storage_data[0][-1]:  # Check if is_new_day for the first record
                # Log the values being used in the update statement
                account, currency, metric = storage_data[0][0], storage_data[0][1], storage_data[0][2]
                self.logger.info(f"Updating previous day's records for account: {account}, currency: {currency}, metric: {metric}")

                # Set is_latest to False for the previous day's records
                update_stmt = (
                    table.update()
                    .where(
                        (table.c.account == account) &
                        (table.c.currency == currency) &
                        (table.c.metric == metric) &
                        (table.c.is_latest == True)
                    )
                    .values(is_latest=False)
                )
                self.logger.info(f"Executing update for previous day's records: {update_stmt}")
                conn.execute(update_stmt)

            # Prepare insert statement
            insert_stmt = insert(table).values([
                {
                    'account': account,
                    'currency': currency,
                    'metric': metric,
                    'value': value,
                    'is_latest': is_new_day,
                    'updated_at': current_time
                } for account, currency, metric, value, is_new_day in storage_data
            ])
            self.logger.info(f"Executing insert for new records: {insert_stmt}")
            conn.execute(insert_stmt)

            conn.commit()

    async def store_portfolio(self, account, contract, position_data, is_new_day):
        table = self.get_or_create_table("account_portfolio")
        current_time = datetime.now().isoformat()

        with self.engine.connect() as conn:
            if is_new_day:
                # Insert a new row
                stmt = insert(table).values(
                    account=account, contract=contract,
                    position=position_data['position'],
                    market_price=position_data['marketPrice'],
                    market_value=position_data['marketValue'],
                    average_cost=position_data['averageCost'],
                    unrealizedPNL=position_data['unrealizedPNL'],
                    realizedPNL=position_data['realizedPNL'],
                    is_latest=True, updated_at=current_time
                )
            else:
                # Update existing row
                stmt = (
                    table.update()
                    .where(
                        (table.c.account == account) &
                        (table.c.contract == contract) &
                        (table.c.is_latest == True)
                    )
                    .values(
                        position=position_data['position'],
                        market_price=position_data['marketPrice'],
                        market_value=position_data['marketValue'],
                        average_cost=position_data['averageCost'],
                        unrealizedPNL=position_data['unrealizedPNL'],
                        realizedPNL=position_data['realizedPNL'],
                        updated_at=current_time
                    )
                )
            
            conn.execute(stmt)
            conn.commit()













if __name__ == "__main__":
    # Test functions to validate PostgreSQL client functionality
    def test_postgresql_client():

        # Initialize the PostgreSQL client
        client = PostgresqlClient(db_name='test', db_user='myuser', db_password='kchau99', is_test_mode=False, use_local=True)

        # Create schema if not exists
        client.create_schema_if_not_exists()

        # Test table creation
        table_name = "account_summary"
        client.get_or_create_table(table_name)

        # Insert test data
        test_data = [
            {"account": "test_account", "currency": "USD", "metric": "test_metric", "value": 100.0, "is_latest": True, "updated_at": datetime.now().isoformat()}
        ]
        client.insert_data(table_name, test_data)

        print(f"Data inserted into {table_name} successfully.")



    # Run the test
    test_postgresql_client()