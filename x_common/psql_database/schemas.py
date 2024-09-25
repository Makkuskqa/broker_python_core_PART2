from sqlalchemy import Index, Column, String, MetaData, Table, BigInteger, Integer, Boolean, Date, ARRAY, Float, UniqueConstraint, JSON
from datetime import datetime
from sqlalchemy import JSON


def get_schema(table_name, test_mode=False):
    """
        Creates and returns a SQLAlchemy Table object representing the 'finviz' table schema.

        This function defines the schema for a table named 'finviz', with each column represented by a SQLAlchemy Column object.
        The columns are primarily of type String, tailored to store data related to financial information as typically provided by Finviz.

        Returns:
            table (Table): A SQLAlchemy Table object with the defined schema for 'finviz'."""
    metadata = MetaData()
                                                                        
    if table_name == "leeway_exchanges":
        table = Table(table_name, metadata,
                    Column('_id', String, unique=True),
                      Column('name', String),
                      Column('exchange_code', String),
                      Column('operating_mic', String),
                      Column('country', String),
                      Column('currency', String),
                      Column('country_iso2', String),
                      Column('country_iso3', String),
                      Column('added_at_day', String, default=datetime.today().strftime('%Y-%m-%d')),)


    elif table_name == "leeway_assets":
        table = Table(table_name, metadata,
                    Column('_id', String, unique=True),
                    Column('ticker', String),
                    Column('company_name', String),
                    Column('is_in', String),
                    Column('exchange_code', String),
                    Column('processed_date', Date, default=None),
                    Column('processed', Boolean, default=False),
                    Column('added_at_day', String, default=datetime.today().strftime('%Y-%m-%d')),)

    return table
    

def get_json_to_sql_column_mapping(table_name):
                                                    
    if table_name == "leeway_exchanges":
        json_to_column_mapping = {}

    elif table_name == "leeway_assets":
        json_to_column_mapping = {}

  