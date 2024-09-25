from sqlalchemy import Index, Column, String, MetaData, Table, BigInteger, Integer, Boolean, Date, ARRAY, Float, UniqueConstraint, JSON
from datetime import datetime
from sqlalchemy import JSON


def get_schema(table_name):
    """
        Creates and returns a SQLAlchemy Table object representing the 'finviz' table schema.

        This function defines the schema for a table named 'finviz', with each column represented by a SQLAlchemy Column object.
        The columns are primarily of type String, tailored to store data related to financial information as typically provided by Finviz.

        Returns:
            table (Table): A SQLAlchemy Table object with the defined schema for 'finviz'."""
    metadata = MetaData()
                                                                        
    if table_name == "bar_data":
        table = Table(table_name, metadata,
                      Column('name', String),
                      Column('time', String),
                      Column('open', String),
                      Column('high', String),
                      Column('low', String),
                      Column('close', String)
                      ,)


    elif table_name == "account_summary":
        table = Table(table_name, metadata,
                    Column("account", String),
                    Column("currency", String),
                    Column("metric", String),
                    Column("value", Float),
                    Column("is_latest", Boolean, default=True),  
                    Column('updated_at', String))
    
    elif table_name == "account_values":
        table = Table(table_name, metadata,
                      Column('account', String),
                      Column('currency', String),
                      Column('key', String),
                      Column('value', String),
                      Column('updated_at', String))  # Add any additional fields as necessary

    elif table_name == "account_portfolio":
        table = Table(table_name, metadata,
                    Column('account', String),
                    Column('contract', String),
                    Column('position', String),
                    Column('market_price', String),
                    Column('market_value', String),
                    Column('average_cost', Float),
                    Column('unrealizedPNL', Float),
                    Column('realizedPNL', Float),
                    Column("is_latest", Boolean, default=True),
                    Column('updated_at', String)
                    ,)
        
    elif table_name == "account_trades":
        table = Table(table_name, metadata,
                Column('Symbol', String),
                Column('Exchange', String),
                Column('Currency', String),
                Column('SecType', String),
                Column('ConId', Integer),
                Column('LocalSymbol', String),
                Column('TradingClass', String),
                Column('Action', String),
                Column('Quantity', Float),
                Column('OrderType', String),
                Column('LimitPrice', Float),
                Column('TimeInForce', String),
                Column('OrderId', Integer),
                Column('ClientId', Integer),
                Column('PermId', Integer),
                Column('AuxPrice', Float),
                Column('Status', String),
                Column('Filled', Float),
                Column('Remaining', Float),
                Column('AvgFillPrice', Float),
                Column('LastFillPrice', Float),
                Column('ParentId', Integer),
                Column('WhyHeld', String),
                Column('MktCapPrice', Float),

                Column('InitOrderTime', String),        # crypto
                Column('ExecutionTime', String),        # crypto  
                Column('Liquidation', Integer),         # crypto + forex 
                Column('CommissionAmount', Float),      # crypto   
                Column('CommissionCurrency', String),   # crypto + forex 

                Column('FillTime', String),
                Column('FillQuantity', Float),
                Column('FillPrice', Float),
                Column('Commission', Float),
                Column('ExecId', String),
                Column('AcctNumber', String),
                Column('CumQty', Float),
                Column('OrderRef', String),
                Column('EvRule', String),
                Column('EvMultiplier', Float),
                Column('ModelCode', String),
                Column('LastLiquidity', Integer),
                Column('RealizedPNL', Float),
                Column('Yield', Float),
                Column('YieldRedemptionDate', String)
                ,)

    return table
    

def get_json_to_sql_column_mapping(table_name):
                                                    
    if table_name == "leeway_exchanges":
        json_to_column_mapping = {}

    elif table_name == "leeway_assets":
        json_to_column_mapping = {}

  