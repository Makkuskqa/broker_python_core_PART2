import asyncio
import logging
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
import queue
from datetime import datetime

class PortfolioManager(EWrapper, EClient):
    def __init__(self, host, port, client_id, storage_manager):
        EWrapper.__init__(self)
        EClient.__init__(self, wrapper=self)
        self.logger = logging.getLogger(__name__)
        self.data_queue = queue.Queue()
        self.account_values = {}
        self.portfolio = {}
        self.account_time = None
        self.req_id = 122
        self.storage_manager = storage_manager

        # Connect to IB API
        self.connect(host, port, client_id)
        self.run_loop()

    def run_loop(self):
        # Start the socket in a separate thread
        asyncio.get_event_loop().run_in_executor(None, self.run)

    def request_account_updates(self):
        self.reqAccountUpdates(True, "9001")

    def updateAccountValue(self, key: str, val: str, currency: str, accountName: str):
        super().updateAccountValue(key, val, currency, accountName)
        if accountName not in self.account_values:
            self.account_values[accountName] = {}
        if currency not in self.account_values[accountName]:
            self.account_values[accountName][currency] = {}
        self.account_values[accountName][currency][key] = val

    def updatePortfolio(self, contract, position, marketPrice, marketValue, averageCost, unrealizedPNL, realizedPNL, accountName):
        super().updatePortfolio(contract, position, marketPrice, marketValue, averageCost, unrealizedPNL, realizedPNL, accountName)
        if accountName not in self.portfolio:
            self.portfolio[accountName] = {}
        self.portfolio[accountName][contract.symbol] = {
            'secType': contract.secType,
            'exchange': contract.exchange,
            'position': position,
            'marketPrice': marketPrice,
            'marketValue': marketValue,
            'averageCost': averageCost,
            'unrealizedPNL': unrealizedPNL,
            'realizedPNL': realizedPNL
        }

    def updateAccountTime(self, timeStamp: str):
        super().updateAccountTime(timeStamp)
        self.account_time = timeStamp

    def accountDownloadEnd(self, accountName: str):
        # print(self.account_values)
        # print("-------------------------------------------------------------------------------")
        # print(self.portfolio)

        super().accountDownloadEnd(accountName)
        self.store_data()

    def store_data(self):
        # Get current timestamp
        current_time = datetime.now().isoformat()

        # Process account_values data
        account_values_data = []
        for account, currencies in self.account_values.items():
            for currency, keys in currencies.items():
                for key, value in keys.items():
                    account_values_data.append({
                        'account': account,
                        'currency': currency,
                        'key': key,
                        'value': value,
                        'updated_at': current_time
                    })

        # Process portfolio data
        portfolio_data = []
        for account, positions in self.portfolio.items():
            for contract, details in positions.items():
                portfolio_data.append({
                    'account': account,
                    'contract': contract,
                    'position': str(details['position']),
                    'market_price': str(details['marketPrice']),
                    'market_value': str(details['marketValue']),
                    'average_cost': details['averageCost'],
                    'unrealizedPNL': details['unrealizedPNL'],
                    'realizedPNL': details['realizedPNL'],
                    'is_latest': True,
                    'updated_at': current_time
                })

        # Store the collected data
        if account_values_data:
            self.storage_manager.insert_data("account_values", account_values_data)
            self.logger.info(f"Stored {len(account_values_data)} account values records.")

        if portfolio_data:
            self.storage_manager.insert_data("account_portfolio", portfolio_data)
            self.logger.info(f"Stored {len(portfolio_data)} portfolio records.")

        if not account_values_data and not portfolio_data:
            self.logger.warning("No data to store.")

    async def cleanup(self):
        await self.storage_manager.close()
        self.disconnect()

    async def run_periodically(self, interval_seconds):
        while True:
            self.request_account_updates()
            await asyncio.sleep(interval_seconds)  # Wait for the specified interval
            self.store_data()  # Store data after each interval









import sys
sys.path.append('..')
from data_storage.postgresql_client import PostgresqlClient

class MinimalApp:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.storage_manager = PostgresqlClient(db_name='test', db_user='myuser', db_password='kchau99', is_test_mode=False, use_local=True)
        self.portfolio_manager = PortfolioManager('127.0.0.1', 4002, 122, self.storage_manager)

    async def run(self):
        try:
            await self.portfolio_manager.run_periodically(60)  # Run every 60 seconds
        except Exception as e:
            self.logger.error(f"An error occurred: {e}")
        finally:
            await self.portfolio_manager.cleanup()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app = MinimalApp()
    asyncio.run(app.run())