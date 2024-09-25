import asyncio
import logging
from datetime import datetime
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
import queue


class StatsManager(EWrapper, EClient):
    def __init__(self, host, port, client_id, storage_manager):
        EWrapper.__init__(self)
        EClient.__init__(self, wrapper=self)
        self.logger = logging.getLogger(__name__)
        self.data_queue = queue.Queue()
        self.account_summary = {}
        self.req_id = 123
        self.last_storage_date = None
        self.storage_manager = storage_manager

        # Connect to IB API
        self.connect(host, port, client_id)
        self.run_loop()

    def run_loop(self):
        # Start the socket in a separate thread
        asyncio.get_event_loop().run_in_executor(None, self.run)

    def request_account_summary(self):
        self.account_summary.clear()
        #self.logger.info("Clearing previous account summary data")

        self.reqAccountSummary(self.req_id, "All", "$LEDGER:ALL")
        #self.logger.info(f"Requested account summary with reqId: {self.req_id}")

    def accountSummary(self, reqId, account, tag, value, currency):
        #self.logger.info(f"Received: ReqId: {reqId}, Account: {account}, Tag: {tag}, Value: {value}, Currency: {currency}")
        if account not in self.account_summary:
            self.account_summary[account] = {}
        if currency not in self.account_summary[account]:
            self.account_summary[account][currency] = {}
        self.account_summary[account][currency][tag] = {'value': value, 'currency': currency}
        self.data_queue.put(f"AccountSummary. ReqId: {reqId}, Account: {account}, Tag: {tag}, Value: {value}, Currency: {currency}")

    def accountSummaryEnd(self, reqId):
        self.logger.info(f"Account summary end received: ReqId: {reqId}")
        self.data_queue.put(f"AccountSummaryEnd. ReqId: {reqId}")

    def error(self, reqId, errorCode, errorString):
        self.logger.error(f"Error {errorCode}: {errorString}")

    async def process_account_summary(self):
        timeout = 5  
        start_time = datetime.now()
        #data_received = []
        storage_data = []  # New list to collect data for storage
        while (datetime.now() - start_time).total_seconds() < timeout:
            try:
                data = self.data_queue.get(timeout=1)
                #data_received.append(data)
                # if data.startswith("AccountSummaryEnd"):
                #     break
            except queue.Empty:
                pass

        #self.logger.info(f"Queue size: {self.data_queue.qsize()}")
        #self.logger.info(f"Data received: {data_received}")
        #self.logger.info(f"Account Summary: {self.account_summary}")


        current_date = datetime.now().date()
        is_new_day = self.last_storage_date != current_date
        self.last_storage_date = current_date

        for account, currencies in self.account_summary.items():
            for currency, metrics in currencies.items():
                for metric, data in metrics.items():
                    value = float(data['value']) if data['value'].replace('.', '').isdigit() else 0.0
                    storage_data.append((account, currency, metric, value, is_new_day))  

        # Store all collected data in one call
        #print(storage_data)
        #await self.storage_manager.store_account_summary(storage_data)
        self.storage_manager.insert_data("account_summary", storage_data)



        #self.logger.info("Processed and stored account summary in database")


    def cancel_account_summary(self):
        self.cancelAccountSummary(self.req_id)
        #self.logger.info(f"Cancelled account summary request with reqId: {self.req_id}")


    async def run_once(self):
        self.request_account_summary()
        await asyncio.sleep(5)  

        if self.data_queue.empty():
            self.logger.warning("No data received from IB API")
        else:
            self.logger.info(f"Data queue size before processing: {self.data_queue.qsize()}")

        await self.process_account_summary()
        #await asyncio.sleep(5)  # Wait for data to load
        self.cancel_account_summary()


    async def run_periodically(self, interval_seconds):
        while True:
            #self.logger.info("Running account summary request")
            await self.run_once()
            await asyncio.sleep(interval_seconds)


    async def cleanup(self):
        #self.logger.info("Cleaning up resources...")
        await self.storage_manager.close()
        self.disconnect()












import sys
sys.path.append('..')
from data_storage.postgresql_client import PostgresqlClient


class MinimalApp:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.storage_manager = PostgresqlClient(db_name='test', db_user='myuser', db_password='kchau99', is_test_mode=False, use_local=True)
        self.stats_manager = StatsManager('127.0.0.1', 4002, 123, self.storage_manager)

    async def run(self):
        try:
            await self.stats_manager.run_periodically(60)  # Run every 30 seconds
        except Exception as e:
            self.logger.error(f"An error occurred: {e}")
        finally:
            await self.stats_manager.cleanup()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app = MinimalApp()
    asyncio.run(app.run())