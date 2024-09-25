import asyncio
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from connection.ib_connection import IBConnection
from contracts.contract_builder import ContractBuilder
from data_streaming.real_time_data import RealTimeDataStream
from order_execution.executor import OrderExecutor

from application_statistics.stats_summ_new import StatsManager
from application_statistics.account_portfolio import PortfolioManager

from data_storage.postgresql_client import PostgresqlClient
from utilsL.logging_config import (setup_logging, get_logger, log_time)


class TradingApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.logger = get_logger(__name__)
        self.connection = IBConnection(self)
        self.contract_builder = ContractBuilder()
        self.data_stream = RealTimeDataStream(self)
        self.order_executor = OrderExecutor(self)
        self.storage_manager = PostgresqlClient(db_name='test', db_user='myuser', db_password='kchau99', is_test_mode=False, use_local=True)
        self.stats_manager = StatsManager('127.0.0.1', 4002, 123, self.storage_manager)
        self.portfolio_manager = PortfolioManager('127.0.0.1', 4002, 122, self.storage_manager)
        self.tasks = []

    @log_time
    async def run(self):
        try:
            await self.connection.connect('127.0.0.1', 4002, 120)
            


            # Add other tasks if needed
            self.tasks = [
                self.create_task(self.stats_manager.run_periodically(60), "Stats Manager"),
                self.create_task(self.portfolio_manager.run_periodically(60), "Portfolio Manager"),
                # self.create_task(self.data_stream.stream_real_time_data(contract), "Data Stream"),
                # self.create_task(self.storage_manager.periodic_save(3600), "Storage Manager"),
            ]

            # Run all tasks concurrently
            if self.tasks:
                await asyncio.gather(*[task for task, _ in self.tasks], return_exceptions=True)

        except asyncio.CancelledError:
            self.logger.info("Main task was cancelled. Shutting down gracefully.")
        except Exception as e:
            self.logger.error(f"Error in main loop: {str(e)}")
        finally:
            await self.cleanup()



    def create_task(self, coro, name):
        task = asyncio.create_task(coro, name=name)
        task.add_done_callback(self.handle_task_result)
        return task, name

    def handle_task_result(self, task):
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.logger.error(f"Task {task.get_name()} failed with error: {str(e)}")




    async def cleanup(self):
        self.logger.info("Cleaning up resources...")
        for task, name in self.tasks:
            if not task.done():
                self.logger.info(f"Cancelling task: {name}")
                task.cancel()
        
        await asyncio.gather(*[task for task, _ in self.tasks], return_exceptions=True)

        await self.portfolio_manager.cleanup()
        await self.stats_manager.cleanup()
        await self.storage_manager.close()
        await self.connection.disconnect()



if __name__ == "__main__":
    setup_logging()
    app = TradingApp()
    asyncio.run(app.run())