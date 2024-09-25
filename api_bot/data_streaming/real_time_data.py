import asyncio
import logging
from ibapi.common import TickerId, BarData

class RealTimeDataStream:
    def __init__(self, app):
        self.app = app
        self.logger = logging.getLogger(__name__)
        self.latest_bar = None

    async def stream_real_time_data(self, contract):
        try:
            #self.latest_bar = self.app.reqRealTimeBars(contract, 5, "MIDPOINT", True, [])
            self.latest_bar  = self.app.reqHistoricalData(contract, endDateTime='', durationStr='30 D',barSizeSetting='1 hour', whatToShow='MIDPOINT', useRTH=True, keepUpToDate=True)
            self.logger.info(f"Requested real-time data for {contract.symbol}")

            while True:
                await asyncio.sleep(1)  # Wait for new data

                if self.latest_bar:
                    await self.app.storage_manager.store_bar_data(self.latest_bar)
                    self.latest_bar = None
                    
        except Exception as e:
            self.logger.error(f"Failed to stream real-time data: {str(e)}")


    async def stop(self):
        self.app.sleep(30)
        self.app.cancelRealTimeBars(self.latest_bar)





    def realtimeBar(self, reqId: TickerId, time: int, open_: float, high: float, low: float, close: float,
                    volume: int, wap: float, count: int):
        self.latest_bar = BarData(time, -1, open_, high, low, close, volume, count, wap)
        self.logger.info(f"Received real-time bar: {self.latest_bar}")