import logging
from ibapi.client import EClient

class IBConnection:
    def __init__(self, app):
        self.app = app
        self.logger = logging.getLogger(__name__)

    async def connect(self, host, port, client_id):
        try:
            self.app.connect(host, port, client_id)
            self.logger.info(f"Connected to IB on {host}:{port}")
            # Remove self.app.run() from here
        except Exception as e:
            self.logger.error(f"Failed to connect to IB: {str(e)}")
            raise

    async def disconnect(self):
        self.app.disconnect()
        self.logger.info("Disconnected from IB")