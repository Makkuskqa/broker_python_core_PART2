import asyncio
import logging








import queue
import threading
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
import time

class StatsManager(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.data_queue = queue.Queue()
        self.account_summary = {}

    def accountSummary(self, reqId: int, account: str, tag: str, value: str, currency: str):
        if account not in self.account_summary:
            self.account_summary[account] = {}
        if currency not in self.account_summary[account]:
            self.account_summary[account][currency] = {}
        self.account_summary[account][currency][tag] = {'value': value, 'currency': currency}
        self.data_queue.put(f"AccountSummary. ReqId: {reqId}, Account: {account}, Tag: {tag}, Value: {value}, Currency: {currency}")

    def accountSummaryEnd(self, reqId: int):
        self.data_queue.put(f"AccountSummaryEnd. ReqId: {reqId}")






def run_loop(app):
    app.run()

# Create and connect the app
app = TradeApp()
app.connect("127.0.0.1", 4002, clientId=13)

# Start the app in a separate thread
api_thread = threading.Thread(target=run_loop, args=(app,), daemon=True)
api_thread.start()

# Wait for the connection to be established
time.sleep(1)

if app.isConnected():
    print("Connected successfully")

    # Request account summary
    app.reqAccountSummary(1, "All", "$LEDGER:ALL")

    # Process incoming data for a specific duration or until a condition is met
    timeout = time.time() + 5  # Process data for 5 seconds
    while time.time() < timeout:
        try:
            data = app.data_queue.get(timeout=1)
            print(data)
        except queue.Empty:
            pass

    # Cancel the request and disconnect
    app.cancelAccountSummary(1)
    app.disconnect()
else:
    print("Failed to connect")

# Wait for the API thread to finish
api_thread.join()

# Print the collected account summary data
print("\nAccount Summary Data:")
print(app.account_summary)

# Now you can use app.account_summary dictionary for further processing















{'All': {

'EUR': {
    'Currency': {'value': 'EUR', 'currency': 'EUR'},
   'CashBalance': {'value': '0.89', 'currency': 'EUR'},
   'TotalCashBalance': {'value': '0.89', 'currency': 'EUR'},
   'AccruedCash': {'value': '0.00', 'currency': 'EUR'},
   'StockMarketValue': {'value': '0.00', 'currency': 'EUR'},
   'OptionMarketValue': {'value': '0.00', 'currency': 'EUR'},
   'FutureOptionValue': {'value': '0.00', 'currency': 'EUR'},
   'FuturesPNL': {'value': '0.00', 'currency': 'EUR'},
   'NetLiquidationByCurrency': {'value': '0.89', 'currency': 'EUR'},
   'UnrealizedPnL': {'value': '0.00', 'currency': 'EUR'},
   'RealizedPnL': {'value': '0.00', 'currency': 'EUR'},
   'ExchangeRate': {'value': '1.1123801', 'currency': 'EUR'},
   'FundValue': {'value': '0.00', 'currency': 'EUR'},
   'NetDividend': {'value': '0.00', 'currency': 'EUR'},
   'MutualFundValue': {'value': '0.00', 'currency': 'EUR'},
   'MoneyMarketFundValue': {'value': '0.00', 'currency': 'EUR'},
   'CorporateBondValue': {'value': '0.00', 'currency': 'EUR'},
   'TBondValue': {'value': '0.00', 'currency': 'EUR'},
   'TBillValue': {'value': '0.00', 'currency': 'EUR'},
   'WarrantValue': {'value': '0.00', 'currency': 'EUR'},
   'FxCashBalance': {'value': '0.00', 'currency': 'EUR'},
   'AccountOrGroup': {'value': 'All', 'currency': 'EUR'},
   'RealCurrency': {'value': 'EUR', 'currency': 'EUR'},
   'IssuerOptionValue': {'value': '0.00', 'currency': 'EUR'},
   'Cryptocurrency': {'value': '0.00', 'currency': 'EUR'}},
'USD': {'Currency': {'value': 'USD', 'currency': 'USD'},
   'CashBalance': {'value': '994858.2954', 'currency': 'USD'},
   'TotalCashBalance': {'value': '994858.2954', 'currency': 'USD'},
   'AccruedCash': {'value': '1981.44', 'currency': 'USD'},
   'StockMarketValue': {'value': '2133.95', 'currency': 'USD'},
   'OptionMarketValue': {'value': '0.00', 'currency': 'USD'},
   'FutureOptionValue': {'value': '0.00', 'currency': 'USD'},
   'FuturesPNL': {'value': '0.00', 'currency': 'USD'},
   'NetLiquidationByCurrency': {'value': '1008386.4613', 'currency': 'USD'},
   'UnrealizedPnL': {'value': '-7860.36', 'currency': 'USD'},
   'RealizedPnL': {'value': '0.00', 'currency': 'USD'},
   'ExchangeRate': {'value': '1.00', 'currency': 'USD'},
   'FundValue': {'value': '0.00', 'currency': 'USD'},
   'NetDividend': {'value': '0.00', 'currency': 'USD'},
   'MutualFundValue': {'value': '0.00', 'currency': 'USD'},
   'MoneyMarketFundValue': {'value': '0.00', 'currency': 'USD'},
   'CorporateBondValue': {'value': '0.00', 'currency': 'USD'},
   'TBondValue': {'value': '0.00', 'currency': 'USD'},
   'TBillValue': {'value': '0.00', 'currency': 'USD'},
   'WarrantValue': {'value': '0.00', 'currency': 'USD'},
   'FxCashBalance': {'value': '0.00', 'currency': 'USD'},
   'AccountOrGroup': {'value': 'All', 'currency': 'USD'},
   'RealCurrency': {'value': 'USD', 'currency': 'USD'},
   'IssuerOptionValue': {'value': '0.00', 'currency': 'USD'},
   'Cryptocurrency': {'value': '9412.77', 'currency': 'USD'}},
'BASE': {'Currency': {'value': 'BASE', 'currency': 'BASE'},
   'CashBalance': {'value': '994859.2852', 'currency': 'BASE'},
   'TotalCashBalance': {'value': '994859.2852', 'currency': 'BASE'},
   'AccruedCash': {'value': '1981.44', 'currency': 'BASE'},
   'StockMarketValue': {'value': '2133.95', 'currency': 'BASE'},
   'OptionMarketValue': {'value': '0.00', 'currency': 'BASE'},
   'FutureOptionValue': {'value': '0.00', 'currency': 'BASE'},
   'FuturesPNL': {'value': '0.00', 'currency': 'BASE'},
   'NetLiquidationByCurrency': {'value': '1008387.4511', 'currency': 'BASE'},
   'UnrealizedPnL': {'value': '-7860.36', 'currency': 'BASE'},
   'RealizedPnL': {'value': '0.00', 'currency': 'BASE'},
   'ExchangeRate': {'value': '1.00', 'currency': 'BASE'},
   'FundValue': {'value': '0.00', 'currency': 'BASE'},
   'NetDividend': {'value': '0.00', 'currency': 'BASE'},
   'MutualFundValue': {'value': '0.00', 'currency': 'BASE'},
   'MoneyMarketFundValue': {'value': '0.00', 'currency': 'BASE'},
   'CorporateBondValue': {'value': '0.00', 'currency': 'BASE'},
   'TBondValue': {'value': '0.00', 'currency': 'BASE'},
   'TBillValue': {'value': '0.00', 'currency': 'BASE'},
   'WarrantValue': {'value': '0.00', 'currency': 'BASE'},
   'FxCashBalance': {'value': '0.00', 'currency': 'BASE'},
   'AccountOrGroup': {'value': 'All', 'currency': 'BASE'},
   'RealCurrency': {'value': 'BASE', 'currency': 'BASE'},
   'IssuerOptionValue': {'value': '0.00', 'currency': 'BASE'},
   'Cryptocurrency': {'value': '9412.77', 'currency': 'BASE'}}}}
