from ibapi.client import *
from ibapi.wrapper import *
from ibapi.contract import Contract
import time
import threading

class PortfolioManager(EWrapper, EClient):
    def __init__(self): 
        EClient.__init__(self, self)
        self.account_values = {}
        self.portfolio = {}
        self.account_time = None

    def updateAccountValue(self, key: str, val: str, currency: str, accountName: str):
        super().updateAccountValue(key, val, currency, accountName)
        if accountName not in self.account_values:
            self.account_values[accountName] = {}
        if currency not in self.account_values[accountName]:
            self.account_values[accountName][currency] = {}
        self.account_values[accountName][currency][key] = val
        print("UpdateAccountValue. Key:", key, "Value:", val, "Currency:", currency, "AccountName:", accountName)

    def updatePortfolio(self, contract: Contract, position: float,
                        marketPrice: float, marketValue: float,
                        averageCost: float, unrealizedPNL: float,
                        realizedPNL: float, accountName: str):
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
        
        print("UpdatePortfolio.", "Symbol:", contract.symbol, "SecType:", contract.secType, "Exchange:",
              contract.exchange, "Position:", position, "MarketPrice:", marketPrice,
              "MarketValue:", marketValue, "AverageCost:", averageCost,
              "UnrealizedPNL:", unrealizedPNL, "RealizedPNL:", realizedPNL,
              "AccountName:", accountName)

    def updateAccountTime(self, timeStamp: str):
        super().updateAccountTime(timeStamp)
        self.account_time = timeStamp
        print("UpdateAccountTime. Time:", timeStamp)

    def accountDownloadEnd(self, accountName: str):
        super().accountDownloadEnd(accountName)
        print("AccountDownloadEnd. Account:", accountName)

def run_loop(app):
    app.run()

# Create and connect the app
app = TestWrapper()
app.connect("127.0.0.1", 4002, clientId=15)

# Start the app in a separate thread
api_thread = threading.Thread(target=run_loop, args=(app,), daemon=True)
api_thread.start()

# Wait for the connection to be established
time.sleep(1)

if app.isConnected():
    print("Connected successfully")

    # Request account updates
    app.reqAccountUpdates(True, "9001")

    # Process incoming data for a specific duration
    timeout = time.time() + 10  # Process data for 10 seconds
    while time.time() < timeout:
        time.sleep(1)

    # Cancel the request and disconnect
    app.reqAccountUpdates(False, "9001")
    app.disconnect()
else:
    print("Failed to connect")

# Wait for the API thread to finish
api_thread.join()

# Print the collected data
print("\nAccount Values:")
print(app.account_values)
print("\nPortfolio:")
print(app.portfolio)
print("\nAccount Time:")
print(app.account_time)






app.account_values

{'DU9439365': {'': {'AccountCode': 'DU9439365',
   'AccountReady': 'true',
   'AccountType': 'INDIVIDUAL',
   'ColumnPrio-C': '2',
   'ColumnPrio-P': '5',
   'ColumnPrio-S': '1',
   'Cushion': '0.986411',
   'DayTradesRemaining': '-1',
   'DayTradesRemainingT+1': '-1',
   'DayTradesRemainingT+2': '-1',
   'DayTradesRemainingT+3': '-1',
   'DayTradesRemainingT+4': '-1',
   'DayTradingStatus-S': '::false:996937.76::false',
   'Leverage-S': '0.00',
   'LookAheadNextChange': '1726603200',
   'NLVAndMarginInReview': 'false',
   'SegmentTitle-C': 'US Commodities',
   'SegmentTitle-P': 'Crypto at Paxos',
   'SegmentTitle-S': 'US Securities',
   'TradingType-S': 'STKNOPT',
   'WhatIfPMEnabled': 'true'},
  'BASE': {'AccountOrGroup': 'DU9439365',
   'AccruedCash': '1981.44',
   'CashBalance': '994859.2848',
   'CorporateBondValue': '0.00',
   'Cryptocurrency': '9645.19',
   'Currency': 'BASE',
   'ExchangeRate': '1.00',
   'FundValue': '0.00',
   'FutureOptionValue': '0.00',
   'FuturesPNL': '0.00',
   'FxCashBalance': '0.00',
   'IssuerOptionValue': '0.00',
   'MoneyMarketFundValue': '0.00',
   'MutualFundValue': '0.00',
   'NetDividend': '0.00',
   'NetLiquidationByCurrency': '1008564.388',
   'OptionMarketValue': '0.00',
   'RealCurrency': 'BASE',
   'RealizedPnL': '0.00',
   'StockMarketValue': '2078.47',
   'TBillValue': '0.00',
   'TBondValue': '0.00',
   'TotalCashBalance': '994859.2848',
   'UnrealizedPnL': '-7683.43',
   'WarrantValue': '0.00'},
  'EUR': {'AccountOrGroup': 'DU9439365',
   'AccruedCash': '0.00',
   'CashBalance': '0.89',
   'CorporateBondValue': '0.00',
   'Cryptocurrency': '0.00',
   'Currency': 'EUR',
   'ExchangeRate': '1.1123801',
   'FundValue': '0.00',
   'FutureOptionValue': '0.00',
   'FuturesPNL': '0.00',
   'FxCashBalance': '0.00',
   'IssuerOptionValue': '0.00',
   'MoneyMarketFundValue': '0.00',
   'MutualFundValue': '0.00',
   'NetDividend': '0.00',
   'NetLiquidationByCurrency': '0.89',
   'OptionMarketValue': '0.00',
   'RealCurrency': 'EUR',
   'RealizedPnL': '0.00',
   'StockMarketValue': '0.00',
   'TBillValue': '0.00',
   'TBondValue': '0.00',
   'TotalCashBalance': '0.89',
   'UnrealizedPnL': '0.00',
   'WarrantValue': '0.00'},
  'USD': {'AccountOrGroup': 'DU9439365',
   'AccruedCash': '1981.44',
   'AccruedCash-C': '0.00',
   'AccruedCash-P': '0.00',
   'AccruedCash-S': '1981.44',
   'AccruedDividend': '0.00',
   'AccruedDividend-C': '0.00',
   'AccruedDividend-P': '0.00',
   'AccruedDividend-S': '0.00',
   'AvailableFunds': '994859.29',
   'AvailableFunds-C': '0.00',
   'AvailableFunds-P': '0.00',
   'AvailableFunds-S': '994859.29',
   'Billable': '0.00',
   'Billable-C': '0.00',
   'Billable-P': '0.00',
   'Billable-S': '0.00',
   'BuyingPower': '3979437.14',
   'CashBalance': '994858.2954',
   'CorporateBondValue': '0.00',
   'Cryptocurrency': '9645.19',
   'Currency': 'USD',
   'EquityWithLoanValue': '996937.76',
   'EquityWithLoanValue-C': '0.00',
   'EquityWithLoanValue-P': '0.00',
   'EquityWithLoanValue-S': '996937.76',
   'ExcessLiquidity': '994859.29',
   'ExcessLiquidity-C': '0.00',
   'ExcessLiquidity-P': '0.00',
   'ExcessLiquidity-S': '994859.29',
   'ExchangeRate': '1.00',
   'FullAvailableFunds': '994859.29',
   'FullAvailableFunds-C': '0.00',
   'FullAvailableFunds-P': '0.00',
   'FullAvailableFunds-S': '994859.29',
   'FullExcessLiquidity': '994859.29',
   'FullExcessLiquidity-C': '0.00',
   'FullExcessLiquidity-P': '0.00',
   'FullExcessLiquidity-S': '994859.29',
   'FullInitMarginReq': '2078.47',
   'FullInitMarginReq-C': '0.00',
   'FullInitMarginReq-P': '0.00',
   'FullInitMarginReq-S': '2078.47',
   'FullMaintMarginReq': '2078.47',
   'FullMaintMarginReq-C': '0.00',
   'FullMaintMarginReq-P': '0.00',
   'FullMaintMarginReq-S': '2078.47',
   'FundValue': '0.00',
   'FutureOptionValue': '0.00',
   'FuturesPNL': '0.00',
   'FxCashBalance': '0.00',
   'GrossPositionValue': '11723.66',
   'GrossPositionValue-S': '2078.47',
   'Guarantee': '0.00',
   'Guarantee-C': '0.00',
   'Guarantee-P': '0.00',
   'Guarantee-S': '0.00',
   'IncentiveCoupons': '0.00',
   'IncentiveCoupons-C': '0.00',
   'IncentiveCoupons-P': '0.00',
   'IncentiveCoupons-S': '0.00',
   'IndianStockHaircut': '0.00',
   'IndianStockHaircut-C': '0.00',
   'IndianStockHaircut-P': '0.00',
   'IndianStockHaircut-S': '0.00',
   'InitMarginReq': '2078.47',
   'InitMarginReq-C': '0.00',
   'InitMarginReq-P': '0.00',
   'InitMarginReq-S': '2078.47',
   'IssuerOptionValue': '0.00',
   'LookAheadAvailableFunds': '994859.29',
   'LookAheadAvailableFunds-C': '0.00',
   'LookAheadAvailableFunds-P': '0.00',
   'LookAheadAvailableFunds-S': '994859.29',
   'LookAheadExcessLiquidity': '994859.29',
   'LookAheadExcessLiquidity-C': '0.00',
   'LookAheadExcessLiquidity-P': '0.00',
   'LookAheadExcessLiquidity-S': '994859.29',
   'LookAheadInitMarginReq': '2078.47',
   'LookAheadInitMarginReq-C': '0.00',
   'LookAheadInitMarginReq-P': '0.00',
   'LookAheadInitMarginReq-S': '2078.47',
   'LookAheadMaintMarginReq': '2078.47',
   'LookAheadMaintMarginReq-C': '0.00',
   'LookAheadMaintMarginReq-P': '0.00',
   'LookAheadMaintMarginReq-S': '2078.47',
   'MaintMarginReq': '2078.47',
   'MaintMarginReq-C': '0.00',
   'MaintMarginReq-P': '0.00',
   'MaintMarginReq-S': '2078.47',
   'MoneyMarketFundValue': '0.00',
   'MutualFundValue': '0.00',
   'NetDividend': '0.00',
   'NetLiquidation': '1008564.39',
   'NetLiquidation-C': '0.00',
   'NetLiquidation-P': '9645.19',
   'NetLiquidation-S': '998919.20',
   'NetLiquidationByCurrency': '1008563.3986',
   'NetLiquidationUncertainty': '0.00',
   'OptionMarketValue': '0.00',
   'PASharesValue': '0.00',
   'PASharesValue-C': '0.00',
   'PASharesValue-P': '0.00',
   'PASharesValue-S': '0.00',
   'PhysicalCertificateValue': '0.00',
   'PhysicalCertificateValue-C': '0.00',
   'PhysicalCertificateValue-P': '0.00',
   'PhysicalCertificateValue-S': '0.00',
   'PostExpirationExcess': '0.00',
   'PostExpirationExcess-C': '0.00',
   'PostExpirationExcess-P': '0.00',
   'PostExpirationExcess-S': '0.00',
   'PostExpirationMargin': '0.00',
   'PostExpirationMargin-C': '0.00',
   'PostExpirationMargin-P': '0.00',
   'PostExpirationMargin-S': '0.00',
   'PreviousDayEquityWithLoanValue': '997813.26',
   'PreviousDayEquityWithLoanValue-S': '997813.26',
   'RealCurrency': 'USD',
   'RealizedPnL': '0.00',
   'RegTEquity': '998919.20',
   'RegTEquity-S': '998919.20',
   'RegTMargin': '1039.24',
   'RegTMargin-S': '1039.24',
   'SMA': '997879.96',
   'SMA-S': '997879.96',
   'StockMarketValue': '2078.47',
   'TBillValue': '0.00',
   'TBondValue': '0.00',
   'TotalCashBalance': '994858.2954',
   'TotalCashValue': '994859.29',
   'TotalCashValue-C': '0.00',
   'TotalCashValue-P': '0.00',
   'TotalCashValue-S': '994859.29',
   'TotalDebitCardPendingCharges': '0.00',
   'TotalDebitCardPendingCharges-C': '0.00',
   'TotalDebitCardPendingCharges-P': '0.00',
   'TotalDebitCardPendingCharges-S': '0.00',
   'UnrealizedPnL': '-7683.43',
   'WarrantValue': '0.00'}}}





app.portfolio

{'DU9439365': {'ALZN': {'secType': 'STK',
   'exchange': '',
   'position': 1100.0,
   'marketPrice': 1.73600005,
   'marketValue': 1909.6,
   'averageCost': 4.36136365,
   'unrealizedPNL': -2887.9,
   'realizedPNL': 0.0},
  'BIGGQ': {'secType': 'STK',
   'exchange': '',
   'position': 2000.0,
   'marketPrice': 0.08443595,
   'marketValue': 168.87,
   'averageCost': 1.055,
   'unrealizedPNL': -1941.13,
   'realizedPNL': 0.0},
  'ETH': {'secType': 'CRYPTO',
   'exchange': '',
   'position': 4.04838291,
   'marketPrice': 2382.47998045,
   'marketValue': 9645.19,
   'averageCost': 3087.551295,
   'unrealizedPNL': -2854.4,
   'realizedPNL': 0.0}}}