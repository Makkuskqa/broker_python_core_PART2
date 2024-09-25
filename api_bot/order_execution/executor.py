from ibapi.order import Order
import logging

class OrderExecutor:
    def __init__(self, app):
        self.app = app
        self.logger = logging.getLogger(__name__)

    def place_market_order(self, contract, action, quantity):
        try:
            order = Order()
            order.action = action
            order.orderType = "MKT"
            order.totalQuantity = quantity

            self.app.placeOrder(self.app.nextOrderId(), contract, order)
            self.logger.info(f"Placed market order: {action} {quantity} {contract.symbol}")
        except Exception as e:
            self.logger.error(f"Failed to place market order: {str(e)}")

    # Add methods for other order types




import pandas as pd
from datetime import datetime

class LimitOrderTracker:
    def __init__(self):
        self.df = pd.DataFrame(columns=[
            'Symbol', 'Exchange', 'Currency', 'SecType', 'ConId', 'LocalSymbol', 'TradingClass',
            'Action', 'Quantity', 'OrderType', 'LimitPrice', 'TimeInForce',
            'OrderId', 'ClientId', 'PermId', 'AuxPrice',
            'Status', 'Filled', 'Remaining', 'AvgFillPrice', 'LastFillPrice',
            'ParentId', 'WhyHeld', 'MktCapPrice',
            'FillTime', 'FillQuantity', 'FillPrice', 'Commission',
            'ExecId', 'AcctNumber', 'CumQty', 'OrderRef', 'EvRule', 'EvMultiplier', 'ModelCode', 'LastLiquidity',
            'RealizedPNL', 'Yield', 'YieldRedemptionDate',
            'InitOrderTime', 'ExecutionTime',
        ])

    def format_time(self, time_input):
        if isinstance(time_input, datetime):
            return time_input.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        elif isinstance(time_input, (int, float)):
            dt = datetime.fromtimestamp(time_input)
            return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        return str(time_input)

    def add_trade(self, trade, init_order_time, execution_time):
        trade_info = {
            'Symbol': trade.contract.symbol,
            'Exchange': trade.contract.exchange,
            'Currency': trade.contract.currency,
            'SecType': trade.contract.secType,
            'ConId': trade.contract.conId,
            'LocalSymbol': trade.contract.localSymbol,
            'TradingClass': trade.contract.tradingClass,
            'Action': trade.order.action,
            'Quantity': trade.order.totalQuantity,
            'OrderType': trade.order.orderType,
            'LimitPrice': trade.order.lmtPrice,
            'TimeInForce': trade.order.tif,
            'OrderId': trade.order.orderId,
            'ClientId': trade.order.clientId,
            'PermId': trade.order.permId,
            'AuxPrice': trade.order.auxPrice,
            'Status': trade.orderStatus.status,
            'Filled': trade.orderStatus.filled,
            'Remaining': trade.orderStatus.remaining,
            'AvgFillPrice': trade.orderStatus.avgFillPrice,
            'LastFillPrice': trade.orderStatus.lastFillPrice,
            'ParentId': trade.orderStatus.parentId,
            'WhyHeld': trade.orderStatus.whyHeld,
            'MktCapPrice': trade.orderStatus.mktCapPrice,
            'InitOrderTime': execution_time if isinstance(execution_time, str) else f"{execution_time:.3f}",
            'ExecutionTime': execution_time if isinstance(execution_time, str) else f"{execution_time:.3f}"

        }

        new_rows = []
        for fill in trade.fills:
            fill_info = trade_info.copy()
            fill_info.update({
                'FillTime': self.format_time(fill.time),
                'FillQuantity': fill.execution.shares,
                'FillPrice': fill.execution.price,
                'Commission': fill.commissionReport.commission,
                'ExecId': fill.execution.execId,
                'AcctNumber': fill.execution.acctNumber,
                'CumQty': fill.execution.cumQty,
                'OrderRef': fill.execution.orderRef,
                'EvRule': fill.execution.evRule,
                'EvMultiplier': fill.execution.evMultiplier,
                'ModelCode': fill.execution.modelCode,
                'LastLiquidity': fill.execution.lastLiquidity,
                'RealizedPNL': fill.commissionReport.realizedPNL,
                'Yield': fill.commissionReport.yield_,
                'YieldRedemptionDate': fill.commissionReport.yieldRedemptionDate
            })
            new_rows.append(fill_info)

        new_df = pd.DataFrame(new_rows)
        self.df = pd.concat([self.df, new_df], ignore_index=True)

    def save_to_csv(self):
        filename = f"trade_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        self.df.to_csv(filename, index=False)
        print(f"Trade log saved to {filename}")

# Usage:
tracker = LimitOrderTracker()

# To save the current state of the DataFrame:
#tracker.save_to_csv()