from ibapi.contract import Contract

class ContractBuilder:
    @staticmethod
    def build_crypto_contract(symbol, exchange, currency):
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "CRYPTO"
        contract.exchange = exchange
        contract.currency = currency
        return contract

    
    @staticmethod
    def build_forex_contract(symbol, exchange, currency):
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "CASH"
        contract.exchange = exchange
        contract.currency = currency
        return contract
    
