from src.clients.etherscan_client import EtherscanClient
from src.fetchers.contract import ContractFetcher
from src.schemas.python.contract import Contract


class ContractExtractor:
    def __init__(self, client: EtherscanClient):
        self.fetcher = ContractFetcher(client=client)

    async def run(
        self,
        items: list[dict],
        initial=None,
        total=None,
        batch_size=1,
        show_progress=True,
    ):
        contract_addresses = []
        for block in items:
            for receipt in block:
                contracts = self.collect(receipt)
                contract_addresses.extend(contracts)

        contract_addresses = list(set(contract_addresses))
        responses = await self.fetcher.run(
            contract_addresses=contract_addresses, show_progress=show_progress
        )
        results = []
        for address, response in zip(contract_addresses, responses):
            contract = self.extract(address, response)
            if contract:
                results.append(contract.model_dump())

        return results

    def collect(self, item: dict):
        contract_address = []

        if item["contractAddress"]:
            contract_address.append(item["contractAddress"])

        for log in item.get("logs", []):
            contract_address.append(log["address"])

        return contract_address

    def extract(self, contract_address, result):
        if not result or result == "Invalid Address format":
            return None

        # contract = Contract(
        #     name=result["ContractName"],
        #     address=contract_address,
        #     abi=result["ABI"],
        # )

        try:
            contract = Contract(
                name=result["ContractName"],
                address=contract_address,
                abi=result["ABI"],
            )
        except Exception:
            print(result)
            raise

        return contract
