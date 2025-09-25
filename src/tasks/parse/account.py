from src.schemas.python.account import Account
from src.utils.enumeration import EntityType


def parse_account(results):
    for block_receipt in results[EntityType.RAW_RECEIPT]:
        for receipt in block_receipt:
            accounts = parse(receipt)
            results[EntityType.ACCOUNT].extend(accounts)


def parse(item: dict):
    contract_addresses = []
    account_addresses = []  # Externally Owned Account: wallet address, user_address, ...

    account_addresses.append(item.get("from"))
    if item["contractAddress"]:
        contract_addresses.append(item["contractAddress"])

    for log in item.get("logs", []):
        contract_addresses.append(log["address"])

    if item["to"] not in contract_addresses:
        account_addresses.append(item["to"])

    account_addresses = [
        Account(address=address).model_dump()
        for address in account_addresses
        if address
    ]

    return account_addresses
