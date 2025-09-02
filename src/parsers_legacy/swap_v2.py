from types import SimpleNamespace

from utils.event_abi import EVENT_HEX_SIGNATURES, decode_log


# https://ethereum.stackexchange.com/questions/12553/understanding-logs-and-log-blooms
class SwapExtractor:
    def extract_swap_from_log(self, log):
        topics = log["topics"]
        if topics is None or len(topics) < 1:
            # This is normal, topics can be empty for anonymous events
            return None

        if (topics[0]).casefold() == EVENT_HEX_SIGNATURES["uniswap_v2"]["pair_created"]:
            decode = decode_log("uniswap_v2", "pair_created", log)

            swap = SimpleNamespace(
                contract_address=decode["pair"],
                call="pair_created",
                dex_exchange="uniswap_v2",
                transaction_hash=log['transaction_hash'],
                amount0_in=0,
                amount1_in=0,
                amount0_out=0,
                amount1_out=0,
                log_index=log['log_index'],
                block_number=log['block_number'],
            )
            return swap
        elif (
            (topics[0]).casefold() == EVENT_HEX_SIGNATURES["uniswap_v3"]["pool_created"]
        ):
            decode = decode_log("uniswap_v3", "pool_created", log)

            swap = SimpleNamespace(
                contract_address=decode["pool"],
                call="pair_created",
                dex_exchange="uniswap_v3",
                transaction_hash=log['transaction_hash'],
                amount0_in=0,
                amount1_in=0,
                amount0_out=0,
                amount1_out=0,
                log_index=log['log_index'],
                block_number=log['block_number'],
            )
            return swap
        elif (topics[0]).casefold() in EVENT_HEX_SIGNATURES["uniswap_v2"]["mint"]:
            decode = decode_log("uniswap_v2", "mint", log)

            swap = SimpleNamespace(
                contract_address=decode["sender"],
                call="add",
                dex_exchange="uniswap_v2",
                amount0_in=decode["amount0"],
                amount1_in=decode["amount1"],
                amount0_out=0,
                amount1_out=0,
                transaction_hash=log['transaction_hash'],
                log_index=log['log_index'],
                block_number=log['block_number'],
            )
            return swap
        elif (topics[0]).casefold() in EVENT_HEX_SIGNATURES["uniswap_v3"]["mint"]:
            decode = decode_log("uniswap_v3", "mint", log)

            swap = SimpleNamespace(
                contract_address=decode["sender"],
                call="add",
                dex_exchange="uniswap_v3",
                amount0_in=decode["amount0"],
                amount1_in=decode["amount1"],
                amount0_out=0,
                amount1_out=0,
                transaction_hash=log['transaction_hash'],
                log_index=log['log_index'],
                block_number=log['block_number'],
            )
            return swap
        elif (topics[0]).casefold() in EVENT_HEX_SIGNATURES["uniswap_v2"]["burn"]:
            decode = decode_log("uniswap_v2", "burn", log)

            swap = SimpleNamespace(
                contract_address=decode["sender"],
                call="remove",
                dex_exchange="uniswap_v2",
                amount0_in=0,
                amount1_in=0,
                amount0_out=decode["amount0"],
                amount1_out=decode["amount1"],
                transaction_hash=log['transaction_hash'],
                log_index=log['log_index'],
                block_number=log['block_number'],
            )
            return swap
        elif (topics[0]).casefold() in EVENT_HEX_SIGNATURES["uniswap_v3"]["burn"]:
            decode = decode_log("uniswap_v3", "burn", log)

            swap = SimpleNamespace(
                contract_address=decode["owner"],
                call="remove",
                dex_exchange="uniswap_v3",
                amount0_in=0,
                amount1_in=0,
                amount0_out=decode["amount0"],
                amount1_out=decode["amount1"],
                transaction_hash=log['transaction_hash'],
                log_index=log['log_index'],
                block_number=log['block_number'],
            )
            return swap
        elif (topics[0]).casefold() in EVENT_HEX_SIGNATURES["uniswap_v2"]["swap"]:
            decode = decode_log("uniswap_v2", "swap", log)
            if decode["amount0In"]!=0 and decode["amount0Out"]!=0:
                print(log)
                print(decode["amount0In"], decode["amount0Out"])
                print("i love you 0")
            
            if decode["amount1In"]!=0 and decode["amount1Out"]!=0:
                print(log)
                print(decode["amount1In"], decode["amount1Out"])
                print("i love you 1")

            swap = SimpleNamespace(
                contract_address=decode["sender"],
                amount0_in=decode["amount0In"],
                amount1_in=decode["amount1In"],
                amount0_out=decode["amount0Out"],
                amount1_out=decode["amount1Out"],
                dex_exchange="uniswap_v2",
                transaction_hash=log['transaction_hash'],
                log_index=log['log_index'],
                block_number=log['block_number'],
            )
            return swap
        elif (topics[0]).casefold() in EVENT_HEX_SIGNATURES["uniswap_v3"]["swap"]:
            decode = decode_log("uniswap_v3", "swap", log)

            amount_in = decode["amount0"]
            amount_out = decode["amount1"]

            if amount_in < 0:
                # buy
                amount0_in = 0
                amount1_in = amount_out
                amount0_out = -amount_in
                amount1_out = 0
            else:
                # sell
                amount0_in = amount_in
                amount1_in = 0
                amount0_out = 0
                amount1_out = -amount_out

            swap = SimpleNamespace(
                contract_address=decode["sender"],
                amount0_in=amount0_in,
                amount1_in=amount1_in,
                amount0_out=amount0_out,
                amount1_out=amount1_out,
                dex_exchange="uniswap_v3",
                transaction_hash=log['transaction_hash'],
                log_index=log['log_index'],
                block_number=log['block_number'],
            )
            return swap
        return None
