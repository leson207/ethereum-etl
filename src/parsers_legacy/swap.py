
import logging
from builtins import map
from utils.event_abi import TOPIC_EVENT_HASHES
from types import SimpleNamespace



# https://ethereum.stackexchange.com/questions/12553/understanding-logs-and-log-blooms
# [uniswapv3, uniswapv2]
UNISWAPV2_SWAP_EVENT_TOPIC = TOPIC_EVENT_HASHES["uniswap_v2"]["Swap"]
UNISWAPV2_MINT_EVENT_TOPIC = TOPIC_EVENT_HASHES["uniswap_v2"]["Mint"]
UNISWAPV2_BURN_EVENT_TOPIC = TOPIC_EVENT_HASHES["uniswap_v2"]["Burn"]
UNISWAPV2_PAIR_CREATED_EVENT_TOPIC = TOPIC_EVENT_HASHES["uniswap_v2"]["PairCreated"]

UNISWAPV3_SWAP_EVENT_TOPIC = TOPIC_EVENT_HASHES["uniswap_v3"]["Swap"]
UNISWAPV3_MINT_EVENT_TOPIC = TOPIC_EVENT_HASHES["uniswap_v3"]["Mint"]
UNISWAPV3_BURN_EVENT_TOPIC = TOPIC_EVENT_HASHES["uniswap_v3"]["Burn"]
UNISWAPV3_PAIR_CREATED_EVENT_TOPIC = TOPIC_EVENT_HASHES["uniswap_v3"]["PoolCreated"]

SWAP_EVENT_TOPICS = [UNISWAPV2_SWAP_EVENT_TOPIC, UNISWAPV3_SWAP_EVENT_TOPIC]
TOTAL_EVENT_TOPICS = [UNISWAPV2_SWAP_EVENT_TOPIC, UNISWAPV3_SWAP_EVENT_TOPIC, UNISWAPV2_MINT_EVENT_TOPIC, UNISWAPV2_BURN_EVENT_TOPIC, UNISWAPV2_PAIR_CREATED_EVENT_TOPIC, UNISWAPV3_MINT_EVENT_TOPIC, UNISWAPV3_BURN_EVENT_TOPIC, UNISWAPV3_PAIR_CREATED_EVENT_TOPIC]
# TOTAL_EVENT_TOPICS = [UNISWAPV2_SWAP_EVENT_TOPIC]
logger = logging.getLogger(__name__)

WETH = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'



class EthSwapExtractor(object):
    def extract_swap_from_log(self, receipt_log):
        topics = receipt_log.topics
        if topics is None or len(topics) < 1:
            # This is normal, topics can be empty for anonymous events
            return None
        if (topics[0]).casefold() == UNISWAPV2_PAIR_CREATED_EVENT_TOPIC:
            # Handle unindexed event fields
            topics_with_data = topics + split_to_words(receipt_log.data)
            # if the number of topics and fields in data part != 5, then it's a weird event
            if len(topics_with_data) != 5:
                logger.warning("The number of topics and data parts is not equal to 5 in log {} of transaction {}"
                               .format(receipt_log.log_index, receipt_log.transaction_hash))
                return None

            swap = SimpleNamespace(
                contract_address = word_to_address(topics_with_data[3]),
                call = 'pair_created',
                dex_exchange = 'uniswapv2',
                transaction_hash = receipt_log.transaction_hash,
                amount0_in = 0,
                amount1_in = 0,
                amount0_out = 0,
                amount1_out = 0,
                log_index = receipt_log.log_index,
                block_number = receipt_log.block_number,
            )
            
            return swap
        elif (topics[0]).casefold() == UNISWAPV3_PAIR_CREATED_EVENT_TOPIC:
            # Handle unindexed event fields
            topics_with_data = topics + split_to_words(receipt_log.data)
            # if the number of topics and fields in data part != 5, then it's a weird event
            if len(topics_with_data) != 6:
                logger.warning("The number of topics and data parts is not equal to 5 in log {} of transaction {}"
                               .format(receipt_log.log_index, receipt_log.transaction_hash))
                return None

            swap = SimpleNamespace(
                contract_address = word_to_address(topics_with_data[5]),
                call = 'pair_created',
                dex_exchange = 'uniswapv3',
                transaction_hash = receipt_log.transaction_hash,
                amount0_in = 0,
                amount1_in = 0,
                amount0_out = 0,
                amount1_out = 0,
                log_index = receipt_log.log_index,
                block_number = receipt_log.block_number
            )
            return swap
        elif (topics[0]).casefold() in UNISWAPV2_MINT_EVENT_TOPIC:
            # Handle unindexed event fields
            topics_with_data = topics + split_to_words(receipt_log.data)
            # if the number of topics and fields in data part != 4, then it's a weird event
            if len(topics_with_data) != 4:
                logger.warning("The number of topics and data parts is not equal to 4 in log {} of transaction {}"
                               .format(receipt_log.log_index, receipt_log.transaction_hash))
                return None

            swap = SimpleNamespace(
                contract_address = to_normalized_address(receipt_log.address),
                call = 'add',
                dex_exchange = 'uniswapv2',
                amount0_in = hex_to_dec(topics_with_data[2]),
                amount1_in = hex_to_dec(topics_with_data[3]),
                amount0_out = 0,
                amount1_out = 0,
                transaction_hash = receipt_log.transaction_hash,
                log_index = receipt_log.log_index,
                block_number = receipt_log.block_number,
            )
            return swap
        elif (topics[0]).casefold() in UNISWAPV3_MINT_EVENT_TOPIC:
            # Handle unindexed event fields
            topics_with_data = topics + split_to_words(receipt_log.data)
            # if the number of topics and fields in data part != 4, then it's a weird event
            if len(topics_with_data) != 8:
                logger.warning("The number of topics and data parts is not equal to 4 in log {} of transaction {}"
                               .format(receipt_log.log_index, receipt_log.transaction_hash))
                return None

            swap = SimpleNamespace(
                contract_address = to_normalized_address(receipt_log.address),
                call = 'add',
                dex_exchange = 'uniswapv3',
                amount0_in = hex_to_dec(topics_with_data[6]),
                amount1_in = hex_to_dec(topics_with_data[7]),
                amount0_out = 0,
                amount1_out = 0,
                transaction_hash = receipt_log.transaction_hash,
                log_index = receipt_log.log_index,
                block_number = receipt_log.block_number
            )
            return swap
        elif (topics[0]).casefold() in UNISWAPV2_BURN_EVENT_TOPIC:
            # Handle unindexed event fields
            topics_with_data = topics + split_to_words(receipt_log.data)
            # if the number of topics and fields in data part != 4, then it's a weird event
            if len(topics_with_data) != 5:
                logger.warning("The number of topics and data parts is not equal to 5 in log {} of transaction {}"
                               .format(receipt_log.log_index, receipt_log.transaction_hash))
                return None

            swap = SimpleNamespace(
                contract_address = to_normalized_address(receipt_log.address),
                call = 'remove',
                dex_exchange = 'uniswapv2',
                amount0_in = 0,
                amount1_in = 0,
                amount0_out = hex_to_dec(topics_with_data[3]),
                amount1_out = hex_to_dec(topics_with_data[4]),
                transaction_hash = receipt_log.transaction_hash,
                log_index = receipt_log.log_index,
                block_number = receipt_log.block_number
            )
            return swap
        elif (topics[0]).casefold() in UNISWAPV3_BURN_EVENT_TOPIC:
            # Handle unindexed event fields
            topics_with_data = topics + split_to_words(receipt_log.data)
            # if the number of topics and fields in data part != 4, then it's a weird event
            if len(topics_with_data) != 7:
                logger.warning("The number of topics and data parts is not equal to 5 in log {} of transaction {}"
                               .format(receipt_log.log_index, receipt_log.transaction_hash))
                return None

            swap = SimpleNamespace(
                contract_address = to_normalized_address(receipt_log.address),
                call = 'remove',
                dex_exchange = 'uniswapv3',
                amount0_in = 0,
                amount1_in = 0,
                amount0_out = hex_to_dec(topics_with_data[5]),
                amount1_out = hex_to_dec(topics_with_data[6]),
                transaction_hash = receipt_log.transaction_hash,
                log_index = receipt_log.log_index,
                block_number = receipt_log.block_number
            )
            return swap
        elif (topics[0]).casefold() in SWAP_EVENT_TOPICS:
            # Handle unindexed event fields
            topics_with_data = topics + split_to_words(receipt_log.data)
            # if the number of topics and fields in data part != 4, then it's a weird event
            if len(topics_with_data) != 7 and len(topics_with_data) != 8:
                logger.warning("The number of topics and data parts is not equal to 7 and 8 in log {} of transaction {}"
                               .format(receipt_log.log_index, receipt_log.transaction_hash))
                return None

            swap = SimpleNamespace()
            # swap.call = 'swap'
            swap.contract_address = to_normalized_address(receipt_log.address)
            if topics[0] == UNISWAPV3_SWAP_EVENT_TOPIC:
                amount_in = hex_to_dec(topics_with_data[3], signed=True)
                amount_out = hex_to_dec(topics_with_data[4], signed=True)
                sqrt_price_x96 = int(hex_to_dec(topics_with_data[5]))
                if amount_in < 0:
                    # buy
                    swap.amount0_in = 0
                    swap.amount1_in = amount_out
                    swap.amount0_out = -amount_in
                    swap.amount1_out = 0
                else:
                    # sell
                    # bug: swap amount_in and amount_out
                    # swap.amount0_in = 0
                    # swap.amount1_in = amount_in
                    # swap.amount0_out = -amount_out
                    # swap.amount1_out = 0
                    # fixed:
                    swap.amount0_in = amount_in
                    swap.amount1_in = 0
                    swap.amount0_out = 0
                    swap.amount1_out = -amount_out
                swap.price_in_quote = sqrt_price_x96
                swap.dex_exchange = 'uniswapv3'
            else:
                swap.amount0_in = hex_to_dec(topics_with_data[3])
                swap.amount1_in = hex_to_dec(topics_with_data[4])
                swap.amount0_out = hex_to_dec(topics_with_data[5])
                swap.amount1_out = hex_to_dec(topics_with_data[6])
                swap.dex_exchange = 'uniswapv2'
            swap.transaction_hash = receipt_log.transaction_hash
            swap.log_index = receipt_log.log_index
            swap.block_number = receipt_log.block_number
            return swap
        return None


def chunk_string(string, length):
    return (string[0 + i:length + i] for i in range(0, len(string), length))

def to_normalized_address(address):
    if address is None or not isinstance(address, str):
        return address
    return address.lower()

def hex_to_dec(hex_string, signed=False):
    if hex_string is None:
        return None
    try:
        if signed:
            return int.from_bytes(bytes.fromhex(hex_string[2:]), "big", signed=signed)
        else:
            return int(hex_string, 16)
    except ValueError:
        print("Not a hex string %s" % hex_string)
        return hex_string

def split_to_words(data, length=64):
    if data and len(data) > 2:
        data_without_0x = data[2:]
        words = list(chunk_string(data_without_0x, length))
        words_with_0x = list(map(lambda word: '0x' + word, words))
        return words_with_0x
    return []


def word_to_address(param):
    if param is None:
        return None
    elif len(param) >= 40:
        return to_normalized_address('0x' + param[-40:])
    else:
        return to_normalized_address(param)

