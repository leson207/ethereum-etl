
import tqdm
import time
import copy
import itertools
from collections import defaultdict
import httpx
import json
import os

from src.logger import logger
# Quote tokens supported by the system - ordered by priority
QUOTE_TOKENS = {
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": {"symbol": "WETH", "priority": 1},  # WETH
    "0xdac17f958d2ee523a2206206994597c13d831ec7": {"symbol": "USDT", "priority": 2},  # USDT
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": {"symbol": "USDC", "priority": 3},  # USDC
    "0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d": {"symbol": "USD1", "priority": 4},  # USD1
}

# Legacy constant for backward compatibility
WETH_TOKEN = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"

# Price cache for time-based intervals
PRICE_CACHE = {}
CACHE_INTERVAL_SECONDS = 30  # 30 seconds intervals
MAX_CACHE_ENTRIES = 10000  # Limit cache size to prevent memory issues


def get_cache_key(timestamp, interval_seconds=CACHE_INTERVAL_SECONDS):
    """Generate cache key based on timestamp rounded to interval."""
    # Round timestamp down to the nearest interval
    interval_timestamp = (timestamp // interval_seconds) * interval_seconds
    return interval_timestamp


def get_cached_price(quote_symbol, timestamp):
    """Get cached price for the given quote token and timestamp interval."""
    cache_key = get_cache_key(timestamp)
    cache_entry = PRICE_CACHE.get(f"{quote_symbol}_{cache_key}")
    
    if cache_entry is not None:
        if isinstance(cache_entry, dict):
            return cache_entry.get('price')
        else:
            # Handle legacy cache entries (for backward compatibility)
            return cache_entry
    
    return None


def set_cached_price(quote_symbol, timestamp, price):
    """Cache price for the given quote token and timestamp interval."""
    cache_key = get_cache_key(timestamp)
    cache_entry_key = f"{quote_symbol}_{cache_key}"
    
    # Add price with timestamp for cleanup
    PRICE_CACHE[cache_entry_key] = {
        'price': price,
        'timestamp': cache_key,
        'created_at': time.time()
    }
    
    # Clean old entries if cache gets too large
    if len(PRICE_CACHE) > MAX_CACHE_ENTRIES:
        cleanup_old_cache_entries()


def cleanup_old_cache_entries(keep_recent_hours=24):
    """Remove cache entries older than specified hours to prevent memory bloat."""
    current_time = time.time()
    cutoff_time = current_time - (keep_recent_hours * 3600)  # Convert hours to seconds
    
    keys_to_remove = []
    for key, value in PRICE_CACHE.items():
        if isinstance(value, dict) and value.get('created_at', 0) < cutoff_time:
            keys_to_remove.append(key)
    
    for key in keys_to_remove:
        del PRICE_CACHE[key]
    
    logger.info(f"Cleaned up {len(keys_to_remove)} old cache entries")



def get_quote_token_info(token_address):
    """Get quote token information if the token is a supported quote token."""
    return QUOTE_TOKENS.get(token_address.lower())


def determine_quote_token(token0_address, token1_address, token_service):
    """
    Determine which token should be the quote token based on:
    1. Priority order (WETH > USDT > USDC > USD1)
    2. Token creation order (older token becomes quote if both are quote tokens)
    
    Returns: (quote_address, base_address) or (None, None) if no quote token found
    """
    token0_lower = token0_address.lower()
    token1_lower = token1_address.lower()
    
    token0_quote_info = get_quote_token_info(token0_lower)
    token1_quote_info = get_quote_token_info(token1_lower)
    
    # If neither token is a quote token, skip this pair
    if not token0_quote_info and not token1_quote_info:
        return None, None
    
    # If only one is a quote token, use it
    if token0_quote_info and not token1_quote_info:
        return token0_lower, token1_lower
    elif token1_quote_info and not token0_quote_info:
        return token1_lower, token0_lower
    
    # If both are quote tokens, choose based on priority
    if token0_quote_info and token1_quote_info:
        if token0_quote_info["priority"] < token1_quote_info["priority"]:
            return token0_lower, token1_lower
        elif token1_quote_info["priority"] < token0_quote_info["priority"]:
            return token1_lower, token0_lower
        else:
            # Same priority, choose based on token creation order
            try:
                token0 = token_service.get_token(token0_lower)
                token1 = token_service.get_token(token1_lower)
                
                # Use block_timestamp if available, otherwise fall back to alphabetical order
                if hasattr(token0, 'block_timestamp') and hasattr(token1, 'block_timestamp'):
                    if token0.block_timestamp and token1.block_timestamp:
                        # Older token (lower timestamp) becomes quote
                        return (token0_lower, token1_lower) if token0.block_timestamp < token1.block_timestamp else (token1_lower, token0_lower)
                
                # Fallback to alphabetical order for deterministic behavior
                return (token0_lower, token1_lower) if token0_lower < token1_lower else (token1_lower, token0_lower)
                
            except Exception as e:
                logger.warning(f"Error determining token creation order for {token0_lower} and {token1_lower}: {e}")
                # Fallback to alphabetical order
                return (token0_lower, token1_lower) if token0_lower < token1_lower else (token1_lower, token0_lower)


def get_eth_price(timestamp, proxies=None):
    """
    Fetch ETH/USDT price from Binance with optional proxy support.

    Parameters
    ----------
    timestamp : int
        Millisecond timestamp for the Binance API `startTime` query parameter.
    proxies : dict | None
        Mapping with 'http' and/or 'https' proxy URLs. If None, the function will
        attempt to read proxies from the environment variables HTTP_PROXY /
        HTTPS_PROXY (case-insensitive). When no proxy is configured, the request
        is executed directly without using a proxy.
    """
    # Resolve proxies when they are not explicitly supplied
    if proxies is None:
        proxies_env = {
            'http': os.getenv('HTTP_PROXY') or os.getenv('http_proxy'),
            'https': os.getenv('HTTPS_PROXY') or os.getenv('https_proxy'),
        }
        proxies = {k: v for k, v in proxies_env.items() if v}

    start_time = time.time()
    url = f'https://api.binance.com/api/v3/aggTrades?symbol=ETHUSDT&startTime={timestamp}&limit=1'
    cnt = 0
    while cnt < 3:
        try:
            result = httpx.get(url, timeout=5, proxies=proxies if proxies else None)
            result = json.loads(result.content)
            price = float(result[0]['p'])
            return price
        except:
            import traceback
            traceback.print_exc()
            time.sleep(0.5)
            cnt += 1
    end_time = time.time()
    # logger.info(f"get_eth_price | {end_time - start_time}")
    return None

def get_quote_price_in_usd(quote_address, block_timestamp):
    """Get the USD price for the quote token with time-based caching. If price fetch fails, return the most recent cached price for a previous block."""
    quote_info = get_quote_token_info(quote_address)
    
    if not quote_info:
        return 0
    
    quote_symbol = quote_info["symbol"]
    
    if quote_symbol == "WETH":
        # Check cache first
        cached_price = get_cached_price(quote_symbol, block_timestamp)
        if cached_price is not None:
            return cached_price
        
        # Fetch new price
        price = get_eth_price(block_timestamp * 1000)
        if price is not None:
            set_cached_price(quote_symbol, block_timestamp, price)
            return price
        else:
            # If price fetch fails, look for the most recent cached price before this block
            # Search backwards in intervals
            interval = CACHE_INTERVAL_SECONDS
            search_timestamp = block_timestamp - interval
            while search_timestamp >= 0:
                prev_cached_price = get_cached_price(quote_symbol, search_timestamp)
                if prev_cached_price is not None:
                    return prev_cached_price
                search_timestamp -= interval
            return 0
    elif quote_symbol in ["USDT", "USDC", "USD1"]:
        return 1.0  # Stablecoins pegged to USD
    else:
        # TODO: get price from swaps table
        return 0
    return 0


def join(left, right, join_fields, left_fields, right_fields):
    left_join_fields, right_join_fields = join_fields

    if not isinstance(left_join_fields, list):
        left_join_fields = [left_join_fields]

    if not isinstance(right_join_fields, list):
        right_join_fields = [right_join_fields]

    def field_list_to_dict(field_list):
        result_dict = {}
        for field in field_list:
            if isinstance(field, tuple):
                result_dict[field[0]] = field[1]
            else:
                result_dict[field] = field
        return result_dict

    left_fields_as_dict = field_list_to_dict(left_fields)
    right_fields_as_dict = field_list_to_dict(right_fields)

    left_map = defaultdict(list)
    for item in left:
        left_map['_'.join(str(item[left_join_field]) for left_join_field in left_join_fields)].append(item)

    right_map = defaultdict(list)
    for item in right:
        right_map['_'.join(str(item[right_join_field]) for right_join_field in right_join_fields)].append(item)

    for key in left_map.keys():
        for left_item, right_item in itertools.product(left_map[key], right_map[key]):
            result_item = {}
            for src_field, dst_field in left_fields_as_dict.items():
                result_item[dst_field] = left_item.get(src_field)
            for src_field, dst_field in right_fields_as_dict.items():
                result_item[dst_field] = right_item.get(src_field)

            yield result_item


def enrich_swaps(blocks, transactions, swaps, token_service, contract_service, cached_eth_price=True, skip_liquidity_check=False):
    result = list(join(
        swaps, blocks, ('block_number', 'number'),
        [
            'type',
            'contract_address',
            'transaction_hash',
            'log_index',
            'block_number',
            'call',
            'dex_exchange',
            'price_in_quote',
            'amount0_in',
            'amount1_in',
            'amount0_out',
            'amount1_out',
            'token0_address',
            'token1_address',
        ],
        [
            ('timestamp', 'block_timestamp'),
            ('hash', 'block_hash'),
        ]
    ))

    result = list(join(
        result, transactions, ('transaction_hash', 'hash'),
        [
            'type',
            'contract_address',
            'transaction_hash',
            'log_index',
            'block_number',
            'call',
            'dex_exchange',
            'price_in_quote',
            'amount0_in',
            'amount1_in',
            'amount0_out',
            'amount1_out',
            'token0_address',
            'token1_address',
            'block_timestamp',
            'block_hash',
        ],
        [
            ('from_address', 'sender'),
            ('to_address', 'to'),
        ]))

    result.sort(key=lambda x: (int(x['block_number']), int(x['log_index'])))

    new_result = []
    for i, result_item in enumerate(tqdm.tqdm(result)):
        try:
            contract_address = result_item['contract_address'].lower()
            token0_address, token1_address = contract_service.get_tokens(contract_address)

            if result_item['call'] == 'pair_created':
                contract_service.checkpoint_liquidity[contract_address] = result_item['block_timestamp']

            result_item['token0_address'] = token0_address
            result_item['token1_address'] = token1_address

            # Use new quote token determination logic
            quote_address, base_address = determine_quote_token(token0_address, token1_address, token_service)
            
            if quote_address is None:
                logger.info(f'quote_address is None | result_item: {result_item}')
                continue  # Skip pairs without supported quote tokens

            # Get quote price in USD
            if not cached_eth_price:
                quote_price_usd = get_quote_price_in_usd(quote_address, result_item['block_timestamp'])
            else:
                quote_price_usd = get_quote_price_in_usd(quote_address, result_item['block_timestamp'])

            result_item['quote_price_in_usd'] = quote_price_usd

            token0 = token_service.get_token(token0_address)
            token1 = token_service.get_token(token1_address)
            call = ''

            # Determine which token is base and which is quote
            if token0_address == quote_address:
                # token0 is quote, token1 is base
                base_symbol, quote_symbol = token1.symbol, token0.symbol
                total_supply = token1.total_supply
                base_name = token1.name
                if result_item['call'] == 'add':
                    result_item['amount_base'] = result_item['amount1_in'] / 10 ** token1.decimals
                    result_item['amount_quote'] = result_item['amount0_in'] / 10 ** token0.decimals
                elif result_item['call'] == 'remove':
                    result_item['amount_base'] = result_item['amount1_out'] / 10 ** token1.decimals
                    result_item['amount_quote'] = result_item['amount0_out'] / 10 ** token0.decimals
                else:
                    if result_item['amount0_in'] > 0 and result_item['amount1_out'] > 0:
                        call = 'buy'
                        result_item['amount_base'] = result_item['amount1_out'] / 10 ** token1.decimals
                        result_item['amount_quote'] = result_item['amount0_in'] / 10 ** token0.decimals
                    elif result_item['amount0_out'] > 0 and result_item['amount1_in'] > 0:
                        call = 'sell'
                        result_item['amount_base'] = result_item['amount1_in'] / 10 ** token1.decimals
                        result_item['amount_quote'] = result_item['amount0_out'] / 10 ** token0.decimals
                    else:
                        result_item['amount_base'] = 0
                        result_item['amount_quote'] = 0
            else:
                # token1 is quote, token0 is base
                base_symbol, quote_symbol = token0.symbol, token1.symbol
                total_supply = token0.total_supply
                base_name = token0.name
                if result_item['call'] == 'add':
                    result_item['amount_base'] = result_item['amount0_in'] / 10 ** token0.decimals
                    result_item['amount_quote'] = result_item['amount1_in'] / 10 ** token1.decimals
                elif result_item['call'] == 'remove':
                    result_item['amount_base'] = result_item['amount0_out'] / 10 ** token0.decimals
                    result_item['amount_quote'] = result_item['amount1_out'] / 10 ** token1.decimals
                else:
                    if result_item['amount0_in'] > 0 and result_item['amount1_out'] > 0:
                        call = 'sell'
                        result_item['amount_base'] = result_item['amount0_in'] / 10 ** token0.decimals
                        result_item['amount_quote'] = result_item['amount1_out'] / 10 ** token1.decimals
                    elif result_item['amount0_out'] > 0 and result_item['amount1_in'] > 0:
                        call = 'buy'
                        result_item['amount_base'] = result_item['amount0_out'] / 10 ** token0.decimals
                        result_item['amount_quote'] = result_item['amount1_in'] / 10 ** token1.decimals
                    else:
                        result_item['amount_base'] = 0
                        result_item['amount_quote'] = 0

            result_item['base_address'] = base_address
            result_item['base_symbol'] = base_symbol
            result_item['base_name'] = base_name

            result_item['quote_address'] = quote_address
            result_item['quote_symbol'] = quote_symbol

            result_item['amount_usd'] = result_item['quote_price_in_usd'] * result_item['amount_quote']

            # compute price in usd and quote by uniswapv2 and uniswapv3
            if result_item['dex_exchange'] == 'uniswapv2':
                # compute price in usd and quote by uniswapv2 based k = x * y stored in contract_service
                # x = liquid_base, y = liquid_quote
                # k = x * y
                # price_in_quote = y / x = k / x^2
                # (x + amount_base) * (y + amount_quote) = k = x * y
                # amount_base * y + x * amount_quote + amount_base * amount_quote = 0
                # amount_base * k / x + x * amount_quote + amount_base * amount_quote = 0
                # amount_base * k + x^2 * amount_quote + x * amount_base * amount_quote = 0
                # x^2 * amount_quote + x * amount_base * amount_quote + amount_base * k = 0
                # a = amount_quote
                # b = amount_base * amount_quote  
                # c = amount_base * k
                # x = (-b ± sqrt(b^2 - 4ac)) / (2a)
                # x = (-amount_base * amount_quote - sqrt(amount_base ** 2 * amount_quote **2 - 4 * amount_quote * amount_base * k)) / (2 * amount_quote)
                # price_in_quote = k / x^2 = k / ((amount_base * amount_quote) / (amount_quote + k * amount_base))^2
                result_item['price_in_usd'] = (result_item['amount_usd'] / result_item['amount_base']) if result_item['amount_base'] > 0.01 else 0
                result_item['price_in_quote'] = (result_item['amount_quote'] / result_item['amount_base']) if result_item['amount_base'] > 0.01 else 0
            elif result_item['dex_exchange'] == 'uniswapv3':
                # sqrt_price_x96 = result_item['price_in_quote']
                # if sqrt_price_x96:
                #     result_item['price_in_quote'] = 1.0 / (sqrt_price_x96 / 2 ** 96) ** 2
                #     result_item['price_in_usd'] = result_item['price_in_quote'] * result_item['quote_price_in_usd']
                # else:
                #     result_item['price_in_quote'] = 0
                #     result_item['price_in_usd'] = 0
                result_item['price_in_usd'] = (result_item['amount_usd'] / result_item['amount_base']) if result_item['amount_base'] > 0.01 else 0
                result_item['price_in_quote'] = (result_item['amount_quote'] / result_item['amount_base']) if result_item['amount_base'] > 0.01 else 0

            result_item['marketcap_in_usd'] = result_item['price_in_usd'] * total_supply
            result_item['marketcap_in_quote'] = result_item['price_in_quote'] * total_supply

            if contract_service.checkpoint_liquidity.get(contract_address, None) is not None:
                timestamp = contract_service.checkpoint_liquidity[contract_address]
                if result_item['call'] != 'pair_created' and timestamp <= result_item['block_timestamp']:
                    result_item['amount_liquid_base'] = result_item['amount_base']
                    result_item['amount_liquid_quote'] = result_item['amount_quote']
                else:
                    result_item['amount_liquid_base'] = 0
                    result_item['amount_liquid_quote'] = 0

            # swap is not pair_created and contract is not both checkpoint_liquidity and capture pair_created
            if not skip_liquidity_check and result_item['call'] != 'pair_created' and contract_service.checkpoint_liquidity.get(contract_address, None) is None:
                # add swap with call = 'checkpoint_liquidity' and liquid at that time (amount_base, amount_quote)
                result_item_copy = copy.deepcopy(result_item)
                result_item_copy['call'] = 'checkpoint_liquidity'
                if result_item['dex_exchange'] == 'uniswapv3':
                    liquid0 = token_service.get_balance_of(contract_address, token0_address)
                    liquid1 = token_service.get_balance_of(contract_address, token1_address)
                else:
                    liquid0, liquid1 = contract_service.get_liquidity(contract_address)
                
                if token0_address == quote_address:
                    result_item_copy['amount_base'] = liquid1 / 10 ** token1.decimals
                    result_item_copy['amount_quote'] = liquid0 / 10 ** token0.decimals
                else:
                    result_item_copy['amount_base'] = liquid0 / 10 ** token0.decimals
                    result_item_copy['amount_quote'] = liquid1 / 10 ** token1.decimals

                result_item_copy['amount_liquid_base'] = result_item_copy['amount_base']
                result_item_copy['amount_liquid_quote'] = result_item_copy['amount_quote']

                checkpoint_timestamp = int(time.time())
                contract_service.checkpoint_liquidity[contract_address] = checkpoint_timestamp
                logger.info(f'contract_address: {contract_address} | checkpoint_liquidity: {contract_service.checkpoint_liquidity[contract_address]}')
                result_item_copy['block_timestamp'] = checkpoint_timestamp
                logger.info(f'result_item: {result_item}')
                logger.info('-' * 10 + '\n')

                new_result.append(result_item_copy)

            if result_item['call'] is None or result_item['call'].strip() == '':
                result_item['call'] = call
        except:
            import traceback
            traceback.print_exc()
            logger.info(f'error: {traceback.format_exc()}')
            continue

        new_result.append(result_item)

    if len(result) != len(swaps):
        raise ValueError('The number of swaps is wrong ' + str(result))

    return new_result
