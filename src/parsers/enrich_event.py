from src.parsers.enrich import join
from tqdm.std import tqdm
from src.services.pool_service import PoolService
from src.services.token_service import TokenService
from src.logger import logger
import httpx
import orjson
import copy
import time
QUOTE_TOKENS = {
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": {"symbol": "WETH", "priority": 1},  # WETH
    "0xdac17f958d2ee523a2206206994597c13d831ec7": {"symbol": "USDT", "priority": 2},  # USDT
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": {"symbol": "USDC", "priority": 3},  # USDC
    "0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d": {"symbol": "USD1", "priority": 4},  # USD1
}

def get_quote_token_info(token_address):
    """Get quote token information if the token is a supported quote token."""
    return QUOTE_TOKENS.get(token_address.lower())


def determine_quote_token(token0_address, token1_address):
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



def get_eth_price(timestamp):
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
    url = f'https://api.binance.com/api/v3/aggTrades?symbol=ETHUSDT&startTime={timestamp}&limit=1'
    for _ in range(3):
        result = httpx.get(url, timeout=5, proxies=None)
        result = orjson.loads(result.content)
        price = float(result[0]['p'])
        return price
    
    return None


def get_quote_price_in_usd(quote_address, block_timestamp):
    """Get the USD price for the quote token with time-based caching. If price fetch fails, return the most recent cached price for a previous block."""
    quote_info = get_quote_token_info(quote_address)

    if not quote_info:
        return 0
    
    quote_symbol = quote_info["symbol"]
    
    if quote_symbol == "WETH":
        price = get_eth_price(block_timestamp * 1000)
        return price
    elif quote_symbol in ["USDT", "USDC", "USD1"]:
        return 1.0  # Stablecoins pegged to USD

    return 0


async def enrich_swaps(swaps, blocks, transactions, pool_service:PoolService, token_service: TokenService):
    # TODO: check field of those join later
    result = list(join(
        swaps, blocks, 'block_number', 'number',
        [
            'type',
            'pool_address',
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
        result, transactions, 'transaction_hash', 'hash',
        [
            'type',
            'pool_address',
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
    # TODO: rename result field
    for result_item in tqdm(result):
        # TODO: pool_address to pool_address
        pool_address = result_item['pool_address'].lower()
        token0_address = await pool_service.get_token0_address(pool_address)
        token1_address = await pool_service.get_token1_address(pool_address)

        # if result_item['call'] == 'pair_created':
        #     contract_service.checkpoint_liquidity[pool_address] = result_item['block_timestamp']

        result_item['token0_address'] = token0_address
        result_item['token1_address'] = token1_address

        quote_address, base_address = determine_quote_token(token0_address, token1_address)

        if quote_address is None:
            logger.info(f'quote_address is None | result_item: {result_item}')
            continue  # Skip pairs without supported quote tokens

        quote_price_usd = get_quote_price_in_usd(quote_address, result_item['block_timestamp'])

        result_item['quote_price_in_usd'] = quote_price_usd

        call = ''

        token0_name = await token_service.get_token_name(token0_address)
        token0_symbol = await token_service.get_token_symbol(token0_address)
        token0_decimals = await token_service.get_token_decimals(token0_address)
        token0_total_supply = await token_service.get_token_total_supply(token0_address)

        token1_name = await token_service.get_token_name(token1_address)
        token1_symbol = await token_service.get_token_symbol(token1_address)
        token1_decimals = await token_service.get_token_decimals(token1_address)
        token1_total_supply = await token_service.get_token_total_supply(token1_address)

        # Determine which token is base and which is quote
        # TODO: add decimals of base and quote field to return data
        if token0_address == quote_address:
            # token0 is quote, token1 is base
            base_symbol, quote_symbol = token1_symbol, token0_symbol
            total_supply = token1_total_supply
            base_name = token1_name
            if result_item['call'] == 'mint':
                result_item['amount_base'] = result_item['amount1_in']
                result_item['amount_quote'] = result_item['amount0_in']
            elif result_item['call'] == 'burn':
                result_item['amount_base'] = result_item['amount1_out']
                result_item['amount_quote'] = result_item['amount0_out']
            else:
                if result_item['amount0_in'] > 0 and result_item['amount1_out'] > 0:
                    call = 'buy'
                    result_item['amount_base'] = result_item['amount1_out']
                    result_item['amount_quote'] = result_item['amount0_in']
                elif result_item['amount0_out'] > 0 and result_item['amount1_in'] > 0:
                    call = 'sell'
                    result_item['amount_base'] = result_item['amount1_in']
                    result_item['amount_quote'] = result_item['amount0_out']
                else:
                    result_item['amount_base'] = 0
                    result_item['amount_quote'] = 0
            
        else:
            # token1 is quote, token0 is base
            base_symbol, quote_symbol = token0_symbol, token1_symbol
            total_supply = token0_total_supply
            base_name = token0_name
            if result_item['call'] == 'mint':
                result_item['amount_base'] = result_item['amount0_in']
                result_item['amount_quote'] = result_item['amount1_in']
            elif result_item['call'] == 'burn':
                result_item['amount_base'] = result_item['amount0_out']
                result_item['amount_quote'] = result_item['amount1_out']
            else:
                if result_item['amount0_in'] > 0 and result_item['amount1_out'] > 0:
                    call = 'sell'
                    result_item['amount_base'] = result_item['amount0_in']
                    result_item['amount_quote'] = result_item['amount1_out']
                elif result_item['amount0_out'] > 0 and result_item['amount1_in'] > 0:
                    call = 'buy'
                    result_item['amount_base'] = result_item['amount0_out']
                    result_item['amount_quote'] = result_item['amount1_in']
                else:
                    result_item['amount_base'] = 0
                    result_item['amount_quote'] = 0

        result_item['base_address'] = base_address
        result_item['base_symbol'] = base_symbol
        result_item['base_name'] = base_name

        result_item['quote_address'] = quote_address
        result_item['quote_symbol'] = quote_symbol

        result_item['amount_usd'] = result_item['quote_price_in_usd'] * result_item['amount_quote']

        if result_item['dex'] == 'uniswap_v2':
            result_item['price_in_usd'] = (result_item['amount_usd'] / result_item['amount_base']) if result_item['amount_base'] > 0.01 else 0
            result_item['price_in_quote'] = (result_item['amount_quote'] / result_item['amount_base']) if result_item['amount_base'] > 0.01 else 0
        elif result_item['dex'] == 'uniswap_v3':
            result_item['price_in_usd'] = (result_item['amount_usd'] / result_item['amount_base']) if result_item['amount_base'] > 0.01 else 0
            result_item['price_in_quote'] = (result_item['amount_quote'] / result_item['amount_base']) if result_item['amount_base'] > 0.01 else 0

        result_item['marketcap_in_usd'] = result_item['price_in_usd'] * total_supply
        result_item['marketcap_in_quote'] = result_item['price_in_quote'] * total_supply

        if pool_service.checkpoint_liquidity.get(pool_address, None) is not None:
            timestamp = pool_service.checkpoint_liquidity[pool_address]
            if result_item['call'] != 'pair_created' and timestamp <= result_item['block_timestamp']:
                result_item['amount_liquid_base'] = result_item['amount_base']
                result_item['amount_liquid_quote'] = result_item['amount_quote']
            else:
                result_item['amount_liquid_base'] = 0
                result_item['amount_liquid_quote'] = 0

        # swap is not pair_created and contract is not both checkpoint_liquidity and capture pair_created
        if result_item['call'] != 'pair_created' and pool_service.checkpoint_liquidity.get(pool_address, None) is None:
            # add swap with call = 'checkpoint_liquidity' and liquid at that time (amount_base, amount_quote)
            result_item_copy = copy.deepcopy(result_item)
            result_item_copy['call'] = 'checkpoint_liquidity'
            if result_item['dex_exchange'] == 'uniswapv3':
                liquid0 = pool_service.get_token_balance(pool_address, token0_address)
                liquid1 = pool_service.get_token_balance(pool_address, token1_address)
            else:
                liquid0, liquid1 = pool_service.get_liquidity(pool_address)
            
            if token0_address == quote_address:
                result_item_copy['amount_base'] = liquid1
                result_item_copy['amount_quote'] = liquid0
            else:
                result_item_copy['amount_base'] = liquid0
                result_item_copy['amount_quote'] = liquid1

            result_item_copy['amount_liquid_base'] = result_item_copy['amount_base']
            result_item_copy['amount_liquid_quote'] = result_item_copy['amount_quote']

            checkpoint_timestamp = int(time.time())
            pool_service.checkpoint_liquidity[pool_address] = checkpoint_timestamp
            logger.info(f'pool_address: {pool_address} | checkpoint_liquidity: {pool_service.checkpoint_liquidity[pool_address]}')
            result_item_copy['block_timestamp'] = checkpoint_timestamp
            logger.info(f'result_item: {result_item}')
            logger.info('-' * 10 + '\n')

            new_result.append(result_item_copy)

        if result_item['call'] is None or result_item['call'].strip() == '':
            result_item['call'] = call