from utils.event_abi import EVENT_TEXT_SIGNATURES, decode_log

# log= {
#     "address": "0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f",
#     "topics": [
#     "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9",
#     "0x000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
#     "0x000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
#     ],
#     "data": "0x000000000000000000000000f7ae43fabe91b091e3bae07d91b5c8758e97c12b00000000000000000000000081b7e08f65bdf5648606c89998a9cc8164397647",
#     "blockNumber": "0xc5043f",
#     "transactionHash": "0x950395ad0360cb32373c86cc9ff0169cc2aaaa04f9dd42a3ab6aa8b47d3cb2cf",
#     "transactionIndex": "0x1",
#     "blockHash": "0x97b49e43632ac70c46b4003434058b18db0ad809617bd29f3448d46ca9085576",
#     "logIndex": "0x1",
#     "removed": False
# }
# dex = "uniswap_v2"
# event = "pair_created"

# log = {
#     "address": "0x76eb2fe28b36b3ee97f3adae0c69606eedb2a37c",
#     "topics": ["0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f"],
#     "data": "0x0000000000000000000000005d1124fb77c539ec92e3ef853053bbce1e98271b00000000000000000000000000000000000000000000000000000000009896800000000000000000000000000000000000000000000000000000000ffce9ddbc",
#     "block_hash": "0x77eb9434857c149d825a8a6aef9dbec4f85206101bbc5b16892c833b312739cd",
#     "block_number": 23224151,
#     "block_timestamp": 1756197599,
#     "transaction_hash": "0xdb384d184eeb110ef8c4f479d6861b65b805cb37f0f4652aa418202ee6b4f0db",
#     "transaction_index": 31,
#     "log_index": 187,
#     "removed": False,
# }
# dex = "uniswap_v2"
# event = "mint"


log = {
    "address": "0xf82d8ec196fb0d56c6b82a8b1870f09502a49f88",
    "topics": [
        "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822",
        "0x000000000000000000000000df31a70a21a1931e02033dbba7deace6c45cfd0f",
        "0x000000000000000000000000fbf54f7c8b1159d610de4b196ab474caecb2aaf8",
    ],
    "data": "0x0000000000000000000000000000000000000000000000000cc1350510a6079500000000000000000000000000000000000000000000000000204bd3a8ce710800000000000000000000000000000000000000000000002088892df6bf22ab0b0000000000000000000000000000000000000000000000000000000000000000",
    "block_hash": "0x88ce906e988e5bdbe69e5502ac27774e3bf5a7cdb962cfa15ebb296ea09b3b07",
    "block_number": 23224079,
    "block_timestamp": 1756196735,
    "transaction_hash": "0x2f159af68fdb4d466522d8cf89a3937f6e231c8ff75e980414a5b96146bcc0af",
    "transaction_index": 101,
    "log_index": 257,
    "removed": False,
}
dex = "uniswap_v2"
event = "swap"
print(EVENT_TEXT_SIGNATURES[dex][event])
res = decode_log(dex, event, log)
print(res)
