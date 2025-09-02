from tqdm.std import tqdm

from src.schemas.python.token_transfer import TokenTransfer
from src.utils.enumeration import EntityType
from src.utils.progress_bar import get_progress_bar
from src.logger import logger
TRANSFER_EVENT_TOPIC = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'


class TokenTransferParser:
    def __init__(self, exporter):
        self.exporter = exporter

    def parse(
        self,
        items: list[dict],
        initial=None,
        total=None,
        batch_size=1,
        show_progress=True,
    ):
        p_bar = get_progress_bar(
            tqdm,
            items,
            initial=(initial or 0) // batch_size,
            total=(total or len(items)) // batch_size,
            show=show_progress,
        )

        for item in p_bar:
            token_transfer = self._parse(item)
            if token_transfer:
                self.exporter.add_item(EntityType.TOKEN_TRANSFER, [token_transfer.model_dump()])

    def _parse(self, item: dict):
        topics = item["topics"]
        if topics is None:
            return None

        if (topics[0]).casefold() == TRANSFER_EVENT_TOPIC:
            # Handle unindexed event fields
            topics_with_data = topics + split_to_words(item["data"])
            # if the number of topics and fields in data part != 4, then it's a weird event
            if len(topics_with_data) != 4:
                logger.warning("The number of topics and data parts is not equal to 4 in log {} of transaction {}"
                               .format(item["log_index"], item["transaction_hash"]))
                return None

            token_transfer = TokenTransfer(
                token_address=item["address"],
                from_address=word_to_address(topics_with_data[1]),
                to_address = word_to_address(topics_with_data[2]),
                value = topics_with_data[3],
                transaction_hash = item["transaction_hash"],
                log_index = item["log_index"],
                block_number = item["block_number"]
            )
            return token_transfer

        return None


def split_to_words(data):
    if data and len(data) > 2:
        data_without_0x = data[2:]
        words = list(chunk_string(data_without_0x, 64))
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
    

def chunk_string(string, length):
    return (string[0 + i:length + i] for i in range(0, len(string), length))

def to_normalized_address(address):
    if address is None or not isinstance(address, str):
        return address
    return address.lower()