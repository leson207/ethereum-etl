import asyncio

import msgpack

from src.configs.connection_manager import connection_manager


async def main():
    msg = await connection_manager["jetstream"].get_last_msg("sample", "ethereum.event")
    print(msg)
    # payload = msgpack.packb(item, default=str, use_bin_type=True)
    decode = msgpack.unpackb(msg.data)
    print(decode)


if __name__ == "__main__":
    with asyncio.Runner() as runner:
        runner.run(connection_manager.init_nats())
        runner.run(main())
