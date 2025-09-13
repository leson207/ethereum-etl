import asyncio

if __name__ == "__main__":
    from src.configs.nats_conn import nats_init
    asyncio.run(nats_init())
    from src.configs.nats_conn import jetstream
    asyncio.run(jetstream.publish("ethereum.test", b"first"))
    print("8"*100)
