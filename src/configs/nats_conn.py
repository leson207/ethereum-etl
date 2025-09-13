import asyncio

import nats

from src.configs.environment import env
from src.logger import logger

nats_conn = None
jetstream = None

async def nats_init():
    global nats_conn, jetstream
    nats_conn = await nats.connect(env.NATS_SERVER)
    jetstream = nats_conn.jetstream()

    name = "ETHEREUM" + env.ENVIRONMENT_NAME
    await jetstream.add_stream(name=name, subjects=["ethereum.*"])
    logger.info(f"Created stream '{name}'")

