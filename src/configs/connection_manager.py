from src.configs.nats_conn import nats_init


class ConnectionManager:
    async def create(self):
        self.nats_conn, self.jetstream = await nats_init()

    def close(self):
        # close all connection
        pass
