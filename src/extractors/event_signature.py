# import asyncio
# from types import SimpleNamespace

# from rich.progress import (
#     BarColumn,
#     MofNCompleteColumn,
#     Progress,
#     TextColumn,
#     TimeElapsedColumn,
#     TimeRemainingColumn,
# )

# from clients.four_byte import FourByteClient
# from src.utils.enumeration import EntityType


# class EventSignatureExtractor:
#     def __init__(self, exporter, client: FourByteClient):
#         self.exporter = exporter
#         self.client = client

#     async def run(
#         self,
#         hex_signatures,
#         initial=None,
#         total=None,
#         batch_size=1,
#         show_progress=True,
#     ):
#         with Progress(
#             TextColumn("[bold blue]{task.description}"),
#             BarColumn(),
#             MofNCompleteColumn(),
#             TimeElapsedColumn(),
#             TimeRemainingColumn(),
#             disable=not show_progress,
#         ) as progress:
#             task = progress.add_task(
#                 description="Event signature: ",
#                 total=(total or len(hex_signatures)),
#                 completed=(initial or 0),
#             )

#             tasks = []
#             for hex_signature in hex_signatures:
#                 atask = asyncio.create_task(self._run(progress, task, hex_signature, 1))
#                 tasks.append(atask)

#             for coro in asyncio.as_completed(tasks):
#                 result = await coro
#                 if result is None:
#                     continue
#                 self.exporter.add_item(EntityType.EVENT_SIGNATURE, result.model_dump())

#     async def _run(self, progress, task, input, input_size):
#         response = await self.client.get_signature_from_hex(hex_signature=input)
#         res = self._extract(input, response)
#         progress.update(task, advance=input_size)
#         return res

#     def _extract(self, input, response):
#         event_signature = SimpleNamespace(
#             id=response["id"],
#             created_at=response["created_at"],
#             text_signature=response["text_signature"],
#             hex_signature=response["hex_signature"],
#             bytes_signature=response["bytes_signature"],
#         )

#         return event_signature
