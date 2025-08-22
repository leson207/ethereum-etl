import asyncio

from tqdm.asyncio import tqdm_asyncio
from tqdm.std import tqdm

def get_progress_bar(pbar_type, items, initial, total, show):
    if pbar_type == tqdm_asyncio:
        if show:
            return tqdm_asyncio(items, initial=initial, total=total, smoothing=True)
        else:
            return asyncio.as_completed(items)

    if pbar_type == tqdm:
        if show:
            return tqdm(items, initial=initial, total=total, smoothing=True)
        else:
            return items
