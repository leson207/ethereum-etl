from rich.progress import Progress, TaskID


def finish(progress: Progress, task_id: TaskID, batch_size: int, **kwargs):
    progress.advance(task_id=task_id, advance=batch_size)
