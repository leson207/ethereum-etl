from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from src.fetchers.contract_fetcher import ContractFetcher

class ContractEnricher:
    def __init__(self):
        self.fetcher = ContractFetcher()

    def parse(
        self,
        items: list[dict],
        initial=None,
        total=None,
        batch_size=1,
        show_progress=True,
    ):
        with Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            disable=not show_progress,
        ) as progress:
            task = progress.add_task(
                "Address: ",
                total=(total or len(items)),
                completed=(initial or 0),
            )

# https://api.etherscan.io/v2/api?chainid=1&module=contract&action=getabi&address=0x885d8f2c8e9779499077ff9adfc3f12c6df59538&apikey=2VY1QY3DNZDXDEMSCZFCW6HDVW3A3TEVFF
# enrich contract address with contract service

# ❯  curl -s   -H 'Content-Type: application/json'  "https://api.etherscan.io/v2/api?chainid=1&module=contract&action=getabi&address=0x9c78f1f99f0d50b608d2d752d7af5850db439b70&apikey=2VY1QY3DNZDXDEMSCZFCW6HDVW3A3TEVFF" | jq
# {
#   "status": "0",
#   "message": "NOTOK",
#   "result": "Contract source code not verified"
# }

# ~/Downloads
# ❯  curl -s   -H 'Content-Type: application/json'  "https://api.etherscan.io/v2/api?chainid=1&module=contract&action=getabi&address=0x00a0be1bbc0c99898df7e6524bf16e893c1e3bb9&apikey=2VY1QY3DNZDXDEMSCZFCW6HDVW3A3TEVFF" | jq
# {
#   "status": "1",
#   "message": "OK",
#   "result": "[{\"inputs\":[{\"internalType\":\"address\",\"name\":\"caller\",\"type\":\"address\"}],\"name\":\"CalledWhenPaused\",\"type\":\"error\"},{\"inputs\":[],\"name\":\"CubAlreadyInitialized\",\"type\":\"error\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"fixer\",\"type\":\"address\"}],\"name\":\"FixCallError\",\"type\":\"error\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"fixer\",\"type\":\"address\"},{\"internalType\":\"bytes\",\"name\":\"err\",\"type\":\"bytes\"}],\"name\":\"FixDelegateCallError\",\"type\":\"error\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":false,\"internalType\":\"address\",\"name\":\"previousAdmin\",\"type\":\"address\"},{\"indexed\":false,\"internalType\":\"address\",\"name\":\"newAdmin\",\"type\":\"address\"}],\"name\":\"AdminChanged\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":false,\"internalType\":\"address[]\",\"name\":\"fixes\",\"type\":\"address[]\"}],\"name\":\"AppliedFixes\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"address\",\"name\":\"beacon\",\"type\":\"address\"}],\"name\":\"BeaconUpgraded\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"address\",\"name\":\"implementation\",\"type\":\"address\"}],\"name\":\"Upgraded\",\"type\":\"event\"},{\"stateMutability\":\"payable\",\"type\":\"fallback\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"beacon\",\"type\":\"address\"},{\"internalType\":\"bytes\",\"name\":\"data\",\"type\":\"bytes\"}],\"name\":\"___initializeCub\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address[]\",\"name\":\"fixers\",\"type\":\"address[]\"}],\"name\":\"appliedFixes\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"fixer\",\"type\":\"address\"}],\"name\":\"applyFix\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"stateMutability\":\"payable\",\"type\":\"receive\"}]"
# }
