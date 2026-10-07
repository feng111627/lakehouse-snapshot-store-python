import os

import uvicorn

from lakehouse.api import create_app
from lakehouse.store import SnapshotStore

store = SnapshotStore(os.environ.get("LAKEHOUSE_ROOT", "./data"))
app = create_app(store)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
