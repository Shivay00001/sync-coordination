# Sync Coordination Layer

A Python sync coordination engine that orchestrates synchronization between distributed nodes using vector clocks, operation logging, and conflict resolution with last-write-wins semantics.

## Features

- **Vector Clocks**: Full causality tracking across distributed nodes
- **Operation Logging**: Persistent SQLite-backed operation log
- **Conflict Resolution**: Automatic last-write-wins with vector clock tie-breaking
- **Entity CRUD**: Create, update, delete, and merge operations
- **Remote Sync**: Synchronize operations between multiple nodes
- **State Export**: Full state export for backup and transfer
- **Thread-Safe**: Reentrant lock protection for concurrent access

## Installation

```bash
pip install sync-coordination
```

## Usage

```python
from sync_coordination import SyncCoordinator, OperationType

# Initialize nodes
node1 = SyncCoordinator("node-1", "./sync_store_1")
node2 = SyncCoordinator("node-2", "./sync_store_2")

# Create entity on node 1
op = node1.create_operation(
    OperationType.CREATE, "user:1",
    {"name": "Alice", "email": "alice@example.com"}
)
node1.apply_operation(op)

# Sync to node 2
result = node2.sync_with_remote([op])
print(result)
```

## License

MIT
