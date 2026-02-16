"""
Sync Coordination Layer
Orchestrates synchronization between local and remote stores using
Merkle DAG, event sourcing, and CRDT operations.
"""

import json
import hashlib
import time
import threading
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple, Any
from enum import Enum
from collections import defaultdict
import sqlite3
from pathlib import Path


class SyncStatus(Enum):
    """Status of sync operations"""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    CONFLICT = "conflict"


class OperationType(Enum):
    """Types of sync operations"""
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    MERGE = "merge"


@dataclass
class SyncOperation:
    """Represents a single sync operation"""
    id: str
    type: OperationType
    entity_id: str
    version: int
    data: Dict[str, Any]
    vector_clock: Dict[str, int]
    timestamp: float
    node_id: str
    dependencies: List[str] = field(default_factory=list)
    parent_cid: Optional[str] = None


@dataclass
class SyncState:
    """Tracks synchronization state"""
    node_id: str
    vector_clock: Dict[str, int] = field(default_factory=dict)
    last_sync_time: float = 0.0
    pending_operations: List[str] = field(default_factory=list)
    acknowledged_ops: Set[str] = field(default_factory=set)


@dataclass
class ConflictResolution:
    """Represents a conflict resolution decision"""
    operation_ids: List[str]
    resolution_type: str  # "last_write_wins", "merge", "manual"
    winner_id: Optional[str] = None
    merged_data: Optional[Dict] = None
    timestamp: float = field(default_factory=time.time)


class VectorClock:
    """Vector clock for causality tracking"""
    
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.clock: Dict[str, int] = {node_id: 0}
        self.lock = threading.Lock()
    
    def increment(self):
        """Increment local clock"""
        with self.lock:
            self.clock[self.node_id] += 1
    
    def update(self, other_clock: Dict[str, int]):
        """Merge with another vector clock"""
        with self.lock:
            for node, timestamp in other_clock.items():
                if node not in self.clock:
                    self.clock[node] = timestamp
                else:
                    self.clock[node] = max(self.clock[node], timestamp)
    
    def compare(self, other: Dict[str, int]) -> int:
        """
        Compare with another clock.
        Returns: -1 (less), 0 (concurrent), 1 (greater)
        """
        with self.lock:
            less = False
            greater = False
            
            all_nodes = set(self.clock.keys()) | set(other.keys())
            
            for node in all_nodes:
                v1 = self.clock.get(node, 0)
                v2 = other.get(node, 0)
                
                if v1 < v2:
                    less = True
                elif v1 > v2:
                    greater = True
            
            if less and greater:
                return 0  # Concurrent
            elif less:
                return -1  # Less than
            elif greater:
                return 1  # Greater than
            return 0  # Equal
    
    def get_snapshot(self) -> Dict[str, int]:
        """Get current clock snapshot"""
        with self.lock:
            return self.clock.copy()


class OperationLog:
    """Persistent log of sync operations"""
    
    def __init__(self, storage_path: str):
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.db_path = self.storage_path / "sync_log.db"
        self._init_db()
    
    def _init_db(self):
        """Initialize SQLite database"""
        with sqlite3.connect(str(self.db_path)) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS operations (
                    id TEXT PRIMARY KEY,
                    type TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    data TEXT NOT NULL,
                    vector_clock TEXT NOT NULL,
                    timestamp REAL NOT NULL,
                    node_id TEXT NOT NULL,
                    dependencies TEXT,
                    parent_cid TEXT,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sync_state (
                    node_id TEXT PRIMARY KEY,
                    vector_clock TEXT NOT NULL,
                    last_sync_time REAL NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS conflicts (
                    id TEXT PRIMARY KEY,
                    operation_ids TEXT NOT NULL,
                    resolution_type TEXT NOT NULL,
                    winner_id TEXT,
                    merged_data TEXT,
                    timestamp REAL NOT NULL,
                    resolved INTEGER DEFAULT 0
                )
            """)
            
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ops_entity ON operations(entity_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ops_status ON operations(status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ops_timestamp ON operations(timestamp)")
            conn.commit()
    
    def append_operation(self, op: SyncOperation, status: SyncStatus = SyncStatus.PENDING):
        """Append an operation to the log"""
        with sqlite3.connect(str(self.db_path)) as conn:
            conn.execute("""
                INSERT INTO operations 
                (id, type, entity_id, version, data, vector_clock, timestamp, 
                 node_id, dependencies, parent_cid, status, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                op.id,
                op.type.value,
                op.entity_id,
                op.version,
                json.dumps(op.data),
                json.dumps(op.vector_clock),
                op.timestamp,
                op.node_id,
                json.dumps(op.dependencies),
                op.parent_cid,
                status.value,
                datetime.utcnow().isoformat()
            ))
            conn.commit()
    
    def get_operation(self, op_id: str) -> Optional[SyncOperation]:
        """Retrieve an operation by ID"""
        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.execute("""
                SELECT id, type, entity_id, version, data, vector_clock, 
                       timestamp, node_id, dependencies, parent_cid
                FROM operations WHERE id = ?
            """, (op_id,))
            row = cursor.fetchone()
            
            if not row:
                return None
            
            return SyncOperation(
                id=row[0],
                type=OperationType(row[1]),
                entity_id=row[2],
                version=row[3],
                data=json.loads(row[4]),
                vector_clock=json.loads(row[5]),
                timestamp=row[6],
                node_id=row[7],
                dependencies=json.loads(row[8]) if row[8] else [],
                parent_cid=row[9]
            )
    
    def update_operation_status(self, op_id: str, status: SyncStatus):
        """Update operation status"""
        with sqlite3.connect(str(self.db_path)) as conn:
            conn.execute("""
                UPDATE operations SET status = ? WHERE id = ?
            """, (status.value, op_id))
            conn.commit()
    
    def get_pending_operations(self) -> List[SyncOperation]:
        """Get all pending operations"""
        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.execute("""
                SELECT id, type, entity_id, version, data, vector_clock, 
                       timestamp, node_id, dependencies, parent_cid
                FROM operations 
                WHERE status = ?
                ORDER BY timestamp ASC
            """, (SyncStatus.PENDING.value,))
            
            operations = []
            for row in cursor.fetchall():
                operations.append(SyncOperation(
                    id=row[0],
                    type=OperationType(row[1]),
                    entity_id=row[2],
                    version=row[3],
                    data=json.loads(row[4]),
                    vector_clock=json.loads(row[5]),
                    timestamp=row[6],
                    node_id=row[7],
                    dependencies=json.loads(row[8]) if row[8] else [],
                    parent_cid=row[9]
                ))
            
            return operations
    
    def save_conflict(self, conflict: ConflictResolution) -> str:
        """Save a conflict resolution"""
        conflict_id = hashlib.sha256(
            json.dumps(conflict.operation_ids).encode()
        ).hexdigest()[:16]
        
        with sqlite3.connect(str(self.db_path)) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO conflicts 
                (id, operation_ids, resolution_type, winner_id, merged_data, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                conflict_id,
                json.dumps(conflict.operation_ids),
                conflict.resolution_type,
                conflict.winner_id,
                json.dumps(conflict.merged_data) if conflict.merged_data else None,
                conflict.timestamp
            ))
            conn.commit()
        
        return conflict_id


class SyncCoordinator:
    """
    Main sync coordination engine.
    Orchestrates operations across nodes with conflict resolution.
    """
    
    def __init__(self, node_id: str, storage_path: str):
        self.node_id = node_id
        self.storage_path = Path(storage_path)
        self.vector_clock = VectorClock(node_id)
        self.operation_log = OperationLog(str(self.storage_path))
        
        # In-memory state
        self.entities: Dict[str, Dict] = {}  # entity_id -> current state
        self.entity_versions: Dict[str, int] = {}  # entity_id -> version
        self.entity_clocks: Dict[str, Dict[str, int]] = {}  # entity_id -> vector clock
        
        self.lock = threading.RLock()
    
    def create_operation(
        self,
        op_type: OperationType,
        entity_id: str,
        data: Dict[str, Any],
        dependencies: Optional[List[str]] = None
    ) -> SyncOperation:
        """Create a new sync operation"""
        with self.lock:
            # Increment vector clock
            self.vector_clock.increment()
            
            # Generate operation ID
            op_id = hashlib.sha256(
                f"{self.node_id}:{entity_id}:{time.time()}".encode()
            ).hexdigest()[:16]
            
            # Get current version
            version = self.entity_versions.get(entity_id, 0) + 1
            
            operation = SyncOperation(
                id=op_id,
                type=op_type,
                entity_id=entity_id,
                version=version,
                data=data,
                vector_clock=self.vector_clock.get_snapshot(),
                timestamp=time.time(),
                node_id=self.node_id,
                dependencies=dependencies or []
            )
            
            # Log operation
            self.operation_log.append_operation(operation)
            
            return operation
    
    def apply_operation(self, op: SyncOperation) -> bool:
        """Apply an operation to local state"""
        with self.lock:
            try:
                # Check for conflicts
                if self._has_conflict(op):
                    conflict = self._resolve_conflict(op)
                    if conflict.winner_id != op.id:
                        return False
                
                # Apply based on type
                if op.type == OperationType.CREATE:
                    if op.entity_id in self.entities:
                        return False  # Already exists
                    self.entities[op.entity_id] = op.data
                
                elif op.type == OperationType.UPDATE:
                    if op.entity_id not in self.entities:
                        return False  # Doesn't exist
                    self.entities[op.entity_id].update(op.data)
                
                elif op.type == OperationType.DELETE:
                    if op.entity_id not in self.entities:
                        return False  # Already deleted
                    del self.entities[op.entity_id]
                
                elif op.type == OperationType.MERGE:
                    # Custom merge logic
                    if op.entity_id in self.entities:
                        self.entities[op.entity_id] = self._merge_entities(
                            self.entities[op.entity_id],
                            op.data
                        )
                    else:
                        self.entities[op.entity_id] = op.data
                
                # Update metadata
                self.entity_versions[op.entity_id] = op.version
                self.entity_clocks[op.entity_id] = op.vector_clock
                self.vector_clock.update(op.vector_clock)
                
                # Update operation status
                self.operation_log.update_operation_status(op.id, SyncStatus.COMPLETED)
                
                return True
            
            except Exception as e:
                print(f"Error applying operation: {e}")
                self.operation_log.update_operation_status(op.id, SyncStatus.FAILED)
                return False
    
    def _has_conflict(self, op: SyncOperation) -> bool:
        """Check if operation conflicts with current state"""
        if op.entity_id not in self.entities:
            return False
        
        current_clock = self.entity_clocks.get(op.entity_id, {})
        if not current_clock:
            return False
        
        # Compare entity-specific clock with operation's clock
        entity_vc = VectorClock(self.node_id)
        entity_vc.clock = current_clock.copy()
        comparison = entity_vc.compare(op.vector_clock)
        
        # Concurrent operations indicate conflict
        return comparison == 0
    
    def _resolve_conflict(self, op: SyncOperation) -> ConflictResolution:
        """Resolve conflict using last-write-wins strategy"""
        # Get current state
        current_data = self.entities.get(op.entity_id, {})
        current_timestamp = current_data.get('_timestamp', 0)
        
        # Compare timestamps (last-write-wins)
        if op.timestamp > current_timestamp:
            winner_id = op.id
        else:
            winner_id = None  # Keep current
        
        conflict = ConflictResolution(
            operation_ids=[op.id],
            resolution_type="last_write_wins",
            winner_id=winner_id
        )
        
        self.operation_log.save_conflict(conflict)
        return conflict
    
    def _merge_entities(self, base: Dict, updates: Dict) -> Dict:
        """Merge two entity states"""
        merged = base.copy()
        
        for key, value in updates.items():
            if key.startswith('_'):
                continue  # Skip metadata
            
            if isinstance(value, dict) and key in merged:
                # Recursive merge for nested dicts
                merged[key] = self._merge_entities(merged[key], value)
            else:
                # Simple overwrite
                merged[key] = value
        
        merged['_timestamp'] = time.time()
        return merged
    
    def sync_with_remote(self, remote_operations: List[SyncOperation]) -> Dict[str, Any]:
        """
        Synchronize with remote operations.
        Returns sync summary.
        """
        with self.lock:
            applied = 0
            failed = 0
            conflicts = 0
            
            # Sort by timestamp for causal ordering
            sorted_ops = sorted(remote_operations, key=lambda op: op.timestamp)
            
            for op in sorted_ops:
                if self._has_conflict(op):
                    conflicts += 1
                
                if self.apply_operation(op):
                    applied += 1
                else:
                    failed += 1
            
            # Get pending local operations to send
            pending = self.operation_log.get_pending_operations()
            
            return {
                'applied': applied,
                'failed': failed,
                'conflicts': conflicts,
                'pending_to_send': len(pending),
                'vector_clock': self.vector_clock.get_snapshot()
            }
    
    def get_entity(self, entity_id: str) -> Optional[Dict]:
        """Get current state of an entity"""
        with self.lock:
            return self.entities.get(entity_id)
    
    def get_all_entities(self) -> Dict[str, Dict]:
        """Get all entities"""
        with self.lock:
            return self.entities.copy()
    
    def get_pending_operations(self) -> List[SyncOperation]:
        """Get operations pending sync"""
        return self.operation_log.get_pending_operations()
    
    def export_state(self) -> Dict[str, Any]:
        """Export complete state for backup/transfer"""
        with self.lock:
            return {
                'node_id': self.node_id,
                'vector_clock': self.vector_clock.get_snapshot(),
                'entities': self.entities,
                'entity_versions': self.entity_versions,
                'entity_clocks': self.entity_clocks,
                'timestamp': time.time()
            }
    
    def get_stats(self) -> Dict[str, Any]:
        """Get sync statistics"""
        pending = self.operation_log.get_pending_operations()
        
        return {
            'node_id': self.node_id,
            'entity_count': len(self.entities),
            'pending_operations': len(pending),
            'vector_clock': self.vector_clock.get_snapshot(),
            'total_version': sum(self.entity_versions.values())
        }


# Example usage
if __name__ == "__main__":
    # Initialize coordinators for two nodes
    node1 = SyncCoordinator("node-1", "./sync_store_1")
    node2 = SyncCoordinator("node-2", "./sync_store_2")
    
    # Node 1: Create entity
    op1 = node1.create_operation(
        OperationType.CREATE,
        "user:123",
        {"name": "Alice", "email": "alice@example.com", "_timestamp": time.time()}
    )
    node1.apply_operation(op1)
    print(f"Node 1 created user: {node1.get_entity('user:123')}")
    
    # Node 2: Create same entity (will conflict)
    op2 = node2.create_operation(
        OperationType.CREATE,
        "user:123",
        {"name": "Alice Smith", "email": "alice.smith@example.com", "_timestamp": time.time()}
    )
    node2.apply_operation(op2)
    print(f"Node 2 created user: {node2.get_entity('user:123')}")
    
    # Simulate sync: Node 2 receives Node 1's operation
    result = node2.sync_with_remote([op1])
    print(f"\nSync result: {result}")
    print(f"Node 2 after sync: {node2.get_entity('user:123')}")
    
    # Node 1: Update entity
    op3 = node1.create_operation(
        OperationType.UPDATE,
        "user:123",
        {"status": "active", "_timestamp": time.time()}
    )
    node1.apply_operation(op3)
    
    # Sync both ways
    node1_pending = node1.get_pending_operations()
    node2_pending = node2.get_pending_operations()
    
    node2.sync_with_remote(node1_pending)
    node1.sync_with_remote(node2_pending)
    
    print(f"\nFinal state Node 1: {node1.get_entity('user:123')}")
    print(f"Final state Node 2: {node2.get_entity('user:123')}")
    
    # Get stats
    print(f"\nNode 1 stats: {node1.get_stats()}")
    print(f"Node 2 stats: {node2.get_stats()}")
