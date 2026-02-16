"""Tests for Sync Coordination Layer"""
import os
import shutil
import tempfile
import time
import unittest

from sync_coordination import (
    SyncCoordinator, OperationType, VectorClock, OperationLog
)


class TestVectorClock(unittest.TestCase):
    def test_increment(self):
        vc = VectorClock("node-1")
        vc.increment()
        snap = vc.get_snapshot()
        self.assertEqual(snap["node-1"], 1)

    def test_update(self):
        vc = VectorClock("node-1")
        vc.increment()
        vc.update({"node-2": 5})
        snap = vc.get_snapshot()
        self.assertEqual(snap["node-2"], 5)

    def test_compare_less(self):
        vc = VectorClock("node-1")
        other = {"node-1": 5}
        self.assertEqual(vc.compare(other), -1)

    def test_compare_greater(self):
        vc = VectorClock("node-1")
        vc.increment()
        vc.increment()
        other = {"node-1": 1}
        self.assertEqual(vc.compare(other), 1)

    def test_compare_concurrent(self):
        vc = VectorClock("node-1")
        vc.increment()
        other = {"node-2": 1}
        self.assertEqual(vc.compare(other), 0)


class TestOperationLog(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.log = OperationLog(self.test_dir)

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_append_and_get(self):
        from sync_coordination import SyncOperation, SyncStatus
        op = SyncOperation(
            id="op-1",
            type=OperationType.CREATE,
            entity_id="user:1",
            version=1,
            data={"name": "Alice"},
            vector_clock={"node-1": 1},
            timestamp=time.time(),
            node_id="node-1"
        )
        self.log.append_operation(op)
        retrieved = self.log.get_operation("op-1")
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.entity_id, "user:1")

    def test_pending_operations(self):
        from sync_coordination import SyncOperation
        for i in range(3):
            op = SyncOperation(
                id=f"op-{i}",
                type=OperationType.CREATE,
                entity_id=f"entity:{i}",
                version=1,
                data={},
                vector_clock={},
                timestamp=time.time(),
                node_id="node-1"
            )
            self.log.append_operation(op)

        pending = self.log.get_pending_operations()
        self.assertEqual(len(pending), 3)


class TestSyncCoordinator(unittest.TestCase):
    def setUp(self):
        self.test_dir1 = tempfile.mkdtemp()
        self.test_dir2 = tempfile.mkdtemp()
        self.node1 = SyncCoordinator("node-1", self.test_dir1)
        self.node2 = SyncCoordinator("node-2", self.test_dir2)

    def tearDown(self):
        shutil.rmtree(self.test_dir1, ignore_errors=True)
        shutil.rmtree(self.test_dir2, ignore_errors=True)

    def test_create_entity(self):
        op = self.node1.create_operation(
            OperationType.CREATE, "user:1",
            {"name": "Alice", "_timestamp": time.time()}
        )
        result = self.node1.apply_operation(op)
        self.assertTrue(result)

        entity = self.node1.get_entity("user:1")
        self.assertIsNotNone(entity)
        self.assertEqual(entity["name"], "Alice")

    def test_update_entity(self):
        op1 = self.node1.create_operation(
            OperationType.CREATE, "user:1",
            {"name": "Alice", "_timestamp": time.time()}
        )
        self.node1.apply_operation(op1)

        op2 = self.node1.create_operation(
            OperationType.UPDATE, "user:1",
            {"email": "alice@example.com", "_timestamp": time.time()}
        )
        self.node1.apply_operation(op2)

        entity = self.node1.get_entity("user:1")
        self.assertEqual(entity["email"], "alice@example.com")

    def test_delete_entity(self):
        op1 = self.node1.create_operation(
            OperationType.CREATE, "user:1",
            {"name": "Alice", "_timestamp": time.time()}
        )
        self.node1.apply_operation(op1)

        op2 = self.node1.create_operation(
            OperationType.DELETE, "user:1", {}
        )
        self.node1.apply_operation(op2)

        entity = self.node1.get_entity("user:1")
        self.assertIsNone(entity)

    def test_sync_between_nodes(self):
        op = self.node1.create_operation(
            OperationType.CREATE, "user:1",
            {"name": "Alice", "_timestamp": time.time()}
        )
        self.node1.apply_operation(op)

        result = self.node2.sync_with_remote([op])
        self.assertEqual(result['applied'], 1)

        entity = self.node2.get_entity("user:1")
        self.assertIsNotNone(entity)
        self.assertEqual(entity["name"], "Alice")

    def test_merge_operation(self):
        op1 = self.node1.create_operation(
            OperationType.CREATE, "user:1",
            {"name": "Alice", "_timestamp": time.time()}
        )
        self.node1.apply_operation(op1)

        op2 = self.node1.create_operation(
            OperationType.MERGE, "user:1",
            {"email": "alice@example.com"}
        )
        self.node1.apply_operation(op2)

        entity = self.node1.get_entity("user:1")
        self.assertEqual(entity["email"], "alice@example.com")

    def test_get_all_entities(self):
        for i in range(3):
            op = self.node1.create_operation(
                OperationType.CREATE, f"user:{i}",
                {"name": f"User {i}", "_timestamp": time.time()}
            )
            self.node1.apply_operation(op)

        entities = self.node1.get_all_entities()
        self.assertEqual(len(entities), 3)

    def test_export_state(self):
        op = self.node1.create_operation(
            OperationType.CREATE, "user:1",
            {"name": "Test", "_timestamp": time.time()}
        )
        self.node1.apply_operation(op)

        state = self.node1.export_state()
        self.assertEqual(state['node_id'], 'node-1')
        self.assertEqual(len(state['entities']), 1)

    def test_stats(self):
        stats = self.node1.get_stats()
        self.assertEqual(stats['node_id'], 'node-1')
        self.assertEqual(stats['entity_count'], 0)


if __name__ == "__main__":
    unittest.main()
