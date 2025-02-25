import unittest
from unittest.mock import MagicMock
from abc import ABC
from rpcx.server import RPCManager, RPCService


class TestService(RPCService):
    def test_method(self):
        return "test_method_called"

    def add(self, a: int, b: int) -> int:
        return a + b

class AnotherTestService(RPCService):
    def another_test_method(self):
        return "another_test_method_called"
    
    def multiply(self, a: int, b: int) -> int:
        return a * b

class RPCManagerTest(unittest.TestCase):
    def setUp(self):
        self.rpc_manager = RPCManager()

    def test_happy_path(self):
        method = self.rpc_manager.get_method("TestService", "test_method")
        self.assertIsNotNone(method)
        self.assertEqual(method.name, "test_method")
        self.assertTrue(callable(method.func))

    def test_service_not_found(self):
        method = self.rpc_manager.get_method("NonExistentService", "test_method")
        self.assertIsNone(method)

    def test_method_not_found(self):
        method = self.rpc_manager.get_method("AnotherTestService", "non_existent_method")
        self.assertIsNone(method)

    def test_method_not_callable(self):
        class NonCallableMethodService(RPCService):
            test_method = "not_callable"

        self.rpc_manager.services["NonCallableMethodService"] = NonCallableMethodService()
        method = self.rpc_manager.get_method("NonCallableMethodService", "test_method")
        self.assertIsNone(method)

    def test_multiple_services(self):
        method1 = self.rpc_manager.get_method("TestService", "test_method")
        method2 = self.rpc_manager.get_method("AnotherTestService", "another_test_method")
        self.assertIsNotNone(method1)
        self.assertIsNotNone(method2)
        self.assertEqual(method1.name, "test_method")
        self.assertEqual(method2.name, "another_test_method")

    def test_direct_subclass_only(self):
        class IndirectSubclass(TestService):
            pass

        self.rpc_manager = RPCManager()
        method = self.rpc_manager.get_method("IndirectSubclass", "test_method")
        self.assertIsNone(method)

    def test_service_with_instance_methods(self):
        service = TestService()
        self.rpc_manager.services["TestService"] = service
        method = self.rpc_manager.get_method("TestService", "test_method")
        self.assertIsNotNone(method)
        self.assertEqual(method.name, "test_method")
        self.assertTrue(callable(method.func))

    def test_service_call_method(self):
        service = TestService()
        add_method = self.rpc_manager.get_method("TestService", "add")
        self.assertIsNotNone(add_method)
        self.assertEqual(add_method.name, "add")
        self.assertTrue(callable(add_method.func))
        result = add_method.func(1, 2)
        self.assertEqual(result, 3)

if __name__ == '__main__':
    unittest.main()