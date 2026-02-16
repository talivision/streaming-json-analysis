import time
import unittest

from engine import TypeRegistry


class TypeRegistryTests(unittest.TestCase):
    def test_same_shape_different_semantic_values_split(self):
        registry = TypeRegistry()
        now = time.time()

        a = {
            "event": "experiment_exposure",
            "experiment": "checkout_flow_v3",
            "variant": "control",
            "cohort": "new_user",
        }
        b = {
            "event": "experiment_exposure",
            "experiment": "checkout_flow_v3",
            "variant": "treatment",
            "cohort": "new_user",
        }

        type_a, is_new_a = registry.register(a, now)
        type_b, is_new_b = registry.register(b, now)

        self.assertTrue(is_new_a)
        self.assertTrue(is_new_b)
        self.assertNotEqual(type_a, type_b)
        self.assertEqual(len(registry.types), 2)

    def test_optional_field_variant_merges_at_default_thresholds(self):
        registry = TypeRegistry()
        now = time.time()

        base = {"event": "login", "user": "alice", "status": "ok"}
        optional = {
            "event": "login",
            "user": "bob",
            "status": "ok",
            "context": {"source": "web"},
        }

        type_base, is_new_base = registry.register(base, now)
        type_optional, is_new_optional = registry.register(optional, now)

        self.assertTrue(is_new_base)
        self.assertFalse(is_new_optional)
        self.assertEqual(type_base, type_optional)
        self.assertEqual(len(registry.types), 1)

    def test_similarity_threshold_edge_case_can_force_split(self):
        registry = TypeRegistry(similarity_threshold=0.95)
        now = time.time()

        base = {"event": "login", "user": "alice", "status": "ok"}
        optional = {
            "event": "login",
            "user": "bob",
            "status": "ok",
            "context": {"source": "web"},
        }

        type_base, _ = registry.register(base, now)
        type_optional, is_new_optional = registry.register(optional, now)

        self.assertTrue(is_new_optional)
        self.assertNotEqual(type_base, type_optional)
        self.assertEqual(len(registry.types), 2)

    def test_semantic_overlap_threshold_edge_case(self):
        registry = TypeRegistry(semantic_overlap_threshold=0.8)
        now = time.time()

        control = {"event": "experiment_exposure", "variant": "control", "user": "a"}
        treatment = {"event": "experiment_exposure", "variant": "treatment", "user": "b"}

        type_control, _ = registry.register(control, now)
        type_treatment, is_new_treatment = registry.register(treatment, now)

        self.assertTrue(is_new_treatment)
        self.assertNotEqual(type_control, type_treatment)
        self.assertEqual(len(registry.types), 2)


if __name__ == "__main__":
    unittest.main()
