import time
import unittest

from engine import BaselineModel, CorrelationEngine, TypeRegistry


class TypeRegistryTests(unittest.TestCase):
    def test_same_shape_different_semantic_values_split(self):
        registry = TypeRegistry(
            semantic_overlap_threshold=0.75,
            semantic_min_support=1,
            semantic_max_unique_ratio=1.0,
            semantic_value_min_count=1,
        )
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
        registry = TypeRegistry(
            semantic_overlap_threshold=0.8,
            semantic_min_support=1,
            semantic_max_unique_ratio=1.0,
            semantic_value_min_count=1,
        )
        now = time.time()

        control = {"event": "experiment_exposure", "variant": "control", "user": "a"}
        treatment = {"event": "experiment_exposure", "variant": "treatment", "user": "b"}

        type_control, _ = registry.register(control, now)
        type_treatment, is_new_treatment = registry.register(treatment, now)

        self.assertTrue(is_new_treatment)
        self.assertNotEqual(type_control, type_treatment)
        self.assertEqual(len(registry.types), 2)

    def test_high_cardinality_string_field_is_not_used_as_discriminator_by_default(self):
        registry = TypeRegistry()
        now = time.time()

        for i in range(20):
            obj = {
                "metric_name": f"wb-name-{i}",
                "value": 1.23,
                "host": "web-1",
            }
            registry.register(obj, now + i)

        # Adaptive discriminator logic should avoid splitting on near-unique strings.
        self.assertEqual(len(registry.types), 1)


class CorrelationEngineTests(unittest.TestCase):
    def test_delayed_observations_captured_in_post_window(self):
        baseline = BaselineModel()
        corr = CorrelationEngine(baseline, post_window_sec=2.0)
        period = corr.add_period(label="login", start=100.0, end=101.0)

        corr.observe_at("type-a", 100.5, {"event": "inside"})
        corr.observe_at("type-a", 101.5, {"event": "delayed"})
        corr.observe_at("type-a", 103.5, {"event": "too_late"})

        rows_all, total_all = corr.raw_observations("login", "type-a", limit=0)
        rows_post, total_post = corr.raw_observations("login", "type-a", limit=0, phase="post")

        self.assertEqual(total_all, 2)
        self.assertEqual(total_post, 1)
        self.assertEqual(rows_post[0]["phase"], "post")
        self.assertEqual(rows_post[0]["period_id"], period.id)

    def test_is_in_period_for_replay_baseline_gating(self):
        baseline = BaselineModel()
        corr = CorrelationEngine(baseline)
        corr.add_period(label="search", start=10.0, end=20.0)

        self.assertTrue(corr.is_in_period(10.0))
        self.assertTrue(corr.is_in_period(15.0))
        self.assertTrue(corr.is_in_period(20.0))
        self.assertFalse(corr.is_in_period(9.9))
        self.assertFalse(corr.is_in_period(20.1))


if __name__ == "__main__":
    unittest.main()
