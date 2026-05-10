# topos.bound.plane.variants
"""
@desc: Verifies structural invariants of the topos flow field.

@regimes:
- Laminar Regime: low-density ψ events project directly to Φ surface
- Turbulence Regime: high-density ψ burst triggers folding compression
- Relaxation Regime: accumulated pressure releases via delayed flush or explicit flush
- Context Topos: nested flow scopes preserve phase inheritance
"""
import unittest
import time
from topos.bound.plane.emitter import get_emitter, flow_scope, _flow_context
from topos.bound.plane.surface import SurfacePlane

class ToposVariants(unittest.TestCase):
    def setUp(self):
        """
        @phase.init: Reset the BoundPlane phase field before each test.

        Φ(t₀): vacuum state
        - meter history cleared
        - folding cache cleared
        - flow context reset
        """
        SurfacePlane.meter.history.clear()
        SurfacePlane.fold_cache.clear()
        _flow_context.set({})
        
        # [topos Sensor]: 개별 노드에 부착되는 위상 발화기
        self.emitter = get_emitter("test.engine", phase="verify", boundary="core")

    def test_laminar_flow(self):
        """
        @invariant: low-density ψ events remain uncompressed.
        @flow: ψ → Φ(surface)
        """
        with flow_scope(flow_id="FLOW-NORMAL"):
            self.emitter.info("system_initialized")
            self.emitter.info("heartbeat_pulse")
            
        ## laminar regime must not trigger folding
        self.assertEqual(len(SurfacePlane.fold_cache), 0, "Laminar flow should not trigger folding.")
        
        ## events must exist in pressure meter history
        key1 = "verify:test.engine:system_initialized"
        key2 = "verify:test.engine:heartbeat_pulse"
        self.assertIn(key1, SurfacePlane.meter.history)
        self.assertIn(key2, SurfacePlane.meter.history)

    def test_turbulence_and_folding(self):
        """
        @invariant: high-density ψ bursts must trigger structural folding.
        @flow: ψ burst -> boundary pressure -> folding compression -> summary node Φ'
        """
        target_msg = "processing_unit"
        key = f"verify:test.engine:{target_msg}"

        with flow_scope(flow_id="FLOW-BURST"):
            for _ in range(50):
                self.emitter.info(target_msg)
        
        ## turbulence must generate folding
        self.assertIn(key, SurfacePlane.fold_cache, "High density events must be caught in fold_cache.")
        
        folded_event = SurfacePlane.fold_cache[key]

        ## compression magnitude must reflect burst density
        self.assertGreater(folded_event.fold_count, 1, "Fold count should reflect the burst volume.")

        ## event type must mutate into summary node
        self.assertEqual(folded_event.kind, "summary", "Event kind must be mutated to 'summary'.")

    def test_attenuation_and_implicit_flush(self):
        """
        @invariant: accumulated folding must release after pressure decay via new event
        @flow: accumulation -> attenuation window -> new ψ event -> delayed flush
        """
        target_msg = "spam_signal"
        key = f"verify:test.engine:{target_msg}"

        ## induce turbulence to trap events
        for _ in range(20):
            self.emitter.warn(target_msg) # warn 래퍼 사용 테스트
        
        self.assertIn(key, SurfacePlane.fold_cache)
        
        ## simulate pressure relaxation
        time.sleep(SurfacePlane.meter.window + 0.1)
        
        ## new event triggers implicit flush
        # self.emitter.signal("recovery_signal")
        self.emitter.info(target_msg)

        ## folding cache must be cleared
        self.assertNotIn(key, SurfacePlane.fold_cache, "Fold cache must be flushed implicitly after pressure drops.")

    def test_explicit_flush(self):
        """
        @invariant: explicit flush forces all folded events to surface
        @flow: accumulation -> BoundPlane.flush() -> Surface
        """
        target_msg = "silent_burst"
        key = f"verify:test.engine:{target_msg}"

        ## induce turbulence
        for _ in range(20):
            self.emitter.crit(target_msg) # crit 래퍼 사용 테스트
            
        self.assertIn(key, SurfacePlane.fold_cache)
        
        ## 강제 플러시 호출 (시간 지연이나 새 이벤트 없이 즉각 방출)
        SurfacePlane.flush()
        
        self.assertEqual(len(SurfacePlane.fold_cache), 0, "Explicit flush must clear all folded events.")

    def test_context_nesting_integrity(self):
        """
        @invariant: nested flow scopes preserve hierarchical context.
        @flow: Φ_parent -> Φ_child (override) -> Φ_parent (restore)
        """
        with flow_scope(flow_id="FLOW-PARENT", phase="routing"):
            parent_ctx = _flow_context.get()
            self.assertEqual(parent_ctx.get("flow_id"), "FLOW-PARENT")
            self.assertEqual(parent_ctx.get("phase"), "routing")
            
            with flow_scope(flow_id="FLOW-CHILD", boundary="child_worker"):
                child_ctx = _flow_context.get()
                ## child overrides flow_id
                self.assertEqual(child_ctx.get("flow_id"), "FLOW-CHILD")

                ## child defines its own boundary
                self.assertEqual(child_ctx.get("boundary"), "child_worker")

                ## phase must inherit from parent
                self.assertEqual(child_ctx.get("phase"), "routing")
                
            ## after child exit, parent context must be restored
            restored_ctx = _flow_context.get()
            self.assertEqual(restored_ctx.get("flow_id"), "FLOW-PARENT")
            self.assertIsNone(restored_ctx.get("boundary"))

if __name__ == "__main__":
    print("## Initiating Topos Structural Verification for BoundPlane...")
    unittest.main(verbosity=2)