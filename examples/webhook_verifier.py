"""05 -- Best-practice business plugin: webhook verification on an agent.

The full pattern for adding a capability to a real agent:

    capability.py-style class  -- business logic, no framework knowledge
    factory(scope)             -- sync, closes over host surfaces
    install()                  -- every registration pairs with a disposer
    scope.config               -- config injected per generation

This example verifies a Dodo-style payment callback with real HMAC-SHA256:
an attacker who tampers with the payload after signing is rejected by
constant-time comparison, not by the model's judgment.

Run: python examples/05_webhook_verifier.py
"""

import asyncio
import hashlib
import hmac
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agentscope.tool import FunctionTool, Toolkit

from anvilcore import CapabilityDescriptor, Context
from anvilcore.adapters.agentscope import register_tool
from anvilcore.semantic_layer import PluginScope

SECRET = "dodo-webhook-secret-demo"


# ---- pure business logic (testable without any framework) --------------

def verify_webhook_signature(payload: str, signature: str, secret: str) -> str:
    expected = hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return "VALID" if hmac.compare_digest(expected, signature) else "INVALID"


# ---- the plugin shell (~15 lines, no HMAC knowledge) ---------------------

class WebhookVerifierCapability:
    def __init__(self, scope: PluginScope, toolkit: Toolkit):
        self.scope = scope
        self.toolkit = toolkit

    def verify_webhook(self, payload: str, signature: str) -> str:
        verdict = verify_webhook_signature(
            payload, signature, self.scope.config["secret"])
        print(f"    verify_webhook -> {verdict}  payload={payload[:56]}")
        return verdict

    async def install(self) -> None:
        tool = FunctionTool(self.verify_webhook, name="verify_webhook")
        unregister = register_tool(self.toolkit, tool)

        async def publish(collect):
            collect("tool:verify_webhook", unregister)

        await self.scope.effect("webhook-verifier.install", publish)


def make_factory(toolkit: Toolkit):
    def factory(scope: PluginScope):
        return WebhookVerifierCapability(scope, toolkit)
    return factory


async def main() -> None:
    ctx = Context()
    toolkit = Toolkit()
    registry = ctx.registry

    payload = '{"event":"payment.captured","order_id":"D-1001","amount":1990}'
    good_signature = sign(payload, SECRET)
    tampered_payload = payload.replace("D-1001", "D-9999")

    descriptor = CapabilityDescriptor(
        id="webhook-verifier", version="1", factory=make_factory(toolkit),
        config={"secret": SECRET})
    await registry.plugin(descriptor)

    cap = ctx.registry.manager.get("webhook-verifier").instance.instance
    print("[1] authentic callback:")
    cap.verify_webhook(payload, good_signature)

    print("[2] tampered payload + replayed signature (the real attack):")
    cap.verify_webhook(tampered_payload, good_signature)

    print("[3] config hot update: rotate the secret without a restart")
    await registry.update("webhook-verifier", config={"secret": "rotated-v2"})
    # always fetch the FRESH generation -- the old object is retired and
    # still carries its own (old) config; that per-generation isolation is
    # exactly why a rotation can never half-apply
    cap = ctx.registry.manager.get("webhook-verifier").instance.instance
    print("    old signature now fails (rotation works):")
    cap.verify_webhook(payload, good_signature)
    print("    new secret passes:")
    cap.verify_webhook(payload, sign(payload, "rotated-v2"))

    gen = ctx.registry.manager.get("webhook-verifier").installation_generation
    print("[4] plugin generation:", gen, "(bumped by the hot update)")


def sign(payload: str, secret: str) -> str:
    return hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()


if __name__ == "__main__":
    asyncio.run(main())
