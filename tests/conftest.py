"""Make the project's modules importable from tests (they live in folders, not a package)."""
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
for folder in ("ml", "agent", "gateway", "governance", "observability"):
    sys.path.insert(0, str(ROOT / folder))

# gateway/pii_guardrail.py subclasses LiteLLM's CustomGuardrail; tests only need its pure functions.
if "litellm" not in sys.modules:
    stub = types.ModuleType("litellm.integrations.custom_guardrail")
    stub.CustomGuardrail = object
    sys.modules.update({"litellm": types.ModuleType("litellm"),
                        "litellm.integrations": types.ModuleType("litellm.integrations"),
                        "litellm.integrations.custom_guardrail": stub})
