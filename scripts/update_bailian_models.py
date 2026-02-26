#!/usr/bin/env python3
import argparse
import json
from copy import deepcopy
from pathlib import Path
import yaml

PROVIDER_NAME = "bailian"
BASE_URL = "https://coding.dashscope.aliyuncs.com/v1"

MODEL_ITEMS = [
    {"id": "qwen3.5-plus", "name": "qwen3.5-plus", "reasoning": False, "input": ["text", "image"], "cost": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0}, "contextWindow": 1000000, "maxTokens": 65536},
    {"id": "qwen3-max-2026-01-23", "name": "qwen3-max-2026-01-23", "reasoning": False, "input": ["text"], "cost": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0}, "contextWindow": 262144, "maxTokens": 65536},
    {"id": "qwen3-coder-next", "name": "qwen3-coder-next", "reasoning": False, "input": ["text"], "cost": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0}, "contextWindow": 262144, "maxTokens": 65536},
    {"id": "qwen3-coder-plus", "name": "qwen3-coder-plus", "reasoning": False, "input": ["text"], "cost": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0}, "contextWindow": 1000000, "maxTokens": 65536},
    {"id": "MiniMax-M2.5", "name": "MiniMax-M2.5", "reasoning": False, "input": ["text"], "cost": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0}, "contextWindow": 1000000, "maxTokens": 65536},
    {"id": "glm-5", "name": "glm-5", "reasoning": False, "input": ["text"], "cost": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0}, "contextWindow": 202752, "maxTokens": 16384},
    {"id": "glm-4.7", "name": "glm-4.7", "reasoning": False, "input": ["text"], "cost": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0}, "contextWindow": 202752, "maxTokens": 16384},
    {"id": "kimi-k2.5", "name": "kimi-k2.5", "reasoning": False, "input": ["text", "image"], "cost": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0}, "contextWindow": 262144, "maxTokens": 32768},
]

AGENT_MODELS = {
    "bailian/qwen3.5-plus": {},
    "bailian/qwen3-max-2026-01-23": {},
    "bailian/qwen3-coder-next": {},
    "bailian/qwen3-coder-plus": {},
    "bailian/MiniMax-M2.5": {},
    "bailian/glm-5": {},
    "bailian/glm-4.7": {},
    "bailian/kimi-k2.5": {},
}


def backup(path: Path):
    bak = path.with_name(path.name + ".bak")
    bak.write_text(path.read_text(), encoding="utf-8")


def update_claude_settings(path: Path, api_key: str):
    data = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    data["models"] = {
        "mode": "merge",
        "providers": {
            PROVIDER_NAME: {
                "baseUrl": BASE_URL,
                "apiKey": api_key,
                "api": "openai-completions",
                "models": deepcopy(MODEL_ITEMS),
            }
        },
    }
    agents = data.setdefault("agents", {})
    defaults = agents.setdefault("defaults", {})
    defaults["model"] = {"primary": "bailian/qwen3.5-plus"}
    defaults["models"] = deepcopy(AGENT_MODELS)
    backup(path)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def update_proxy_yaml(path: Path, api_key: str):
    if not path.exists():
        return False
    with path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    providers = cfg.get("openai-compatibility")
    if not isinstance(providers, list):
        providers = []

    model_entries = [{"name": item["id"], "alias": item["name"]} for item in MODEL_ITEMS]
    provider_obj = {
        "name": "bailian",
        "base-url": BASE_URL,
        "api-key-entries": [{"api-key": api_key}],
        "models": model_entries,
    }

    replaced = False
    for idx, p in enumerate(providers):
        if isinstance(p, dict) and str(p.get("name", "")).lower() == "bailian":
            providers[idx] = provider_obj
            replaced = True
            break
    if not replaced:
        providers.append(provider_obj)

    cfg["openai-compatibility"] = providers
    backup(path)
    with path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f, allow_unicode=True, sort_keys=False)
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-key", required=True)
    parser.add_argument("--claude-settings", default=str(Path.home() / ".claude" / "settings.json"))
    parser.add_argument("--quotio-config", default=str(Path.home() / "Library/Application Support/Quotio/config.yaml"))
    parser.add_argument("--codmate-config", default=str(Path.home() / "Library/Application Support/CodMate/config.yaml"))
    args = parser.parse_args()

    update_claude_settings(Path(args.claude_settings), args.api_key)
    q_ok = update_proxy_yaml(Path(args.quotio_config), args.api_key)
    c_ok = update_proxy_yaml(Path(args.codmate_config), args.api_key)

    print(f"updated_claude_settings={args.claude_settings}")
    print(f"updated_quotio_config={q_ok}")
    print(f"updated_codmate_config={c_ok}")


if __name__ == "__main__":
    main()
