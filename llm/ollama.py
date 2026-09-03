"""Ollama client initialization for app-wide use."""

from __future__ import annotations

import ollama

from config import OLLAMA_BASE_URL, OLLAMA_MODEL


_client: ollama.Client | None = None


def get_ollama_client() -> ollama.Client:
    """Return a singleton Ollama client instance."""
    global _client
    if _client is None:
        base_url = OLLAMA_BASE_URL.rstrip("/").removesuffix("/v1")
        _client = ollama.Client(host=base_url)
    return _client


def generate(prompt: str, model: str = OLLAMA_MODEL, temperature: float = 0) -> str:
    """Generate text using Ollama."""
    client = get_ollama_client()
    resp = client.chat(model=model, prompt=prompt, options={"temperature": temperature})
    return (resp.get("response") or "").strip()