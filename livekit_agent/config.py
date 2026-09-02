"""Configuration for the LiveKit voice agent worker."""
import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


def _get(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _get_float(name: str, default: float) -> float:
    try:
        return float(_get(name) or default)
    except ValueError:
        return default


def _get_int(name: str, default: int) -> int:
    try:
        return int(_get(name) or default)
    except ValueError:
        return default


def _get_bool(name: str, default: bool = True) -> bool:
    val = _get(name, "1" if default else "0").lower()
    return val in ("1", "true", "yes", "on")


@dataclass
class Settings:
    livekit_url: str
    livekit_api_key: str
    livekit_api_secret: str
    livekit_api_url: str
    agent_name: str
    agent_deployment: str
    agent_display_name: str
    agent_logs: bool
    voice_llm_base_url: str
    voice_llm_api_key: str
    voice_llm_model: str
    stt_url: str
    tts_url: str
    tts_language: str
    tts_voice: str
    tts_speed: float
    tts_sample_rate: int
    debug_dump_audio: bool
    library_agent_url: str
    library_agent_timeout: float
    token_ttl_seconds: int


settings = Settings(
    livekit_url=_get("LIVEKIT_URL", "wss://localhost:7880"),
    livekit_api_key=_get("LIVEKIT_API_KEY"),
    livekit_api_secret=_get("LIVEKIT_API_SECRET"),
    livekit_api_url=_get("LIVEKIT_API_URL", ""),
    agent_name=_get("LIVEKIT_AGENT_NAME", "soul-library-voice-assistant"),
    agent_deployment=_get("LIVEKIT_AGENT_DEPLOYMENT", ""),
    agent_display_name=_get("LIVEKIT_AGENT_DISPLAY_NAME", "SOUL Library Assistant"),
    agent_logs=_get_bool("LIVEKIT_AGENT_LOGS", True),
    voice_llm_base_url=_get("OLLAMA_BASE_URL", "http://127.0.0.1:11434/v1"),
    voice_llm_api_key=_get("OLLAMA_API_KEY", "ollama"),
    voice_llm_model=_get("OLLAMA_MODEL", "qwen2.5:7b"),
    stt_url=_get("STT_SERVER_URL", "http://localhost:8000").rstrip("/"),
    tts_url=_get("TTS_SERVER_URL", "http://localhost:8001").rstrip("/"),
    tts_language=_get("TTS_LANGUAGE", "hi"),
    tts_voice=_get("TTS_VOICE", "hm_psi"),
    tts_speed=_get_float("TTS_SPEED", 1.0),
    tts_sample_rate=_get_int("TTS_SAMPLE_RATE", 44100),
    debug_dump_audio=_get_bool("DEBUG_DUMP_AUDIO", False),
    library_agent_url=_get("LIBRARY_AGENT_URL", "http://localhost:7698").rstrip("/"),
    library_agent_timeout=_get_float("LIBRARY_AGENT_TIMEOUT", 60.0),
    token_ttl_seconds=_get_int("LIVEKIT_TOKEN_TTL", 3600),
)
