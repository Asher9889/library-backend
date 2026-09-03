"""Face detection and recognition AI engine.

Encapsulates model loading, embedding cache management, and the
recognition pipeline so nothing else in the application needs to touch
OpenCV / PyTorch / ArcFace directly.
"""

from __future__ import annotations

import base64
import json
import threading
import time
from typing import Any, Dict, Optional

import cv2
import numpy as np
import torch
from torchvision import transforms

from database import execute_query

# ── Model loading ──────────────────────────────────────────────────────────

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(f"[FACE] Using device: {device}")

try:
    from face_detection.scrfd.detector import SCRFD
    from face_recognition.arcface.model import iresnet_inference

    face_detector = SCRFD(model_file="face_detection/scrfd/weights/scrfd_2.5g_bnkps.onnx")
    face_recognizer = iresnet_inference(
        "r100",
        path="face_recognition/arcface/weights/arcface_r100.pth",
        device=device,
    )
    face_recognizer.eval()
    print("[FACE] AI Models loaded successfully")
except Exception as e:
    print(f"[FACE ERROR] Model init failed: {e}")
    face_detector = None
    face_recognizer = None

face_preprocess = transforms.Compose([
    transforms.ToTensor(),
    transforms.Resize((112, 112), antialias=True),
    transforms.Normalize([0.5] * 3, [0.5] * 3),
])


# ── Embedding cache ────────────────────────────────────────────────────────

class FaceEmbeddingCache:
    _embeddings_matrix = None
    _mem_cds: list[str] = []
    _last_refresh: float = 0
    _lock = threading.Lock()

    @classmethod
    def refresh_embeddings(cls):
        current_time = time.time()
        if current_time - cls._last_refresh < 120:
            return

        print("[FACE] Refreshing embedding cache from DB...")
        try:
            db_data = execute_query(
                "SELECT mem_cd, embedding FROM m_memberFace WHERE is_active = 1",
                limit=False,
            )
            if not db_data:
                return

            mem_cds = []
            embeddings_list = []

            for row in db_data:
                try:
                    vector_data = json.loads(row["embedding"])
                    db_emb = np.array(vector_data["vector"], dtype=np.float32)
                    norm = np.linalg.norm(db_emb)
                    if norm > 0:
                        db_emb = db_emb / norm
                        mem_cds.append(row["mem_cd"].strip())
                        embeddings_list.append(db_emb)
                except Exception:
                    continue

            if embeddings_list:
                with cls._lock:
                    cls._embeddings_matrix = np.vstack(embeddings_list)
                    cls._mem_cds = mem_cds
                    cls._last_refresh = current_time
                print(f"[FACE] Cache refreshed: {len(mem_cds)} faces loaded into RAM")
        except Exception as e:
            print(f"[FACE] Cache refresh error: {e}")


# ── Feature extraction ─────────────────────────────────────────────────────

@torch.no_grad()
def extract_feature_fast(face_img: np.ndarray) -> Optional[np.ndarray]:
    try:
        im = cv2.cvtColor(face_img, cv2.COLOR_BGR2RGB)
        im = face_preprocess(im).unsqueeze(0).to(device)
        emb = face_recognizer(im)[0].cpu().numpy()
        norm = np.linalg.norm(emb)
        return emb / norm if norm > 0 else None
    except Exception as e:
        print(f"Feature extraction error: {e}")
        return None


# ── Recognition pipeline ───────────────────────────────────────────────────

def recognize_face_pipeline(image_base64: str) -> Dict[str, Any]:
    if not face_detector or not face_recognizer:
        return {"mem_cd": None, "error": "Models not loaded"}

    try:
        img_data = base64.b64decode(image_base64)
        nparr = np.frombuffer(img_data, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        if img is None:
            return {"mem_cd": None, "error": "Invalid image"}

        bboxes, _ = face_detector.detect(img)
        if bboxes is None or len(bboxes) == 0:
            return {"mem_cd": None, "error": "No face detected"}

        x1, y1, x2, y2, _ = bboxes[0]
        face = img[max(0, int(y1)):int(y2), max(0, int(x1)):int(x2)]
        target_emb = extract_feature_fast(face)
        if target_emb is None:
            return {"mem_cd": None, "error": "Extraction failed"}

        with FaceEmbeddingCache._lock:
            matrix = FaceEmbeddingCache._embeddings_matrix
            mem_cds = FaceEmbeddingCache._mem_cds

        if matrix is None:
            return {"mem_cd": None, "error": "Cache not loaded"}

        similarities = np.dot(matrix, target_emb)
        best_idx = int(np.argmax(similarities))
        best_score = float(similarities[best_idx])

        if best_score >= 0.35:
            return {
                "mem_cd": mem_cds[best_idx],
                "confidence": best_score,
                "error": None,
            }
        else:
            return {"mem_cd": None, "confidence": best_score, "error": "Low confidence"}

    except Exception as e:
        return {"mem_cd": None, "error": str(e)}
