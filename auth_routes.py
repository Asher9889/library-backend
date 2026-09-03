"""RFID and Face authentication endpoints.

These endpoints live under ``/auth/*`` and handle member identity
verification via physical RFID cards and camera-based face recognition.
"""

from __future__ import annotations

import base64
import json
from typing import Any, Dict, List, Optional

import cv2
import numpy as np
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from database import execute_query
from face_engine import (
    FaceEmbeddingCache,
    extract_feature_fast,
    face_detector,
    recognize_face_pipeline,
)

router = APIRouter(prefix="/auth", tags=["auth"])


# ── RFID ───────────────────────────────────────────────────────────────────


class RfidAuthRequest(BaseModel):
    rfid: str


class RfidAuthResponse(BaseModel):
    success: bool
    mem_cd: Optional[str] = None
    member_name: Optional[str] = None
    message: str


@router.post("/rfid", response_model=RfidAuthResponse)
def rfid_auth(request: RfidAuthRequest):
    rfid = (request.rfid or "").strip()
    if not rfid:
        return RfidAuthResponse(success=False, message="RFID khali hai")

    try:
        query = (
            "SELECT R.mem_cd, M.mem_firstnm, M.mem_lstnm "
            "FROM m_memberRfid R "
            "LEFT JOIN m_member M ON M.mem_cd = RTRIM(R.mem_cd) "
            f"WHERE R.Rfid = '{rfid}'"
        )
        results = execute_query(query, limit=False)
    except Exception as e:
        return RfidAuthResponse(success=False, message=f"DB Error: {e}")

    if not results:
        return RfidAuthResponse(
            success=False, message="Yeh RFID kisi member se mapped nahi hai"
        )

    mem_cd = (results[0].get("mem_cd") or "").strip()
    first_nm = (results[0].get("mem_firstnm") or "").strip()
    last_nm = (results[0].get("mem_lstnm") or "").strip()

    return RfidAuthResponse(
        success=True,
        mem_cd=mem_cd,
        member_name=f"{first_nm} {last_nm}".strip(),
        message="Login successful",
    )


# ── Face registration ──────────────────────────────────────────────────────


class FaceRegisterRequest(BaseModel):
    mem_cd: str
    images: List[str]


@router.post("/face_register")
def face_register_api(request: FaceRegisterRequest):
    if not face_detector:
        raise HTTPException(500, "Models not loaded")

    img_base64 = request.images[0]
    img_data = base64.b64decode(img_base64)
    nparr = np.frombuffer(img_data, np.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

    bboxes, _ = face_detector.detect(img)
    if bboxes is None or len(bboxes) == 0:
        raise HTTPException(400, "No face detected in the image")

    x1, y1, x2, y2, _ = bboxes[0]
    face = img[max(0, int(y1)):int(y2), max(0, int(x1)):int(x2)]
    feat = extract_feature_fast(face)

    if feat is None:
        raise HTTPException(400, "Feature extraction failed")

    vector_json = json.dumps({"vector": feat.tolist()})
    mem_cd_clean = request.mem_cd.strip()

    ok, jpg_bytes = cv2.imencode(".jpg", img)
    photo_data_uri = None
    if ok:
        photo_b64 = base64.b64encode(jpg_bytes.tobytes()).decode("ascii")
        photo_data_uri = f"data:image/jpeg;base64,{photo_b64}"

    try:
        exists = execute_query(
            f"SELECT mem_cd FROM m_memberFace WHERE mem_cd = '{mem_cd_clean}'",
            limit=False,
        )
        if exists:
            execute_query(
                "UPDATE m_memberFace SET embedding = ?, photo_base64 = ?, is_active = 1 "
                "WHERE mem_cd = ?",
                params=(vector_json, photo_data_uri, mem_cd_clean),
            )
        else:
            execute_query(
                "INSERT INTO m_memberFace (mem_cd, embedding, photo_base64, is_active) "
                "VALUES (?, ?, ?, 1)",
                params=(mem_cd_clean, vector_json, photo_data_uri),
            )

        FaceEmbeddingCache._last_refresh = 0
        FaceEmbeddingCache.refresh_embeddings()

        return {
            "status": "success",
            "mem_cd": mem_cd_clean,
            "message": "Face registered successfully!",
        }
    except Exception as e:
        raise HTTPException(500, f"DB Error: {e}")


# ── Face authentication ────────────────────────────────────────────────────


class FaceAuthRequest(BaseModel):
    image: str


class FaceAuthResponse(BaseModel):
    success: bool
    mem_cd: Optional[str] = None
    member_name: Optional[str] = None
    message: str


@router.post("/face", response_model=FaceAuthResponse)
def face_auth(request: FaceAuthRequest):
    if not face_detector:
        return FaceAuthResponse(success=False, message="Models not loaded")

    result = recognize_face_pipeline(request.image)

    if result["mem_cd"]:
        try:
            mem_data = execute_query(
                "SELECT mem_firstnm, mem_lstnm FROM m_member "
                f"WHERE mem_cd = RTRIM('{result['mem_cd']}')",
                limit=False,
            )
            name = ""
            if mem_data:
                name = (
                    f"{mem_data[0].get('mem_firstnm', '')} "
                    f"{mem_data[0].get('mem_lstnm', '')}"
                ).strip()

            return FaceAuthResponse(
                success=True,
                mem_cd=result["mem_cd"],
                member_name=name,
                message=f"Login successful! (Score: {result['confidence']:.2f})",
            )
        except Exception as e:
            return FaceAuthResponse(success=False, message=f"DB Error: {e}")
    else:
        return FaceAuthResponse(
            success=False,
            message=f"Match nahi hua: {result['error']}",
        )
