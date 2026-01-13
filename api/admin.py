# api/admin.py
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from util.scheduler_runtime import scheduler
from util.elastic import es
from util.elastic_templates import build_info_docs

router = APIRouter(prefix="/admin", tags=["admin"])

def _now_iso() -> str:
    # info_logs에 찍을 때 보기 편하게 ISO로
    return datetime.now().isoformat(timespec="seconds")

def _log_scheduler_action(
    env: str,
    action: str,
    target: str,
    result: str,
    error_message: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    """
     info_logs에 scheduler 제어 이력 남김 (기존 build_info_docs 사용)
    - run_id는 "YYYYMMDD_HH" 규칙을 main과 동일하게 맞추려면 main의 _run_id_kst를 import해서 써도 됨.
      여기선 admin 단독으로도 동작하도록 hour 기반 run_id를 동일 포맷으로 생성.
    """
    run_id = datetime.now().strftime("%Y%m%d_%H")

    doc = build_info_docs(
        run_id=run_id,
        job_id="admin_scheduler_control",
        component="admin",
        stage=f"{action}_{target}",
        status="ok" if result == "ok" else "error",
        message=f"scheduler {action} {target}",
        error_message=error_message,
        service_name="donga-api",
        env=env,
    )
    if extra:
        # build_info_docs가 dict라 가정
        doc.update({"context": extra, "ts": _now_iso()})

    es.index(index="info_logs", document=doc)


class ToggleJobBody(BaseModel):
    paused: bool

@router.get("/scheduler/jobs")
def list_jobs(env: str = "dev"):
    """
    현재 scheduler에 등록된 job 목록
    - next_run_time / trigger / name / id 반환
    """
    try:
        jobs = []
        for j in scheduler.get_jobs():
            jobs.append(
                {
                    "id": j.id,
                    "name": j.name,
                    "next_run_time": j.next_run_time.isoformat() if j.next_run_time else None,
                    "trigger": str(j.trigger),
                }
            )
        return {"ok": True, "jobs": jobs}
    except Exception as e:
        _log_scheduler_action(env, "list", "jobs", "error", error_message=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/scheduler/jobs/{job_id}")
def patch_job(job_id: str, body: ToggleJobBody, env: str = "dev"):
    """
    PATCH /admin/scheduler/jobs/{job_id}
    body: {"paused": true|false}

    - paused=true  -> pause_job
    - paused=false -> resume_job
    """
    # job 존재 확인 (없으면 404)
    job = scheduler.get_job(job_id)
    if not job:
        _log_scheduler_action(env, "patch", job_id, "error", error_message="job not found")
        raise HTTPException(status_code=404, detail=f"job not found: {job_id}")

    try:
        if body.paused:
            scheduler.pause_job(job_id)
            _log_scheduler_action(
                env,
                action="pause",
                target=job_id,
                result="ok",
                extra={"job_id": job_id, "paused": True},
            )
            return {"ok": True, "job_id": job_id, "paused": True}
        else:
            scheduler.resume_job(job_id)
            _log_scheduler_action(
                env,
                action="resume",
                target=job_id,
                result="ok",
                extra={"job_id": job_id, "paused": False},
            )
            return {"ok": True, "job_id": job_id, "paused": False}

    except Exception as e:
        _log_scheduler_action(env, "patch", job_id, "error", error_message=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/scheduler/pause-all")
def pause_all(env: str = "dev"):
    """
    전체 job pause (필요하면 사용)
    """
    try:
        scheduler.pause()
        _log_scheduler_action(env, "pause", "all", "ok", extra={"paused": True})
        return {"ok": True, "paused_all": True}
    except Exception as e:
        _log_scheduler_action(env, "pause", "all", "error", error_message=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/scheduler/resume-all")
def resume_all(env: str = "dev"):
    try:
        scheduler.resume()
        _log_scheduler_action(env, "resume", "all", "ok", extra={"paused": False})
        return {"ok": True, "paused_all": False}
    except Exception as e:
        _log_scheduler_action(env, "resume", "all", "error", error_message=str(e))
        raise HTTPException(status_code=500, detail=str(e))
