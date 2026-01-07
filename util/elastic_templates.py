import os
import inspect
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

# 권장: @timestamp는 UTC로 저장 (Kibana/ES가 KST로 보기 변환 가능)
UTC = timezone.utc

def build_error_doc(
    message: str,
    *,
    # v2 템플릿 필드들
    service_name: Optional[str] = None, # 발생 위치, 시스템 혹은 모듈 단위 식별자
    service_environment: Optional[str] = None,   #신경 안써도 됨

    #파이프라인 내 위치시 명시
    pipeline_run_id: Optional[str] = None,
    pipeline_job: Optional[str] = None,
    pipeline_step: Optional[str] = None,

    # 심각성[{4:경고,3:오류,2:심각,1:치명)
    event_severity: Optional[int] = None,        # short
    event_outcome: Optional[str] = None,         # "failure" | "success" 등

    error_type: Optional[str] = None,            # ex) "ValueError"
    error_code: Optional[str] = None,            # 내부 에러 코드가 있으면
    error_message: Optional[str] = None,         # 예외 메시지(상세, 문자열로 구성)
    error_stack_trace: Optional[str] = None,     # 스택트레이스 문자열

    exception: Optional[BaseException] = None,   # exception 넣으면 type/message/trace 자동 세팅

    metrics: Optional[Dict[str, Any]] = None,    # success_count/error_count/latency_ms 등 지표
    context: Optional[Dict[str, Any]] = None,    # flattened (임의 키)
    samples: Optional[List[Dict[str, Any]]] = None,
    tags: Optional[Sequence[str]] = None,

    level: str = "ERROR",
) -> Dict[str, Any]:
    """
    error_log data stream용 표준 에러 로그 문서 생성기 (v2 템플릿 호환)
    - log.logger: 자동으로 file:function
    - message: 요약(제목)
    - error.*: 예외 상세
    - context/samples: flattened/샘플 데이터
    """

    # 호출한 쪽 위치 자동 수집
    frame = inspect.currentframe().f_back
    filename = os.path.basename(frame.f_code.co_filename) if frame else "unknown"
    funcname = frame.f_code.co_name if frame else "unknown"

    # exception이 들어오면 자동 파싱
    if exception is not None:
        if error_type is None:
            error_type = type(exception).__name__
        if error_message is None:
            error_message = str(exception)
        if error_stack_trace is None:
            error_stack_trace = "".join(
                traceback.format_exception(type(exception), exception, exception.__traceback__)
            )

    doc: Dict[str, Any] = {
        "@timestamp": datetime.now(UTC).isoformat(),
        "log": {
            "level": level,
            "logger": f"{filename}:{funcname}",
        },
        "message": message,  # 한 줄 요약
    }

    # service
    if service_name or service_environment:
        doc["service"] = {}
        if service_name:
            doc["service"]["name"] = service_name
        if service_environment:
            doc["service"]["environment"] = service_environment

    # pipeline
    if pipeline_run_id or pipeline_job or pipeline_step:
        doc["pipeline"] = {}
        if pipeline_run_id:
            doc["pipeline"]["run_id"] = pipeline_run_id
        if pipeline_job:
            doc["pipeline"]["job"] = pipeline_job
        if pipeline_step:
            doc["pipeline"]["step"] = pipeline_step

    # event
    if event_severity is not None or event_outcome is not None:
        doc["event"] = {}
        if event_severity is not None:
            doc["event"]["severity"] = int(event_severity)
        if event_outcome is not None:
            doc["event"]["outcome"] = event_outcome

    # error
    if error_type or error_code or error_message or error_stack_trace:
        doc["error"] = {}
        if error_type:
            doc["error"]["type"] = error_type
        if error_code:
            doc["error"]["code"] = error_code
        if error_message:
            doc["error"]["message"] = error_message
        if error_stack_trace:
            doc["error"]["stack_trace"] = error_stack_trace

    # metrics
    if metrics:
        doc["metrics"] = metrics

    # context (flattened)
    if context:
        doc["context"] = context

    # samples (flattened) - 최대 10개
    if samples:
        doc["samples"] = samples[:10]

    # tags (keyword[])
    if tags:
        doc["tags"] = list(tags)

    return doc


def build_info_docs(
    *,
    run_id: str,
    component: str,
    stage: str,
    status: str,               # "ok" | "warn" | "error"
    job_id: str | None = None,
    service_name: str = "backend",
    env: str = "dev",
    version: str | None = None,
    duration_ms: int | None = None,
    input_cnt: int | None = None,
    success_cnt: int | None = None,
    failed_cnt: int | None = None,
    message: str | None = None,
    error_message: str | None = None,
    retryable: bool | None = None,
):
    """
    info_logs datastream용 stage summary 문서 생성
    - None 값 필드는 아예 넣지 않음 (dynamic:false 대응)
    """
    doc: Dict[str, Any]  = {
        "@timestamp": datetime.now(timezone.utc).isoformat(),

        "trace": {
            "run_id": run_id,
        },

        "service": {
            "name": service_name,
            "env": env,
        },

        "pipeline": {
            "component": component,
            "stage": stage,
            "status": status,
        },
    }

    # optional: job_id
    if job_id:
        doc["trace"]["job_id"] = job_id

    # optional: version
    if version:
        doc["service"]["version"] = version

    # optional: message
    if message:
        doc["pipeline"]["message"] = message

    # optional: timing
    if duration_ms is not None:
        doc["timing"] = {"duration_ms": duration_ms}

    counts = {}
    if input_cnt is not None:
        counts["input"] = input_cnt
    if success_cnt is not None:
        counts["success"] = success_cnt
    if failed_cnt is not None:
        counts["failed"] = failed_cnt
    if counts:
        doc["counts"] = counts

    if status == "error":
        err = {}
        if error_message:
            err["message"] = error_message
        if retryable is not None:
            err["retryable"] = retryable
        if err:
            doc["error"] = err
    return doc

