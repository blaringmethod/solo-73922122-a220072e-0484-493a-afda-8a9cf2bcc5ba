"""FastAPI application for the prediction-interval backtest workbench."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from urllib.parse import quote

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles

from . import prediction_index, storage
from .backtest import ValidationError, evaluate_backtest, parse_actuals, parse_cutoff

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MAX_UPLOAD_BYTES = int(os.environ.get("BACKTEST_MAX_UPLOAD_BYTES", 20 * 1024 * 1024))

app = FastAPI(
    title="Interval Backtest Workbench",
    version="2.0.0",
    description=(
        "预测区间回测工作台。预测 CSV 按**完整字节的 SHA-256**建立可复用的 "
        "SQLite 解析索引：相同内容重复上传直接复用索引（响应中 "
        "`prediction_index.reused=true`），仍按本次 cutoff 选取截止前最新预测并执行"
        "原有整批校验与统计。\n\n"
        "## 在 /docs 演示\n\n"
        "1. 用 `POST /api/backtests` 上传 `samples/predictions.csv` + "
        "`samples/actuals.csv`，cutoff 填 `2026-11-20T12:00:00Z`，首次返回 "
        "`reused=false`；\n"
        "2. 不改文件再点一次 **Execute**（可换 cutoff 或实际值），返回 "
        "`reused=true` 且 `content_sha256` 不变；\n"
        "3. `GET /api/prediction-index` 查看缓存索引（只有一份、status=ready）；\n"
        "4. 并发演示：`for i in $(seq 1 10); do curl ... & done` 同时首次上传同一"
        "文件，10 个请求都成功但索引只构建一次；\n"
        "5. 改动预测文件任意一个字节再上传，会得到新的 SHA-256 与新索引，绝不误命中。"
    ),
)
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")


@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    with open(os.path.join(BASE_DIR, "static", "index.html"), encoding="utf-8") as handle:
        return HTMLResponse(handle.read())


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}


async def _read_upload(upload: UploadFile, label: str) -> bytes:
    content = await upload.read(MAX_UPLOAD_BYTES + 1)
    if len(content) > MAX_UPLOAD_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"{label} file exceeds the {MAX_UPLOAD_BYTES} byte limit",
        )
    if not content.strip():
        raise HTTPException(status_code=400, detail=f"{label} file is empty")
    return content


def _validation_response(exc: ValidationError) -> JSONResponse:
    return JSONResponse(
        status_code=422,
        content={
            "detail": "Upload rejected",
            "errors": exc.errors,
            "total_errors": exc.total_errors,
        },
    )


@app.post(
    "/api/backtests",
    summary="创建回测（预测解析按 SHA-256 索引复用）",
    status_code=201,
)
async def create_backtest(
    prediction_csv: UploadFile = File(..., description="预测 CSV，完整字节参与 SHA-256"),
    actual_csv: UploadFile = File(..., description="实际值 CSV，每次重新解析、不入索引"),
    cutoff: str = Form("", description="UTC 评估截止时间，如 2026-11-20T12:00:00Z"),
) -> JSONResponse:
    """首次上传会构建预测解析索引（`prediction_index.reused=false`）；之后上传
    字节完全相同的预测文件时直接复用索引（`reused=true`），仍按本次 cutoff
    选取截止前最新预测并执行原有的整批校验与统计。索引未完整建立、进程重启前
    的残留构建标记或构建失败都不会参与回测。"""
    try:
        prediction_bytes = await _read_upload(prediction_csv, "prediction")
        actual_bytes = await _read_upload(actual_csv, "actual")
    except HTTPException as exc:
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": exc.detail, "errors": [exc.detail]},
        )

    # Cutoff is validated first so a missing/malformed cutoff never triggers an
    # index build -- identical error ordering to the original one-shot path.
    try:
        cutoff_dt = parse_cutoff(cutoff)
    except ValidationError as exc:
        return _validation_response(exc)

    # The prediction parse is deterministic for the file's bytes and
    # independent of the cutoff, so it is served from (or published into) the
    # SHA-256 keyed SQLite index. A failed/incomplete build raises here and no
    # partial index is ever used.
    try:
        index_outcome = prediction_index.get_or_build(prediction_bytes)
    except ValidationError as exc:
        return _validation_response(exc)

    # Actuals vary per run and are never indexed.
    try:
        parsed_actuals = parse_actuals(actual_bytes)
    except ValidationError as exc:
        return _validation_response(exc)

    try:
        result = evaluate_backtest(
            cutoff=cutoff_dt,
            predictions=index_outcome.predictions,
            actuals=parsed_actuals,
        )
    except ValidationError as exc:
        return _validation_response(exc)

    summary = {
        "cutoff": result.cutoff.isoformat().replace("+00:00", "Z"),
        "warnings": result.warnings,
        "submitted_rows": result.submitted_rows,
        "horizons": result.horizons,
        "misses": result.misses,
        "samples": result.intervals,
    }
    run_id = storage.save_backtest(
        cutoff=summary["cutoff"],
        prediction_filename=prediction_csv.filename or "predictions.csv",
        actual_filename=actual_csv.filename or "actuals.csv",
        prediction_csv=prediction_bytes,
        actual_csv=actual_bytes,
        summary=summary,
        submitted_rows=summary["submitted_rows"],
    )
    run = storage.get_backtest(run_id)
    assert run is not None
    return JSONResponse(
        status_code=201,
        content={
            "id": run_id,
            "download_url": f"/api/backtests/{run_id}/download/samples.csv",
            "summary_url": f"/api/backtests/{run_id}/download/summary.csv",
            "run": run,
            "prediction_index": {
                "content_sha256": index_outcome.content_sha256,
                "byte_size": index_outcome.byte_size,
                "reused": index_outcome.reused,
                "index_status_url": (
                    "/api/prediction-index/" + index_outcome.content_sha256
                ),
            },
        },
    )


@app.get(
    "/api/prediction-index",
    summary="列出已建立的预测解析索引",
)
def list_prediction_index(limit: int = 50) -> dict:
    """诊断接口：列出当前可复用的预测解析索引。每条记录按预测文件完整字节的
    SHA-256 标识，`status` 只会是 `ready`（构建中的标记对任何回测都不可见，
    失败会整体回滚而不留记录）。可用于核对重复上传是否只产生了一份索引。"""
    statuses = prediction_index.list_status(limit)
    return {
        "indices": [
            {
                "content_sha256": item.content_sha256,
                "byte_size": item.byte_size,
                "status": item.status,
                "row_count": item.row_count,
                "parsed_rows": item.parsed_rows,
                "error_count": item.error_count,
                "created_at": item.created_at,
                "updated_at": item.updated_at,
            }
            for item in statuses
        ]
    }


@app.get(
    "/api/prediction-index/{content_sha256}",
    summary="查询单个预测解析索引状态",
)
def get_prediction_index(content_sha256: str) -> dict:
    """按 SHA-256 查询一份索引的状态与行数统计；不存在返回 404。"""
    item = prediction_index.get_status(content_sha256)
    if item is None:
        raise HTTPException(status_code=404, detail="prediction index not found")
    return {
        "content_sha256": item.content_sha256,
        "byte_size": item.byte_size,
        "status": item.status,
        "row_count": item.row_count,
        "parsed_rows": item.parsed_rows,
        "error_count": item.error_count,
        "created_at": item.created_at,
        "updated_at": item.updated_at,
    }


@app.get("/api/backtests")
def list_backtests(limit: int = 20) -> dict:
    limit = max(1, min(limit, 100))
    runs = storage.list_backtests(limit)
    for run in runs:
        # Keep the list payload small; full rows are fetched on demand.
        run.pop("samples", None)
        run.pop("misses", None)
    return {"runs": runs}


@app.get("/api/backtests/{run_id}")
def get_backtest(run_id: str) -> dict:
    run = storage.get_backtest(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="backtest not found")
    return run


def _download(content: bytes, filename: str, media_type: str) -> Response:
    ascii_name = filename.encode("ascii", "replace").decode("ascii").replace('"', "'")
    return Response(
        content=content,
        media_type=media_type,
        headers={
            "Content-Disposition": (
                f'attachment; filename="{ascii_name}"; '
                f"filename*=UTF-8''{quote(filename)}"
            )
        },
    )


@app.get("/api/backtests/{run_id}/download/samples.csv")
def download_samples(run_id: str) -> Response:
    run = storage.get_backtest(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="backtest not found")
    return _download(
        storage.render_samples_csv(run), f"backtest-{run_id}-samples.csv", "text/csv"
    )


@app.get("/api/backtests/{run_id}/download/summary.csv")
def download_summary(run_id: str) -> Response:
    run = storage.get_backtest(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="backtest not found")
    return _download(
        storage.render_summary_csv(run), f"backtest-{run_id}-summary.csv", "text/csv"
    )


@app.get("/api/backtests/{run_id}/download/input/{kind}.csv")
def download_input(run_id: str, kind: str) -> Response:
    if kind not in ("prediction", "actual"):
        raise HTTPException(status_code=404, detail="unknown input kind")
    found = storage.render_input_csv(run_id, kind)
    if found is None:
        raise HTTPException(status_code=404, detail="backtest not found")
    filename, content = found
    return _download(content, filename, "text/csv")
