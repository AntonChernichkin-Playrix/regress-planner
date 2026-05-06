import asyncio
import json
import re
import time
import urllib.request
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import parse_qs
from uuid import UUID

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response, StreamingResponse
from fastapi.templating import Jinja2Templates
from sqlmodel import Session, col, select

import base64
import secrets

from app.config import QASE_TOKEN, QASE_API_BASE, BASIC_AUTH_USER, BASIC_AUTH_PASS
from app.db import engine, get_all_teams, init_db
from app.models import (
    Device, Feature, Member, HistorySnapshot,
    REGRESSION_SCOPE_CHOICES, REGRESSION_SCOPE_LABELS,
    DEVICE_CHOICES, DEVICE_LABELS,
    utcnow,
)
import json as _json

# необходимо для регистрации таблиц в SQLModel.metadata до create_all
_ = (Feature, Member, HistorySnapshot)

BASE = Path(__file__).resolve().parent.parent
templates = Jinja2Templates(directory=str(BASE / "templates"))


def _eff_hours(f) -> float:
    """Приоритет: override_hours > short_hours > check_hours."""
    if f.override_hours is not None:
        return f.override_hours
    if f.short_hours is not None:
        return f.short_hours
    return f.check_hours


def _fmt_num(v: float) -> str:
    """Форматирует число: целое без .0, дробное с одним знаком."""
    try:
        f = float(v)
        return str(int(f)) if f == int(f) else f"{f:.1f}"
    except (TypeError, ValueError):
        return str(v)


templates.env.filters["fmtnum"]   = _fmt_num
templates.env.filters["fromjson"] = _json.loads

_MAX_CHECK_HOURS = 9_999


def _no_store(response: Response) -> None:
    response.headers["Cache-Control"] = "no-store, max-age=0"


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="Regress planner", lifespan=lifespan)


# ---------------------------------------------------------------------------
# SSE-бродкаст: уведомление всех клиентов об изменениях
# ---------------------------------------------------------------------------

_last_update: float    = time.time()
_last_update_by: str   = ""


def _mark_updated(tab_id: str = "") -> None:
    """Вызывается после каждого write-запроса. Будит всех SSE-подписчиков."""
    global _last_update, _last_update_by
    _last_update    = time.time()
    _last_update_by = tab_id


_AUTH_ENABLED = bool(BASIC_AUTH_USER)
_AUTH_REALM   = "Regress Planner"

@app.middleware("http")
async def _basic_auth(request: Request, call_next):
    """HTTP Basic Auth — проверяет логин/пароль на каждый запрос."""
    if not _AUTH_ENABLED:
        return await call_next(request)

    auth_header = request.headers.get("Authorization", "")
    authenticated = False

    if auth_header.startswith("Basic "):
        try:
            decoded   = base64.b64decode(auth_header[6:]).decode("utf-8", errors="replace")
            user, _, pwd = decoded.partition(":")
            user_ok = secrets.compare_digest(user.encode(), BASIC_AUTH_USER.encode())
            pass_ok = secrets.compare_digest(pwd.encode(),  BASIC_AUTH_PASS.encode())
            authenticated = user_ok and pass_ok
        except Exception:
            pass

    if not authenticated:
        return Response(
            content="Требуется авторизация",
            status_code=401,
            headers={
                "WWW-Authenticate": f'Basic realm="{_AUTH_REALM}"',
                "Content-Type": "text/plain; charset=utf-8",
            },
        )

    return await call_next(request)


@app.middleware("http")
async def _update_tracker(request: Request, call_next):
    """После любого успешного write-запроса уведомляем SSE-клиентов."""
    # Запоминаем browser_tab_id ДО вызова (тело можно прочитать только раз)
    browser_tab_id = request.headers.get("X-Tab-Id", "")
    response = await call_next(request)
    if request.method in ("POST", "PUT", "DELETE", "PATCH"):
        if not request.url.path.startswith("/api/qase/"):
            if response.status_code < 400:
                _mark_updated(browser_tab_id)
    return response


@app.get("/api/events")
async def sse_events(request: Request):
    """
    Server-Sent Events. Клиент передаёт свой уникальный tab_id в ?tab=
    чтобы не получать уведомление о своих же изменениях.
    Проверяет обновления каждые 2 сек — достаточно для live-эффекта
    и не перегружает единственный воркер uvicorn.
    """
    my_tab_id = request.query_params.get("tab", "")

    async def generator():
        last_seen = _last_update
        yield "data: connected\n\n"
        ticks = 0
        while True:
            await asyncio.sleep(2)
            ticks += 1

            if await request.is_disconnected():
                break

            if _last_update > last_seen:
                last_seen = _last_update
                ticks = 0
                if not (my_tab_id and my_tab_id == _last_update_by):
                    yield "data: update\n\n"
            elif ticks >= 15:          # keepalive раз в ~30 сек
                yield ": keepalive\n\n"
                ticks = 0

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_hours(raw: str) -> float:
    s = (raw or "").strip().replace(",", ".")
    if not s:
        return 0.0
    return float(s)


def _form_field(flat: dict[str, str], *keys: str) -> str:
    for k in keys:
        if k in flat and flat[k] is not None:
            return str(flat[k])
    return ""


async def _read_form(request: Request) -> dict[str, str]:
    """
    Стабильный разбор тела POST.
    urlencoded — сырые байты + urllib; multipart — через request.form().
    """
    content_type = (request.headers.get("content-type") or "").lower()
    if "multipart/form-data" in content_type:
        form = await request.form()
        return {
            str(k): (str(v) if v is not None else "")
            for k, v in form.multi_items()
            if not (hasattr(v, "read") and not isinstance(v, (str, bytes)))
        }
    body = await request.body()
    if not body:
        return {}
    text = body.decode("utf-8", errors="replace")
    parsed = parse_qs(text, keep_blank_values=True, strict_parsing=False)
    return {k: (v[0] if v else "") for k, v in parsed.items()}


def _validate_feature(name_raw: str, team: str, check_hours_raw: str):
    """
    Возвращает (name, team, hours_float, error_or_None).
    Первая найденная ошибка — финальная, последующие не перекрывают.
    """
    n = (name_raw or "").strip()
    t = (team or "").strip()
    error = None

    try:
        h = _parse_hours(check_hours_raw)
    except ValueError:
        h = 0.0
        error = "Некорректное число часов"

    if error is None and h < 0:
        error = "Часы не могут быть отрицательными"
    if error is None and h > _MAX_CHECK_HOURS:
        error = f"Слишком большое значение часов (максимум {_MAX_CHECK_HOURS})"
    if error is None and not n:
        error = "Укажите название фичи"
    if error is None and len(t) > 120:
        error = "Название команды слишком длинное (максимум 120 символов)"

    return n, t, h, error


def _form_tmpl(request, feature, name_raw, team, check_hours_raw, external_ref,
               regression_scope, error, status=422):
    r = templates.TemplateResponse(
        "feature_form.html",
        {
            "request": request,
            "feature": feature,
            "form_name": name_raw,
            "form_team": team,
            "form_check_hours": check_hours_raw,
            "form_external_ref": external_ref,
            "form_regression_scope": regression_scope,
            "scope_choices": REGRESSION_SCOPE_CHOICES,
            "scope_labels": REGRESSION_SCOPE_LABELS,
            "error": error,
        },
        status_code=status,
    )
    _no_store(r)
    return r


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
def index():
    return RedirectResponse("/features", status_code=303)


@app.get("/features", response_class=HTMLResponse)
def list_features(request: Request):
    with Session(engine) as session:
        rows = list(session.exec(
            select(Feature).order_by(col(Feature.team), col(Feature.name))
        ).all())
    teams = get_all_teams()
    r = templates.TemplateResponse(
        "feature_list.html",
        {"request": request, "features": rows, "scope_labels": REGRESSION_SCOPE_LABELS, "all_teams": teams},
    )
    _no_store(r)
    return r


@app.get("/features/new", response_class=HTMLResponse)
def new_feature_form(request: Request):
    r = templates.TemplateResponse(
        "feature_form.html",
        {
            "request": request,
            "feature": None,
            "error": None,
            "scope_choices": REGRESSION_SCOPE_CHOICES,
            "scope_labels": REGRESSION_SCOPE_LABELS,
        },
    )
    _no_store(r)
    return r


@app.post("/features", response_class=HTMLResponse, name="create_feature")
async def create_feature(request: Request):
    flat = await _read_form(request)
    name_raw = _form_field(flat, "feature_title", "name")
    team = _form_field(flat, "team")
    check_hours_raw = _form_field(flat, "check_hours") or "0"
    external_ref = _form_field(flat, "external_ref")
    regression_scope = _form_field(flat, "regression_scope")
    if regression_scope not in REGRESSION_SCOPE_CHOICES:
        regression_scope = ""

    n, t, h, error = _validate_feature(name_raw, team, check_hours_raw)
    if error:
        return _form_tmpl(request, None, name_raw, team, check_hours_raw,
                          external_ref, regression_scope, error)

    with Session(engine) as session:
        f = Feature(
            team=t, name=n, check_hours=h,
            regression_scope=regression_scope,
            external_ref=(external_ref.strip() or None),
        )
        session.add(f)
        session.commit()
    return RedirectResponse("/features", status_code=303)


@app.get("/features/{feature_id}/edit", response_class=HTMLResponse)
def edit_feature_form(request: Request, feature_id: UUID):
    with Session(engine) as session:
        f = session.get(Feature, feature_id)
    if not f:
        return HTMLResponse("Не найдено", status_code=404)
    r = templates.TemplateResponse(
        "feature_form.html",
        {
            "request": request,
            "feature": f,
            "error": None,
            "scope_choices": REGRESSION_SCOPE_CHOICES,
            "scope_labels": REGRESSION_SCOPE_LABELS,
        },
    )
    _no_store(r)
    return r


@app.post("/features/{feature_id}", response_class=HTMLResponse, name="update_feature")
async def update_feature(request: Request, feature_id: UUID):
    flat = await _read_form(request)
    name_raw = _form_field(flat, "feature_title", "name")
    team = _form_field(flat, "team")
    check_hours_raw = _form_field(flat, "check_hours") or "0"
    external_ref = _form_field(flat, "external_ref")
    regression_scope = _form_field(flat, "regression_scope")
    if regression_scope not in REGRESSION_SCOPE_CHOICES:
        regression_scope = ""

    with Session(engine) as session:
        f = session.get(Feature, feature_id)
        if not f:
            return HTMLResponse("Не найдено", status_code=404)

        n, t, h, error = _validate_feature(name_raw, team, check_hours_raw)
        if error:
            return _form_tmpl(request, f, name_raw, team, check_hours_raw,
                              external_ref, regression_scope, error)

        f.team = t
        f.name = n
        f.check_hours = h
        f.regression_scope = regression_scope
        f.external_ref = external_ref.strip() or None
        f.updated_at = utcnow()
        session.add(f)
        session.commit()

    return RedirectResponse("/features", status_code=303)


@app.post("/features/{feature_id}/scope")
async def update_scope(request: Request, feature_id: UUID):
    flat = await _read_form(request)
    scope = _form_field(flat, "regression_scope")
    if scope not in REGRESSION_SCOPE_CHOICES:
        scope = ""
    with Session(engine) as session:
        f = session.get(Feature, feature_id)
        if not f:
            return HTMLResponse("Не найдено", status_code=404)
        f.regression_scope = scope
        f.updated_at = utcnow()
        session.add(f)
        session.commit()
    return JSONResponse({"ok": True, "scope": scope})


@app.post("/features/{feature_id}/comment")
async def update_feature_comment(request: Request, feature_id: UUID):
    flat = await _read_form(request)
    comment = (_form_field(flat, "comment") or "").strip()[:500] or None
    with Session(engine) as session:
        f = session.get(Feature, feature_id)
        if not f:
            return JSONResponse({"ok": False, "error": "Не найдено"}, status_code=404)
        f.comment = comment
        f.updated_at = utcnow()
        session.add(f)
        session.commit()
    return JSONResponse({"ok": True, "comment": comment or ""})


@app.post("/features/{feature_id}/qase-url")
async def update_feature_qase_url(request: Request, feature_id: UUID):
    flat = await _read_form(request)
    url = (_form_field(flat, "qase_url") or "").strip()[:500]
    with Session(engine) as session:
        f = session.get(Feature, feature_id)
        if not f:
            return JSONResponse({"ok": False, "error": "Не найдено"}, status_code=404)
        f.qase_url = url or None
        f.updated_at = utcnow()
        session.add(f)
        session.commit()
    return JSONResponse({"ok": True, "qase_url": url})


@app.post("/features/{feature_id}/team")
async def update_feature_team(request: Request, feature_id: UUID):
    flat = await _read_form(request)
    team = (_form_field(flat, "team") or "").strip()[:120]
    with Session(engine) as session:
        f = session.get(Feature, feature_id)
        if not f:
            return JSONResponse({"ok": False, "error": "Не найдено"}, status_code=404)
        f.team = team
        f.updated_at = utcnow()
        session.add(f)
        session.commit()
    return JSONResponse({"ok": True, "team": team})


@app.post("/features/{feature_id}/hours")
async def update_feature_hours(request: Request, feature_id: UUID):
    flat = await _read_form(request)
    raw = (_form_field(flat, "check_hours") or "").strip().replace(",", ".")
    if not raw:
        return JSONResponse({"ok": False, "error": "Укажите значение"}, status_code=422)
    try:
        v = float(raw)
        if v < 0 or v > _MAX_CHECK_HOURS:
            return JSONResponse({"ok": False, "error": "Недопустимое значение"}, status_code=422)
    except ValueError:
        return JSONResponse({"ok": False, "error": "Некорректное число"}, status_code=422)
    with Session(engine) as session:
        f = session.get(Feature, feature_id)
        if not f:
            return JSONResponse({"ok": False, "error": "Не найдено"}, status_code=404)
        f.check_hours = v
        f.updated_at = utcnow()
        session.add(f)
        session.commit()
        eff = _eff_hours(f)
        base = f.check_hours
    return JSONResponse({"ok": True, "check_hours": base, "effective_hours": eff})


@app.post("/features/{feature_id}/short-hours")
async def update_feature_short_hours(request: Request, feature_id: UUID):
    flat = await _read_form(request)
    raw = (_form_field(flat, "short_hours") or "").strip().replace(",", ".")
    with Session(engine) as session:
        f = session.get(Feature, feature_id)
        if not f:
            return JSONResponse({"ok": False, "error": "Не найдено"}, status_code=404)
        if raw == "" or raw == "-":
            f.short_hours = None
        else:
            try:
                v = float(raw)
                if v < 0 or v > _MAX_CHECK_HOURS:
                    return JSONResponse({"ok": False, "error": "Недопустимое значение"}, status_code=422)
                f.short_hours = v
            except ValueError:
                return JSONResponse({"ok": False, "error": "Некорректное число"}, status_code=422)
        f.updated_at = utcnow()
        session.add(f)
        session.commit()
        eff = _eff_hours(f)
        sh = f.short_hours
    return JSONResponse({"ok": True, "short_hours": sh, "effective_hours": eff})


@app.post("/features/{feature_id}/override-hours")
async def update_feature_override_hours(request: Request, feature_id: UUID):
    flat = await _read_form(request)
    raw = (_form_field(flat, "override_hours") or "").strip().replace(",", ".")
    with Session(engine) as session:
        f = session.get(Feature, feature_id)
        if not f:
            return JSONResponse({"ok": False, "error": "Не найдено"}, status_code=404)
        if raw == "" or raw == "-":
            f.override_hours = None
        else:
            try:
                v = float(raw)
                if v < 0 or v > _MAX_CHECK_HOURS:
                    return JSONResponse({"ok": False, "error": "Недопустимое значение"}, status_code=422)
                f.override_hours = v
            except ValueError:
                return JSONResponse({"ok": False, "error": "Некорректное число"}, status_code=422)
        f.updated_at = utcnow()
        session.add(f)
        session.commit()
        eff = _eff_hours(f)
        ovr = f.override_hours
    return JSONResponse({"ok": True, "override_hours": ovr, "effective_hours": eff})


@app.post("/features/{feature_id}/delete", response_class=HTMLResponse)
def delete_feature(feature_id: UUID):
    with Session(engine) as session:
        f = session.get(Feature, feature_id)
        if not f:
            return HTMLResponse("Не найдено", status_code=404)
        session.delete(f)
        session.commit()
    return RedirectResponse("/features", status_code=303)


# ===========================================================================
# Members
# ===========================================================================

_MAX_DAYS = 999


def _validate_member(name_raw: str, team: str, days_raw: str, device: str):
    """Возвращает (name, team, days_float, device, error_or_None)."""
    n = (name_raw or "").strip()
    t = (team or "").strip()
    # Множественный выбор: comma-separated, каждое значение из DEVICE_CHOICES (без "")
    raw_parts = [p.strip() for p in (device or "").split(",")]
    dv = ",".join(p for p in raw_parts if p in DEVICE_CHOICES and p != "")
    error = None
    try:
        d = _parse_hours(days_raw)
    except ValueError:
        d = 0.0
        error = "Некорректное количество дней"
    if error is None and d < 0:
        error = "Количество дней не может быть отрицательным"
    if error is None and d > _MAX_DAYS:
        error = f"Слишком большое значение (максимум {_MAX_DAYS})"
    if error is None and not n:
        error = "Укажите имя участника"
    if error is None and len(t) > 120:
        error = "Название команды слишком длинное"
    return n, t, d, dv, error


def _member_tmpl(request, member, name_raw, team, days_raw, error, device_raw="", status=422):
    r = templates.TemplateResponse(
        "member_form.html",
        {
            "request": request,
            "member": member,
            "form_name": name_raw,
            "form_team": team,
            "form_days": days_raw,
            "form_device": device_raw,
            "device_choices": DEVICE_CHOICES,
            "device_labels": DEVICE_LABELS,
            "error": error,
        },
        status_code=status,
    )
    _no_store(r)
    return r


@app.get("/members", response_class=HTMLResponse)
def list_members(request: Request):
    with Session(engine) as session:
        rows = list(session.exec(
            select(Member).order_by(col(Member.team), col(Member.full_name))
        ).all())
    total_days = sum(m.available_days for m in rows)
    all_teams = sorted({m.team for m in rows if m.team})
    r = templates.TemplateResponse(
        "member_list.html",
        {"request": request, "members": rows, "total_days": total_days, "all_teams": all_teams},
    )
    _no_store(r)
    return r


@app.get("/members/new", response_class=HTMLResponse)
def new_member_form(request: Request):
    r = templates.TemplateResponse(
        "member_form.html",
        {
            "request": request, "member": None, "error": None,
            "device_choices": DEVICE_CHOICES, "device_labels": DEVICE_LABELS,
        },
    )
    _no_store(r)
    return r


@app.post("/members", response_class=HTMLResponse, name="create_member")
async def create_member(request: Request):
    flat = await _read_form(request)
    name_raw = _form_field(flat, "full_name")
    team = _form_field(flat, "team")
    days_raw = _form_field(flat, "available_days") or "0"
    device_raw = _form_field(flat, "device")

    n, t, d, dv, error = _validate_member(name_raw, team, days_raw, device_raw)
    if error:
        return _member_tmpl(request, None, name_raw, team, days_raw, error, device_raw)

    with Session(engine) as session:
        session.add(Member(full_name=n, team=t, available_days=d, device=dv))
        session.commit()
    return RedirectResponse("/members", status_code=303)


@app.get("/members/{member_id}/edit", response_class=HTMLResponse)
def edit_member_form(request: Request, member_id: UUID):
    with Session(engine) as session:
        m = session.get(Member, member_id)
    if not m:
        return HTMLResponse("Не найдено", status_code=404)
    r = templates.TemplateResponse(
        "member_form.html",
        {
            "request": request, "member": m, "error": None,
            "device_choices": DEVICE_CHOICES, "device_labels": DEVICE_LABELS,
        },
    )
    _no_store(r)
    return r


@app.post("/members/{member_id}", response_class=HTMLResponse, name="update_member")
async def update_member(request: Request, member_id: UUID):
    flat = await _read_form(request)
    name_raw = _form_field(flat, "full_name")
    team = _form_field(flat, "team")
    days_raw = _form_field(flat, "available_days") or "0"
    device_raw = _form_field(flat, "device")

    with Session(engine) as session:
        m = session.get(Member, member_id)
        if not m:
            return HTMLResponse("Не найдено", status_code=404)

        n, t, d, dv, error = _validate_member(name_raw, team, days_raw, device_raw)
        if error:
            return _member_tmpl(request, m, name_raw, team, days_raw, error, device_raw)

        m.full_name = n
        m.team = t
        m.available_days = d
        m.device = dv
        m.updated_at = utcnow()
        session.add(m)
        session.commit()

    return RedirectResponse("/members", status_code=303)


@app.post("/members/{member_id}/comment")
async def update_member_comment(request: Request, member_id: UUID):
    flat = await _read_form(request)
    comment = (_form_field(flat, "comment") or "").strip()[:500] or None
    with Session(engine) as session:
        m = session.get(Member, member_id)
        if not m:
            return JSONResponse({"ok": False, "error": "Не найдено"}, status_code=404)
        m.comment = comment
        m.updated_at = utcnow()
        session.add(m)
        session.commit()
    return JSONResponse({"ok": True, "comment": comment or ""})


@app.post("/members/{member_id}/device")
async def update_member_device(request: Request, member_id: UUID):
    flat = await _read_form(request)
    raw_parts = [p.strip() for p in (flat.get("device") or "").split(",")]
    dv = ",".join(p for p in raw_parts if p in DEVICE_CHOICES and p != "")
    with Session(engine) as session:
        m = session.get(Member, member_id)
        if not m:
            return JSONResponse({"ok": False, "error": "Не найдено"}, status_code=404)
        m.device = dv
        m.updated_at = utcnow()
        session.add(m)
        session.commit()
    return JSONResponse({"ok": True, "device": dv})


@app.post("/members/{member_id}/days")
async def update_member_days(request: Request, member_id: UUID):
    flat = await _read_form(request)
    days_raw = _form_field(flat, "available_days") or "0"
    try:
        d = _parse_hours(days_raw)
        if d < 0 or d > _MAX_DAYS:
            return JSONResponse({"ok": False, "error": "Недопустимое значение"}, status_code=422)
    except ValueError:
        return JSONResponse({"ok": False, "error": "Некорректное число"}, status_code=422)
    with Session(engine) as session:
        m = session.get(Member, member_id)
        if not m:
            return JSONResponse({"ok": False, "error": "Не найдено"}, status_code=404)
        m.available_days = d
        m.updated_at = utcnow()
        session.add(m)
        session.commit()
    return JSONResponse({"ok": True, "days": d})


@app.post("/members/{member_id}/delete", response_class=HTMLResponse)
def delete_member(member_id: UUID):
    with Session(engine) as session:
        m = session.get(Member, member_id)
        if not m:
            return HTMLResponse("Не найдено", status_code=404)
        session.delete(m)
        session.commit()
    return RedirectResponse("/members", status_code=303)


@app.post("/members-sync-devices")
def sync_member_devices():
    """Проставляет профильные девайсы участникам на основе таблицы устройств."""
    updated = 0
    with Session(engine) as session:
        members = list(session.exec(select(Member)).all())
        for m in members:
            devs = list(session.exec(
                select(Device).where(col(Device.employee) == m.full_name)
            ).all())
            has_global  = any(d.profile_global  for d in devs)
            has_china   = any(d.profile_china   for d in devs)
            has_vietnam = any(d.profile_vietnam for d in devs)
            parts = []
            if has_global:  parts.append("global")
            if has_china:   parts.append("china")
            if has_vietnam: parts.append("vietnam")
            new_device = ",".join(parts)
            if m.device != new_device:
                m.device = new_device
                session.add(m)
                updated += 1
        session.commit()
    return JSONResponse({"updated": updated})


# ─────────────────────────── Распределение ───────────────────────────

_DEVICE_LETTER = {"global": "Г", "china": "К", "vietnam": "В", "": "—"}


@app.get("/distribution", response_class=HTMLResponse)
def distribution_page(request: Request):
    with Session(engine) as session:
        members = list(session.exec(
            select(Member).order_by(col(Member.team), col(Member.full_name))
        ).all())
        features = list(session.exec(
            select(Feature)
            .where(col(Feature.regression_scope).in_(["yes", "partial"]))
            .order_by(col(Feature.team), col(Feature.name))
        ).all())

    member_map: dict[str, Member] = {str(m.id): m for m in members}

    assignments: dict[str, list] = {str(m.id): [] for m in members}
    for f in features:
        if f.assigned_member_id:
            mid = str(f.assigned_member_id)
            if mid in assignments:
                assignments[mid].append(f)

    remaining: dict[str, float] = {}
    for m in members:
        total_h = m.available_days * 8
        used_h = sum(_eff_hours(f) for f in assignments[str(m.id)])
        remaining[str(m.id)] = round(total_h - used_h, 1)

    unassigned = [f for f in features if not f.assigned_member_id]

    feature_registry = {
        str(f.id): {
            "name": f.name,
            "hours": _eff_hours(f),
            "base_hours": f.check_hours,
            "short_hours": f.short_hours,
            "override_hours": f.override_hours,
            "team": f.team or "",
            "qase_url": f.qase_url or "",
        }
        for f in features
    }
    assigned_ids = [str(f.id) for f in features if f.assigned_member_id]

    r = templates.TemplateResponse("distribution.html", {
        "request": request,
        "members": members,
        "features": features,
        "member_map": member_map,
        "assignments": assignments,
        "remaining": remaining,
        "unassigned_features": unassigned,
        "device_letter": _DEVICE_LETTER,
        "feature_registry_json": json.dumps(feature_registry, ensure_ascii=False),
        "assigned_ids_json": json.dumps(assigned_ids),
    })
    _no_store(r)
    return r


@app.post("/distribution/assign")
async def assign_feature(request: Request):
    flat = await _read_form(request)
    fid_str = (_form_field(flat, "feature_id") or "").strip()
    mid_str = (_form_field(flat, "member_id") or "").strip()

    if not fid_str:
        return JSONResponse({"ok": False, "error": "feature_id required"}, status_code=422)
    try:
        fid = UUID(fid_str)
    except (ValueError, TypeError):
        return JSONResponse({"ok": False, "error": "Неверный ID фичи"}, status_code=422)

    with Session(engine) as session:
        f = session.get(Feature, fid)
        if not f:
            return JSONResponse({"ok": False, "error": "Фича не найдена"}, status_code=404)

        old_mid_str = str(f.assigned_member_id) if f.assigned_member_id else None
        old_remaining = None
        if old_mid_str:
            old_m = session.get(Member, UUID(old_mid_str))
            if old_m:
                old_feats = [
                    x for x in session.exec(
                        select(Feature).where(col(Feature.assigned_member_id) == UUID(old_mid_str))
                    ).all()
                    if str(x.id) != fid_str
                ]
                old_remaining = round(old_m.available_days * 8 - sum(_eff_hours(x) for x in old_feats), 1)

        new_mid_str = None
        new_member_name = None
        new_remaining = None

        if mid_str:
            try:
                mid = UUID(mid_str)
            except ValueError:
                return JSONResponse({"ok": False, "error": "Неверный ID участника"}, status_code=422)
            f.assigned_member_id = mid
            new_mid_str = mid_str
            new_m = session.get(Member, mid)
            if new_m:
                new_member_name = new_m.full_name
                curr_feats = [
                    x for x in session.exec(
                        select(Feature).where(col(Feature.assigned_member_id) == mid)
                    ).all()
                    if str(x.id) != fid_str
                ]
                new_remaining = round(
                    new_m.available_days * 8 - sum(_eff_hours(x) for x in curr_feats) - _eff_hours(f), 1
                )
        else:
            f.assigned_member_id = None

        f.updated_at = utcnow()
        session.add(f)
        session.commit()

        # Читаем атрибуты внутри сессии — после commit объект detach-ится
        fname  = f.name
        fhours = _eff_hours(f)

    return JSONResponse({
        "ok": True,
        "feature_id": fid_str,
        "feature_name": fname,
        "feature_hours": fhours,
        "old_member_id": old_mid_str,
        "old_member_remaining": old_remaining,
        "new_member_id": new_mid_str,
        "new_member_name": new_member_name,
        "new_member_remaining": new_remaining,
    })


# ──────────────────────────────────────────────────────────────
#  Устройства
# ──────────────────────────────────────────────────────────────

def _device_ctx(d: "Device | None" = None) -> dict:
    """Общий контекст для формы устройства."""
    return {
        "status":      d.status      if d else "",
        "employee":    d.employee    if d else "",
        "device_name": d.device_name if d else "",
        "inv_number":  d.inv_number  if d else "",
        "os_version":  d.os_version  if d else "",
        "ram_mb":      d.ram_mb      if d else "",
        "profile_global":  bool(d.profile_global)  if d else False,
        "profile_china":   bool(d.profile_china)   if d else False,
        "profile_vietnam": bool(d.profile_vietnam) if d else False,
        "root":        d.root        if d else None,
        "perf_class":  d.perf_class  if d else "",
        "asan":        d.asan        if d else None,
        "ratio":       d.ratio  or "" if d else "",
        "udid":        d.udid   or "" if d else "",
        "serial":      d.serial or "" if d else "",
        "comment":     d.comment or "" if d else "",
    }


@app.get("/devices", response_class=HTMLResponse)
def list_devices(request: Request):
    with Session(engine) as session:
        rows = list(session.exec(
            select(Device).order_by(col(Device.employee), col(Device.device_name))
        ).all())
    employees = sorted({d.employee for d in rows if d.employee})
    statuses  = sorted({d.status   for d in rows if d.status})
    perf_classes = sorted({d.perf_class for d in rows if d.perf_class})
    r = templates.TemplateResponse("devices.html", {
        "request": request,
        "devices": rows,
        "employees": employees,
        "statuses": statuses,
        "perf_classes": perf_classes,
    })
    _no_store(r)
    return r


@app.get("/members/{member_id}/devices")
def member_devices(member_id: UUID):
    """JSON: список устройств участника по имени."""
    with Session(engine) as session:
        m = session.get(Member, member_id)
        if not m:
            return JSONResponse({"devices": []})
        rows = list(session.exec(
            select(Device)
            .where(col(Device.employee) == m.full_name)
            .order_by(col(Device.device_name))
        ).all())
    result = []
    for d in rows:
        profiles = []
        if d.profile_global:  profiles.append("Г")
        if d.profile_china:   profiles.append("К")
        if d.profile_vietnam: profiles.append("В")
        result.append({
            "id":         str(d.id),
            "device":     d.device_name,
            "inv":        d.inv_number or "",
            "os":         d.os_version or "",
            "ram":        d.ram_mb,
            "perf":       d.perf_class or "",
            "profile":    profiles,
            "root":       d.root,
            "asan":       d.asan,
            "status":     d.status or "",
            "ratio":      d.ratio or "",
            "udid":       d.udid or "",
            "serial":     d.serial or "",
            "comment":    d.comment or "",
        })
    return JSONResponse({"devices": result, "name": m.full_name})


def _parse_device_form(flat: dict) -> dict:
    def s(k): return (_form_field(flat, k) or "").strip()
    def b(k): return _form_field(flat, k) == "on"
    def opt_b(k):
        v = s(k)
        if v == "true": return True
        if v == "false": return False
        return None
    try:
        ram = int(float(s("ram_mb"))) if s("ram_mb") else None
    except ValueError:
        ram = None
    return {
        "status":          s("status")[:60],
        "employee":        s("employee")[:200],
        "device_name":     s("device_name")[:300],
        "inv_number":      s("inv_number")[:100],
        "os_version":      s("os_version")[:50],
        "ram_mb":          ram,
        "profile_global":  b("profile_global"),
        "profile_china":   b("profile_china"),
        "profile_vietnam": b("profile_vietnam"),
        "profile": b("profile_global") or b("profile_china") or b("profile_vietnam"),
        "root":            opt_b("root"),
        "perf_class":      s("perf_class")[:30],
        "asan":            opt_b("asan"),
        "ratio":           s("ratio")[:300] or None,
        "udid":            s("udid")[:200] or None,
        "serial":          s("serial")[:100] or None,
        "comment":         s("comment")[:500] or None,
    }


def _device_employees() -> list[str]:
    """Список имён: сначала участники, затем уникальные имена из устройств."""
    with Session(engine) as session:
        mbr_names = {r for r in session.exec(
            select(col(Member.full_name)).where(col(Member.full_name) != "")
        ).all()}
        dev_names = {r for r in session.exec(
            select(col(Device.employee)).where(col(Device.employee) != "")
        ).all()}
    return sorted(mbr_names | dev_names)


@app.get("/devices/new", response_class=HTMLResponse)
def new_device_form(request: Request):
    r = templates.TemplateResponse("device_form.html", {
        "request": request, "device": None, "error": None,
        "employees": _device_employees(),
        **_device_ctx()
    })
    _no_store(r)
    return r


@app.post("/devices/new", response_class=HTMLResponse)
async def create_device(request: Request):
    flat = await _read_form(request)
    data = _parse_device_form(flat)
    if not data["device_name"] and not data["employee"]:
        r = templates.TemplateResponse("device_form.html", {
            "request": request, "device": None,
            "error": "Укажите название устройства или сотрудника",
            "employees": _device_employees(),
            **data
        })
        _no_store(r); return r
    with Session(engine) as session:
        d = Device(**data)
        session.add(d)
        session.commit()
    return RedirectResponse("/devices", status_code=303)


@app.get("/devices/{device_id}/edit", response_class=HTMLResponse)
def edit_device_form(request: Request, device_id: UUID):
    with Session(engine) as session:
        d = session.get(Device, device_id)
        if not d:
            return HTMLResponse("Не найдено", status_code=404)
        ctx = _device_ctx(d)
        did = str(d.id)
    r = templates.TemplateResponse("device_form.html", {
        "request": request, "device": {"id": did}, "error": None,
        "employees": _device_employees(),
        **ctx
    })
    _no_store(r)
    return r


@app.post("/devices/{device_id}/edit", response_class=HTMLResponse)
async def update_device(request: Request, device_id: UUID):
    flat = await _read_form(request)
    data = _parse_device_form(flat)
    with Session(engine) as session:
        d = session.get(Device, device_id)
        if not d:
            return HTMLResponse("Не найдено", status_code=404)
        for k, v in data.items():
            setattr(d, k, v)
        session.add(d)
        session.commit()
    return RedirectResponse("/devices", status_code=303)


@app.post("/devices/{device_id}/delete", response_class=HTMLResponse)
def delete_device(device_id: UUID):
    with Session(engine) as session:
        d = session.get(Device, device_id)
        if d:
            session.delete(d)
            session.commit()
    return RedirectResponse("/devices", status_code=303)


# ──────────────────────────────────────────────────────────────
#  История регрессов
# ──────────────────────────────────────────────────────────────

def _build_snapshot() -> dict:
    """Собирает снимок текущего состояния распределения."""
    with Session(engine) as session:
        members = list(session.exec(
            select(Member).order_by(col(Member.team), col(Member.full_name))
        ).all())
        features = list(session.exec(
            select(Feature).where(
                Feature.regression_scope.in_(["yes", "partial"])
            ).order_by(col(Feature.team), col(Feature.name))
        ).all())

    mbr_map = {str(m.id): m for m in members}

    # Группируем фичи по участнику
    assignments: dict[str, list[dict]] = {str(m.id): [] for m in members}
    unassigned = []
    for f in features:
        entry = {
            "id":    str(f.id),
            "team":  f.team,
            "name":  f.name,
            "hours": f.effective_hours,
            "scope": f.regression_scope,
        }
        mid = str(f.assigned_member_id) if f.assigned_member_id else None
        if mid and mid in assignments:
            assignments[mid].append(entry)
        else:
            unassigned.append(entry)

    members_data = []
    for m in members:
        mid = str(m.id)
        total_h = m.available_days * 8
        assigned = assignments[mid]
        spent_h = sum(a["hours"] for a in assigned)
        members_data.append({
            "id":             mid,
            "full_name":      m.full_name,
            "team":           m.team,
            "available_days": m.available_days,
            "total_hours":    total_h,
            "spent_hours":    spent_h,
            "remaining_hours": round(total_h - spent_h, 2),
            "device":         m.device,
            "comment":        m.comment or "",
            "features":       assigned,
        })

    total_avail_days  = round(sum(m["available_days"] for m in members_data), 2)
    total_avail_hours = round(sum(m["total_hours"]    for m in members_data), 2)
    total_spent_hours = round(sum(m["spent_hours"]    for m in members_data), 2)
    total_feat_hours  = round(sum(f["hours"] for m in members_data for f in m["features"]), 2)

    return {
        "members":    members_data,
        "unassigned": unassigned,
        "totals": {
            "members_count":      len(members),
            "features_count":     len(features),
            "unassigned_count":   len(unassigned),
            "total_avail_days":   total_avail_days,
            "total_avail_hours":  total_avail_hours,
            "total_spent_hours":  total_spent_hours,
            "total_feat_hours":   total_feat_hours,
        }
    }


@app.get("/dashboards", response_class=HTMLResponse)
def dashboards(request: Request):
    with Session(engine) as session:
        snapshots = list(session.exec(
            select(HistorySnapshot).order_by(HistorySnapshot.created_at)
        ).all())
    snaps_data = []
    for h in snapshots:
        t = _json.loads(h.snapshot)["totals"]
        snaps_data.append({
            "id":           str(h.id),
            "version":      h.version,
            "created_at":   h.created_at.strftime("%d.%m.%Y"),
            "avail_hours":  t.get("total_avail_hours", 0) or 0,
            "spent_hours":  t.get("total_spent_hours", 0) or 0,
            "actual_hours": t.get("actual_hours") or None,
            "avail_days":   t.get("total_avail_days", 0) or 0,
            "features":     t.get("features_count", 0) or 0,
            "unassigned":   t.get("unassigned_count", 0) or 0,
            "members":      t.get("members_count", 0) or 0,
        })

    # Для детального дашборда по участникам берём все снимки
    members_by_snap: dict[str, list] = {}
    for h in snapshots:
        snap = _json.loads(h.snapshot)
        members_by_snap[str(h.id)] = snap.get("members", [])

    r = templates.TemplateResponse("dashboards.html", {
        "request": request,
        "snaps":   snaps_data,
        "members_by_snap": _json.dumps(members_by_snap, ensure_ascii=False),
    })
    _no_store(r)
    return r


@app.get("/history", response_class=HTMLResponse)
def list_history(request: Request):
    with Session(engine) as session:
        snapshots = list(session.exec(
            select(HistorySnapshot).order_by(HistorySnapshot.created_at.desc())
        ).all())
    r = templates.TemplateResponse("history_list.html", {
        "request": request, "snapshots": snapshots
    })
    _no_store(r)
    return r


@app.post("/history")
async def save_history(request: Request):
    flat = await _read_form(request)
    version = (_form_field(flat, "version")      or "").strip()
    note    = (_form_field(flat, "note")         or "").strip()
    actual_raw = (_form_field(flat, "actual_hours") or "").strip()
    if not version:
        return JSONResponse({"error": "Укажите версию"}, status_code=400)
    try:
        actual_hours = float(actual_raw) if actual_raw else None
    except ValueError:
        actual_hours = None
    snap = _build_snapshot()
    snap["totals"]["actual_hours"] = actual_hours
    with Session(engine) as session:
        h = HistorySnapshot(
            version=version[:100],
            note=note[:500] or None,
            snapshot=_json.dumps(snap, ensure_ascii=False),
        )
        session.add(h)
        session.commit()
        hid = str(h.id)
    return JSONResponse({"id": hid, "version": version})


@app.get("/history/{snapshot_id}", response_class=HTMLResponse)
def view_history(request: Request, snapshot_id: UUID):
    with Session(engine) as session:
        h = session.get(HistorySnapshot, snapshot_id)
        if not h:
            return HTMLResponse("Не найдено", status_code=404)
        snap = _json.loads(h.snapshot)
        version    = h.version
        note       = h.note or ""
        created_at = h.created_at
        hid        = str(h.id)
    r = templates.TemplateResponse("history_detail.html", {
        "request": request,
        "version": version, "note": note,
        "created_at": created_at, "hid": hid,
        "snap": snap,
    })
    _no_store(r)
    return r


@app.post("/history/{snapshot_id}/edit")
async def edit_history(request: Request, snapshot_id: UUID):
    flat = await _read_form(request)
    version    = (_form_field(flat, "version") or "").strip()
    actual_raw = (_form_field(flat, "actual_hours") or "").strip()
    if not version:
        return JSONResponse({"error": "Укажите версию"}, status_code=400)
    try:
        actual_hours = float(actual_raw) if actual_raw else None
    except ValueError:
        actual_hours = None
    with Session(engine) as session:
        h = session.get(HistorySnapshot, snapshot_id)
        if not h:
            return JSONResponse({"error": "Не найдено"}, status_code=404)
        h.version = version[:100]
        snap = _json.loads(h.snapshot)
        snap["totals"]["actual_hours"] = actual_hours
        h.snapshot = _json.dumps(snap, ensure_ascii=False)
        session.add(h)
        session.commit()
    return JSONResponse({"ok": True, "version": version, "actual_hours": actual_hours})


@app.post("/history/{snapshot_id}/delete", response_class=HTMLResponse)
def delete_history(snapshot_id: UUID):
    with Session(engine) as session:
        h = session.get(HistorySnapshot, snapshot_id)
        if h:
            session.delete(h)
            session.commit()
    return RedirectResponse("/history", status_code=303)


# ──────────────────────────────────────────────────────────────
#  Qase интеграция
# ──────────────────────────────────────────────────────────────

def _qase_parse_url(url: str) -> tuple[str, int, dict[str, str]] | None:
    """
    Парсит URL суита Qase.
    Возвращает (project_code, suite_id, custom_field_filters).

    custom_field_filters: {field_id_str: value_str}  для передачи в API
    Пример URL:
      https://app.qase.io/project/FD?customFields={"134":[1],"136":[1]}&suite=17411
    """
    from urllib.parse import urlparse, parse_qs, unquote

    parsed  = urlparse(url)
    qs      = parse_qs(parsed.query, keep_blank_values=True)

    # 1. Код проекта
    code: str | None = None
    m = re.search(r'/(?:project|repository|suite)/([A-Z0-9]{2,10})(?:/|$|\?)', url, re.I)
    if m:
        code = m.group(1).upper()
    else:
        m2 = re.search(r'/([A-Z]{2,10})(?:/|[?#&]|$)', url, re.I)
        if m2:
            code = m2.group(1).upper()

    # 2. suite_id
    suite_id: int | None = None
    # /suite/CODE/ID
    ms = re.search(r'/suite/[A-Z0-9]+/(\d+)', url, re.I)
    if ms:
        suite_id = int(ms.group(1))
    else:
        for key in ("suite", "suiteId"):
            if key in qs:
                suite_id = int(qs[key][0])
                break
        if suite_id is None:
            mi = re.search(r'[=/](\d{3,})', url)
            if mi:
                suite_id = int(mi.group(1))

    if not code or suite_id is None:
        return None

    # 3. customFields → custom_field filters для API
    cf_filters: dict[str, str] = {}
    raw_cf = qs.get("customFields", [None])[0]
    if raw_cf:
        try:
            cf_map: dict = _json.loads(unquote(raw_cf))
            for fid, vals in cf_map.items():
                if isinstance(vals, list) and vals:
                    cf_filters[str(fid)] = str(vals[0])
                elif vals:
                    cf_filters[str(fid)] = str(vals)
        except Exception:
            pass

    return code, suite_id, cf_filters


def _qase_get(path: str) -> dict:
    req = urllib.request.Request(
        f"{QASE_API_BASE}{path}",
        headers={"Token": QASE_TOKEN, "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return _json.loads(resp.read())


def _qase_post(path: str, payload: dict) -> dict:
    data = _json.dumps(payload, ensure_ascii=False).encode()
    req = urllib.request.Request(
        f"{QASE_API_BASE}{path}",
        data=data,
        headers={"Token": QASE_TOKEN, "Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        return _json.loads(resp.read())


import time as _time

_suite_tree_cache: dict[str, tuple[float, dict, dict]] = {}
_SUITE_TREE_TTL = 600  # 10 минут


def _qase_suite_tree(code: str) -> tuple[dict[int, list[int]], dict[int, int]]:
    """
    Загружает дерево суитов проекта (с кешем на 10 мин).
    Возвращает: (children {parent_id: [child_id,...]}, cases_count {suite_id: n}).
    """
    now = _time.monotonic()
    if code in _suite_tree_cache:
        ts, ch, sc = _suite_tree_cache[code]
        if now - ts < _SUITE_TREE_TTL:
            return ch, sc

    all_suites: list[dict] = []
    limit, offset = 100, 0
    while True:
        data = _qase_get(f"/suite/{code}?limit={limit}&offset={offset}")
        entities = data.get("result", {}).get("entities", [])
        all_suites.extend(entities)
        total = data.get("result", {}).get("total", 0)
        offset += limit
        if offset >= total:
            break

    children: dict[int, list[int]] = {}
    suite_cases: dict[int, int] = {}
    for s in all_suites:
        pid = s.get("parent_id")
        sid = s["id"]
        if pid is not None:
            children.setdefault(pid, []).append(sid)
        suite_cases[sid] = s.get("cases_count", 0) or 0

    _suite_tree_cache[code] = (now, children, suite_cases)
    return children, suite_cases


def _case_matches_cf_filters(case: dict, cf_filters: dict[str, str]) -> bool:
    """
    Проверяет, что кейс соответствует хотя бы одному фильтру по кастомным полям.
    Qase UI применяет OR-логику между разными custom field условиями (если cf134=1
    не заполнено ни у одного кейса, а cf136=1 у 48 кейсов, показывает 48).
    API v1 не поддерживает серверную фильтрацию — применяем клиентски.
    """
    if not cf_filters:
        return True
    cf_map = {str(cf["id"]): str(cf.get("value", "") or "") for cf in case.get("custom_fields", [])}
    # OR: достаточно совпадения хотя бы по одному полю
    for fid, expected in cf_filters.items():
        actual = cf_map.get(str(fid))
        if actual is not None and actual == expected:
            return True
    return False


def _qase_collect_cases(
    code: str, suite_id: int, cf_filters: dict[str, str] | None = None
) -> list[int]:
    """
    Собирает все case_id из суита и всех его дочерних суитов (рекурсивно).
    cf_filters: фильтр по кастомным полям {"136": "1"} — применяется клиентски,
    так как API v1 не поддерживает серверную фильтрацию по custom_field.
    Суиты с cases_count == 0 пропускаются для экономии запросов.
    """
    children, suite_cases = _qase_suite_tree(code)

    # BFS: собираем все suite_id в поддереве
    suites_to_query: list[int] = []
    stack = [suite_id]
    while stack:
        cur = stack.pop()
        if suite_cases.get(cur, 0) > 0:
            suites_to_query.append(cur)
        for ch in children.get(cur, []):
            stack.append(ch)

    ids: list[int] = []
    limit = 100
    for sid in suites_to_query:
        offset = 0
        while True:
            data     = _qase_get(f"/case/{code}?suite_id={sid}&limit={limit}&offset={offset}")
            result   = data.get("result", {})
            entities = result.get("entities", [])
            for e in entities:
                if _case_matches_cf_filters(e, cf_filters or {}):
                    ids.append(e["id"])
            filtered = result.get("filtered", 0) or 0
            offset  += limit
            if offset >= filtered:
                break
    return ids


@app.post("/api/qase/create-run")
async def qase_create_run(request: Request):
    flat       = await _read_form(request)
    feature_id_str = _form_field(flat, "feature_id") or ""
    run_title  = (_form_field(flat, "run_title") or "").strip()

    if not run_title:
        return JSONResponse({"error": "Укажите название Test Run"}, status_code=400)

    # Достаём qase_url фичи из БД
    try:
        fid = UUID(feature_id_str)
    except ValueError:
        return JSONResponse({"error": "Неверный ID фичи"}, status_code=400)

    with Session(engine) as session:
        f = session.get(Feature, fid)
        if not f or not f.qase_url:
            return JSONResponse({"error": "У фичи нет ссылки на Qase"}, status_code=400)
        qase_url = f.qase_url

    parsed = _qase_parse_url(qase_url)
    if not parsed:
        return JSONResponse({"error": f"Не удалось распознать URL суита: {qase_url}"}, status_code=400)

    code, suite_id, cf_filters = parsed

    try:
        case_ids = _qase_collect_cases(code, suite_id, cf_filters)
        if not case_ids:
            return JSONResponse({"error": f"В суите {code}/{suite_id} не найдено кейсов"}, status_code=400)

        result = _qase_post(f"/run/{code}", {
            "title": run_title,
            "cases": case_ids,
        })
        run_id  = result["result"]["id"]
        run_url = f"https://app.qase.io/run/{code}/dashboard/{run_id}"
        return JSONResponse({
            "ok":      True,
            "run_id":  run_id,
            "run_url": run_url,
            "cases":   len(case_ids),
        })
    except Exception as e:
        return JSONResponse({"error": f"Ошибка Qase API: {e}"}, status_code=500)


@app.post("/api/qase/create-team-run")
async def qase_create_team_run(request: Request):
    """
    Создаёт Test Run из объединения кейсов всех чек-листов Qase,
    назначенных на участников одной команды.
    Принимает: run_title, feature_ids[] — список ID фич с qase_url.
    """
    body      = await request.body()
    raw       = parse_qs(body.decode("utf-8", errors="replace"), keep_blank_values=False)
    run_title = (raw.get("run_title", [""])[0] or "").strip()
    fid_strs  = raw.get("feature_ids", [])

    if not run_title:
        return JSONResponse({"error": "Укажите название Test Run"}, status_code=400)
    if not fid_strs:
        return JSONResponse({"error": "Нет фич с чек-листами Qase для этой команды"}, status_code=400)

    # Собираем qase_url для всех переданных фич
    qase_urls: list[str] = []
    with Session(engine) as session:
        for fs in fid_strs:
            try:
                fid = UUID(fs.strip())
            except ValueError:
                continue
            f = session.get(Feature, fid)
            if f and f.qase_url:
                qase_urls.append(f.qase_url)

    if not qase_urls:
        return JSONResponse({"error": "У выбранных фич нет ссылок на Qase"}, status_code=400)

    # Определяем код проекта (берём из первого URL, предполагаем один проект)
    first_parsed = _qase_parse_url(qase_urls[0])
    if not first_parsed:
        return JSONResponse({"error": f"Не удалось распознать URL: {qase_urls[0]}"}, status_code=400)
    project_code = first_parsed[0]

    try:
        all_case_ids: set[int] = set()
        skipped: list[str] = []
        for url in qase_urls:
            parsed = _qase_parse_url(url)
            if not parsed:
                skipped.append(url)
                continue
            code, suite_id, cf_filters = parsed
            ids = _qase_collect_cases(code, suite_id, cf_filters)
            all_case_ids.update(ids)

        if not all_case_ids:
            return JSONResponse({"error": "Не найдено кейсов ни в одном из чек-листов"}, status_code=400)

        result = _qase_post(f"/run/{project_code}", {
            "title": run_title,
            "cases": list(all_case_ids),
        })
        run_id  = result["result"]["id"]
        run_url = f"https://app.qase.io/run/{project_code}/dashboard/{run_id}"
        return JSONResponse({
            "ok":       True,
            "run_id":   run_id,
            "run_url":  run_url,
            "cases":    len(all_case_ids),
            "checklists": len(qase_urls),
            "skipped":  len(skipped),
        })
    except Exception as e:
        return JSONResponse({"error": f"Ошибка Qase API: {e}"}, status_code=500)
