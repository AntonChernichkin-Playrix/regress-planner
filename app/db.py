import os

from sqlalchemy import inspect, text
from sqlmodel import Session, SQLModel, create_engine

# Путь к БД: переменная окружения DATA_DIR (для Railway Volume),
# иначе — рядом с проектом в папке data/
_default_data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
_DATA_DIR = os.environ.get("DATA_DIR", _default_data_dir)
os.makedirs(_DATA_DIR, exist_ok=True)

_DB_PATH = os.path.join(_DATA_DIR, "features.db")

# Если БД пустая или отсутствует (первый запуск на Railway Volume) —
# копируем семенную БД из репозитория
def _db_is_empty(path: str) -> bool:
    """Возвращает True если файл не существует или не содержит таблицы features."""
    if not os.path.exists(path) or os.path.getsize(path) < 1024:
        return True
    try:
        import sqlite3 as _sqlite3
        with _sqlite3.connect(path) as _c:
            tables = {r[0] for r in _c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if "features" not in tables:
                return True
            count = _c.execute("SELECT COUNT(*) FROM features").fetchone()[0]
            return count == 0
    except Exception:
        return True

if _db_is_empty(_DB_PATH):
    import shutil
    _SEED = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data_seed", "features.db")
    if os.path.exists(_SEED):
        shutil.copy2(_SEED, _DB_PATH)

DATABASE_URL = f"sqlite:///{_DB_PATH}"

connect_args = {"check_same_thread": False}
engine = create_engine(DATABASE_URL, connect_args=connect_args)


def _add_column_if_missing(cols: set, col_name: str, ddl: str, conn) -> None:
    if col_name not in cols:
        conn.execute(text(ddl))


def _run_migrations() -> None:
    insp = inspect(engine)

    if insp.has_table("features"):
        cols = {c["name"] for c in insp.get_columns("features")}
        with engine.begin() as conn:
            _add_column_if_missing(
                cols, "team",
                "ALTER TABLE features ADD COLUMN team VARCHAR(120) NOT NULL DEFAULT ''",
                conn,
            )
            if "team" not in cols:
                conn.execute(text(
                    "CREATE INDEX IF NOT EXISTS ix_features_team ON features (team)"
                ))
            _add_column_if_missing(
                cols, "regression_scope",
                "ALTER TABLE features ADD COLUMN regression_scope VARCHAR(10) NOT NULL DEFAULT ''",
                conn,
            )
            _add_column_if_missing(
                cols, "assigned_member_id",
                "ALTER TABLE features ADD COLUMN assigned_member_id VARCHAR(36) DEFAULT NULL",
                conn,
            )
            _add_column_if_missing(
                cols, "short_hours",
                "ALTER TABLE features ADD COLUMN short_hours REAL DEFAULT NULL",
                conn,
            )
            _add_column_if_missing(
                cols, "comment",
                "ALTER TABLE features ADD COLUMN comment VARCHAR(500) DEFAULT NULL",
                conn,
            )
            _add_column_if_missing(
                cols, "override_hours",
                "ALTER TABLE features ADD COLUMN override_hours REAL DEFAULT NULL",
                conn,
            )
            _add_column_if_missing(
                cols, "qase_url",
                "ALTER TABLE features ADD COLUMN qase_url VARCHAR(500) DEFAULT NULL",
                conn,
            )

    if insp.has_table("members"):
        mcols = {c["name"] for c in insp.get_columns("members")}
        with engine.begin() as conn:
            _add_column_if_missing(
                mcols, "device",
                "ALTER TABLE members ADD COLUMN device VARCHAR(20) NOT NULL DEFAULT ''",
                conn,
            )
            _add_column_if_missing(
                mcols, "comment",
                "ALTER TABLE members ADD COLUMN comment VARCHAR(500) DEFAULT NULL",
                conn,
            )


    if insp.has_table("devices"):
        dcols = {c["name"] for c in insp.get_columns("devices")}
        with engine.begin() as conn:
            _add_column_if_missing(dcols, "profile_global",
                "ALTER TABLE devices ADD COLUMN profile_global BOOLEAN DEFAULT NULL", conn)
            _add_column_if_missing(dcols, "profile_china",
                "ALTER TABLE devices ADD COLUMN profile_china BOOLEAN DEFAULT NULL", conn)
            _add_column_if_missing(dcols, "profile_vietnam",
                "ALTER TABLE devices ADD COLUMN profile_vietnam BOOLEAN DEFAULT NULL", conn)
            _add_column_if_missing(dcols, "ratio",
                "ALTER TABLE devices ADD COLUMN ratio VARCHAR(300) DEFAULT NULL", conn)
            _add_column_if_missing(dcols, "udid",
                "ALTER TABLE devices ADD COLUMN udid VARCHAR(200) DEFAULT NULL", conn)
            _add_column_if_missing(dcols, "serial",
                "ALTER TABLE devices ADD COLUMN serial VARCHAR(100) DEFAULT NULL", conn)


def init_db() -> None:
    SQLModel.metadata.create_all(engine)
    _run_migrations()


def get_all_teams() -> list[str]:
    """Уникальные команды из обеих таблиц — для автодополнения."""
    from sqlmodel import Session, text as _text
    teams: set[str] = set()
    with Session(engine) as s:
        for row in s.exec(_text("SELECT DISTINCT team FROM features WHERE team != '' UNION SELECT DISTINCT team FROM members WHERE team != ''")).all():
            teams.add(row[0])
    return sorted(teams)
