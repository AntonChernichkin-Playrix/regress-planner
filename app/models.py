from datetime import datetime, timezone
from typing import Optional
from uuid import UUID, uuid4

from sqlmodel import Field, SQLModel, Column
import sqlalchemy as sa


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


# Допустимые значения поля regression_scope
REGRESSION_SCOPE_CHOICES = ("", "yes", "partial", "no")
REGRESSION_SCOPE_LABELS: dict[str, str] = {
    "":        "Не указано",
    "yes":     "Да",
    "partial": "Частично",
    "no":      "Нет",
}

# Профильный девайс участника
DEVICE_CHOICES = ("", "global", "china", "vietnam")
DEVICE_LABELS: dict[str, str] = {
    "":        "Не указано",
    "global":  "Глобальный",
    "china":   "Китайский",
    "vietnam": "Вьетнам",
}


class Member(SQLModel, table=True):
    __tablename__ = "members"

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    full_name: str = Field(max_length=200, index=True)
    team: str = Field(default="", max_length=120, index=True)
    available_days: float = Field(default=0, ge=0, description="Доступных дней на регресс")
    device: str = Field(default="", max_length=50, description="Профильный девайс (comma-separated)")
    comment: Optional[str] = Field(default=None, max_length=500, description="Комментарий")
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)


class Device(SQLModel, table=True):
    __tablename__ = "devices"

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    status: str = Field(default="", max_length=60)
    employee: str = Field(default="", max_length=200, index=True)
    device_name: str = Field(default="", max_length=300, index=True)
    inv_number: str = Field(default="", max_length=100)
    os_version: str = Field(default="", max_length=50)
    ram_mb: Optional[int] = Field(default=None)
    profile: Optional[bool] = Field(default=None)
    profile_global: Optional[bool] = Field(default=None)
    profile_china: Optional[bool] = Field(default=None)
    profile_vietnam: Optional[bool] = Field(default=None)
    root: Optional[bool] = Field(default=None)
    perf_class: str = Field(default="", max_length=30)
    asan: Optional[bool] = Field(default=None)
    ratio: Optional[str] = Field(default=None, max_length=300)
    udid: Optional[str] = Field(default=None, max_length=200)
    serial: Optional[str] = Field(default=None, max_length=100)
    comment: Optional[str] = Field(default=None, max_length=500)
    created_at: datetime = Field(default_factory=utcnow)


class Feature(SQLModel, table=True):
    __tablename__ = "features"

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    team: str = Field(default="", max_length=120, index=True)
    name: str = Field(max_length=500, index=True)
    check_hours: float = Field(default=0, ge=0, description="Плановые часы на проверку")
    short_hours: Optional[float] = Field(default=None, ge=0, description="Часов - короткий (приоритет над check_hours)")
    override_hours: Optional[float] = Field(default=None, ge=0, description="Часов в эту версию (наивысший приоритет)")
    regression_scope: str = Field(default="", max_length=10, description="Нужен ли регресс в текущую версию")
    assigned_member_id: Optional[UUID] = Field(default=None, index=True, description="Назначен участнику")
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)
    comment: Optional[str] = Field(default=None, max_length=500, description="Комментарий")
    qase_url: Optional[str] = Field(default=None, max_length=500, description="Ссылка на чек-лист Qase")
    # для будущей синхронизации с внешней системой
    external_ref: Optional[str] = Field(default=None, max_length=200, index=True)

    @property
    def effective_hours(self) -> float:
        """Приоритет: override_hours > short_hours > check_hours."""
        if self.override_hours is not None:
            return self.override_hours
        if self.short_hours is not None:
            return self.short_hours
        return self.check_hours


class HistorySnapshot(SQLModel, table=True):
    __tablename__ = "history_snapshots"

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    version: str = Field(max_length=100, description="Версия / метка регресса")
    note: Optional[str] = Field(default=None, max_length=500, description="Примечание")
    snapshot: str = Field(sa_column=Column(sa.Text, nullable=False), description="JSON-снимок состояния")
    created_at: datetime = Field(default_factory=utcnow)
