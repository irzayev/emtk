import os
import re
import smtplib
import uuid
from io import BytesIO
from decimal import Decimal
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo
from email.message import EmailMessage
from functools import wraps
from pathlib import Path
from typing import Optional

from flask import Flask, abort, flash, redirect, render_template, request, send_file, session, url_for
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_sqlalchemy import SQLAlchemy
from flask_wtf.csrf import CSRFProtect
from openpyxl import Workbook
from sqlalchemy import delete as sa_delete
from sqlalchemy import exists, inspect as sa_inspect, select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename


app = Flask(__name__)
# Persist SQLite DB in /app/instance (volume-mounted in compose).
app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("DATABASE_URL", "sqlite:////app/instance/smart_zhk.db")
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
debug_mode = os.getenv("FLASK_DEBUG", "0") == "1"
secret_key = os.getenv("SECRET_KEY")
if not secret_key:
    if debug_mode:
        # Development-only fallback to avoid shipping a static weak key.
        secret_key = "dev-only-secret-change-me"
    else:
        raise RuntimeError("SECRET_KEY must be set in production.")
app.config["SECRET_KEY"] = secret_key
app.config["UPLOAD_FOLDER"] = os.path.join("static", "uploads")

# Session cookie hardening (behind HTTPS reverse proxy).
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = os.getenv("SESSION_COOKIE_SAMESITE", "Lax")
app.config["SESSION_COOKIE_SECURE"] = os.getenv("SESSION_COOKIE_SECURE", "1") == "1"

# CSRF protection for all mutating requests.
csrf = CSRFProtect(app)

# Basic rate limiting (in-memory). For multi-instance production, use Redis storage.
limiter = Limiter(get_remote_address, app=app, default_limits=[])
db = SQLAlchemy(app)


def _app_timezone():
    """IANA timezone for displaying stored UTC timestamps (env TZ, default Asia/Baku)."""
    tz_name = (os.getenv("TZ") or "Asia/Baku").strip()
    try:
        return ZoneInfo(tz_name)
    except Exception:
        return timezone.utc


def utc_to_local(dt: Optional[datetime]) -> datetime:
    """Convert app UTC datetimes to local wall time (server zone)."""
    if dt is None:
        dt = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(_app_timezone())


@app.context_processor
def inject_now():
    return {"now": lambda: datetime.now(timezone.utc)}


@app.template_filter("azn")
def azn(value):
    try:
        return f"{float(value):.2f} AZN"
    except (TypeError, ValueError):
        return "0.00 AZN"


class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    full_name = db.Column(db.String(120), nullable=False)
    phone = db.Column(db.String(30), nullable=True)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)
    role = db.Column(db.String(20), nullable=False, default="resident")


class Building(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False, unique=True)
    address = db.Column(db.String(255), nullable=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))


class Apartment(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    number = db.Column(db.String(20), unique=True, nullable=False)
    floor = db.Column(db.Integer, nullable=False)
    rooms = db.Column(db.Integer, nullable=True)
    area = db.Column(db.Float, nullable=False)
    owner_user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    owner = db.relationship("User", backref="apartments")
    credit_balance = db.Column(db.Numeric(12, 2), nullable=False, default=Decimal("0.00"))
    building_id = db.Column(db.Integer, db.ForeignKey("building.id"), nullable=True)
    building = db.relationship("Building", backref="apartments")


class Tariff(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False)
    type = db.Column(db.String(20), nullable=False)  # per_m2 | fixed
    amount = db.Column(db.Numeric(12, 2), nullable=False)
    is_active = db.Column(db.Boolean, nullable=False, default=True)


class ApartmentPreset(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False)
    rooms = db.Column(db.Integer, nullable=False)
    area = db.Column(db.Float, nullable=False)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))


class TariffApartment(db.Model):
    __table_args__ = (db.UniqueConstraint("tariff_id", "apartment_id", name="uq_tariff_apartment"),)
    id = db.Column(db.Integer, primary_key=True)
    tariff_id = db.Column(db.Integer, db.ForeignKey("tariff.id"), nullable=False)
    apartment_id = db.Column(db.Integer, db.ForeignKey("apartment.id"), nullable=False)
    tariff = db.relationship("Tariff", backref="apartment_links")
    apartment = db.relationship("Apartment", backref="tariff_links")


class Invoice(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    apartment_id = db.Column(db.Integer, db.ForeignKey("apartment.id"), nullable=False)
    apartment = db.relationship("Apartment", backref="invoices")
    period = db.Column(db.String(7), nullable=False)  # YYYY-MM
    amount = db.Column(db.Numeric(12, 2), nullable=False)
    paid_amount = db.Column(db.Numeric(12, 2), nullable=False, default=Decimal("0.00"))
    status = db.Column(db.String(20), nullable=False, default="gozlemede")
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))


class Payment(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    invoice_id = db.Column(db.Integer, db.ForeignKey("invoice.id"), nullable=False)
    invoice = db.relationship("Invoice", backref="payments")
    amount = db.Column(db.Numeric(12, 2), nullable=False)
    comment = db.Column(db.String(255), nullable=True)
    status = db.Column(db.String(20), nullable=False, default="pending")  # pending | confirmed | rejected
    reviewer_user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=True)
    reviewed_at = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))


class SmtpConfig(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    host = db.Column(db.String(120), nullable=True)
    port = db.Column(db.Integer, nullable=False, default=587)
    username = db.Column(db.String(120), nullable=True)
    password = db.Column(db.String(255), nullable=True)
    sender_email = db.Column(db.String(120), nullable=True)
    use_tls = db.Column(db.Boolean, nullable=False, default=True)
    system_name = db.Column(db.String(120), nullable=True)
    house_address = db.Column(db.String(255), nullable=True)
    commandant_name = db.Column(db.String(120), nullable=True)
    contact_phone = db.Column(db.String(50), nullable=True)


class WorkLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(200), nullable=False)
    description = db.Column(db.Text, nullable=False)
    before_photo_url = db.Column(db.String(255), nullable=True)
    after_photo_url = db.Column(db.String(255), nullable=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))


class Announcement(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(200), nullable=False)
    text = db.Column(db.Text, nullable=False)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))


class Poll(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(200), nullable=False)
    is_anonymous = db.Column(db.Boolean, nullable=False, default=True)
    is_open = db.Column(db.Boolean, nullable=False, default=True)
    result_visibility = db.Column(db.String(20), nullable=False, default="immediate")
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))


class Vote(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    poll_id = db.Column(db.Integer, db.ForeignKey("poll.id"), nullable=False)
    apartment_id = db.Column(db.Integer, db.ForeignKey("apartment.id"), nullable=False)
    choice = db.Column(db.Enum("yes", "no", name="vote_choice"), nullable=False)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))


class AuditLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    actor_user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    action = db.Column(db.String(255), nullable=False)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))


# Kateqoriyalar (Xərclər): şablon və xərc sətirlərində istifadə olunur.
EXPENSE_CATEGORIES = (
    "əmək haqqı",
    "əlavə hərc",
    "komunal",
    "servis",
)


def _parse_expense_category(raw: Optional[str]) -> Optional[str]:
    v = (raw or "").strip()
    return v if v in EXPENSE_CATEGORIES else None


class ExpenseTemplate(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False)
    category = db.Column(db.String(64), nullable=True)
    default_amount = db.Column(db.Numeric(12, 2), nullable=False, default=Decimal("0.00"))
    is_recurring = db.Column(db.Boolean, nullable=False, default=True)
    is_active = db.Column(db.Boolean, nullable=False, default=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))


class Expense(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    period = db.Column(db.String(7), nullable=False)  # YYYY-MM
    name = db.Column(db.String(120), nullable=False)
    category = db.Column(db.String(64), nullable=True)
    amount = db.Column(db.Numeric(12, 2), nullable=False)
    is_paid = db.Column(db.Boolean, nullable=False, default=False)
    paid_at = db.Column(db.DateTime, nullable=True)
    template_id = db.Column(db.Integer, db.ForeignKey("expense_template.id"), nullable=True)
    template = db.relationship("ExpenseTemplate", backref="expenses")
    created_by_user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))


class BalanceTopUp(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    amount = db.Column(db.Numeric(12, 2), nullable=False)
    comment = db.Column(db.String(255), nullable=True)
    created_by_user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))


def audit(action: str) -> None:
    user_id = session.get("user_id")
    if user_id:
        db.session.add(AuditLog(actor_user_id=user_id, action=action))
        db.session.commit()


def _apply_credit_to_invoice(invoice: "Invoice") -> float:
    """Apply apartment credit to this invoice, return applied amount."""
    apt = invoice.apartment
    if not apt:
        return 0.0
    credit = float(apt.credit_balance or 0)
    if credit <= 0:
        return 0.0
    remaining = max(0.0, float(invoice.amount or 0) - float(invoice.paid_amount or 0))
    if remaining <= 0:
        return 0.0
    applied = min(credit, remaining)
    invoice.paid_amount = round(float(invoice.paid_amount or 0) + applied, 2)
    apt.credit_balance = round(credit - applied, 2)
    invoice.status = "odenilib" if float(invoice.paid_amount or 0) >= float(invoice.amount or 0) else "gozlemede"
    return round(applied, 2)


def _move_invoice_overpay_to_credit(invoice: "Invoice") -> float:
    """If invoice is overpaid, cap paid_amount and move overflow to apartment credit."""
    apt = invoice.apartment
    if not apt:
        return 0.0
    overflow = float(invoice.paid_amount or 0) - float(invoice.amount or 0)
    if overflow <= 0:
        return 0.0
    invoice.paid_amount = float(invoice.amount or 0)
    apt.credit_balance = round(float(apt.credit_balance or 0) + overflow, 2)
    invoice.status = "odenilib"
    return round(overflow, 2)


def _apply_payment_delta(invoice: "Invoice", delta: float, *, debt_adjustment: bool = False) -> dict:
    """
    Apply a payment delta to an invoice/apartment.
    Positive delta increases paid; negative delta is a correction (decrease) or,
    when debt_adjustment is True (admin negative Mədaxil), records extra debt
    by lowering paid_amount (may go negative so balance = paid - amount matches).

    Returns dict with keys: moved_to_credit, removed_from_credit.
    """
    apt = invoice.apartment
    if not apt:
        # No apartment linked — just update paid_amount directly.
        invoice.paid_amount = round(float(invoice.paid_amount or 0) + float(delta), 2)
        invoice.status = "odenilib" if float(invoice.paid_amount or 0) >= float(invoice.amount or 0) else "gozlemede"
        return {"moved_to_credit": 0.0, "removed_from_credit": 0.0}

    delta = float(delta or 0)
    moved_to_credit = 0.0
    removed_from_credit = 0.0

    if delta >= 0:
        # Payment: add to paid_amount, then push any overflow into apartment credit.
        invoice.paid_amount = round(float(invoice.paid_amount or 0) + delta, 2)
        moved_to_credit = _move_invoice_overpay_to_credit(invoice)
    else:
        if debt_adjustment:
            # Additional debt / manual adjustment: do not pull from apartment credit first.
            invoice.paid_amount = round(float(invoice.paid_amount or 0) + delta, 2)
        else:
            # Correction / reversal: reduce paid_amount.
            # Strategy: first try to recover from apartment credit (so the credit
            # pool shrinks rather than paid_amount going negative), then reduce
            # paid_amount for whatever remains.
            need = -delta  # positive amount we need to subtract
            credit = float(apt.credit_balance or 0)
            take_credit = min(credit, need)
            if take_credit > 0:
                apt.credit_balance = round(credit - take_credit, 2)
                removed_from_credit = round(take_credit, 2)
                need -= take_credit

            if need > 0:
                invoice.paid_amount = max(0.0, round(float(invoice.paid_amount or 0) - need, 2))

        invoice.status = "odenilib" if float(invoice.paid_amount or 0) >= float(invoice.amount or 0) else "gozlemede"

    return {
        "moved_to_credit": round(float(moved_to_credit or 0), 2),
        "removed_from_credit": round(float(removed_from_credit or 0), 2),
    }


def save_uploaded_image(file_storage):
    if not file_storage or not file_storage.filename:
        return None

    allowed_ext = {"jpg", "jpeg", "png", "webp", "gif"}
    ext = file_storage.filename.rsplit(".", 1)[-1].lower() if "." in file_storage.filename else ""
    if ext not in allowed_ext:
        return None

    uploads_dir = Path(app.root_path) / app.config["UPLOAD_FOLDER"] / "worklog"
    uploads_dir.mkdir(parents=True, exist_ok=True)
    original_name = secure_filename(file_storage.filename)
    unique_name = f"{uuid.uuid4().hex}_{original_name}"
    target_path = uploads_dir / unique_name
    file_storage.save(target_path)
    return f"/static/uploads/worklog/{unique_name}"


def ensure_poll_schema():
    # Lightweight SQLite migration for already-created DB.
    with db.engine.connect() as conn:
        columns = {row[1] for row in conn.exec_driver_sql("PRAGMA table_info(poll)")}
        if "result_visibility" not in columns:
            conn.exec_driver_sql("ALTER TABLE poll ADD COLUMN result_visibility VARCHAR(20) DEFAULT 'immediate' NOT NULL")


def ensure_payment_schema():
    # Lightweight SQLite migration for already-created DB.
    with db.engine.connect() as conn:
        columns = {row[1] for row in conn.exec_driver_sql("PRAGMA table_info(payment)")}
        if "status" not in columns:
            conn.exec_driver_sql("ALTER TABLE payment ADD COLUMN status VARCHAR(20) DEFAULT 'confirmed' NOT NULL")
        if "reviewer_user_id" not in columns:
            conn.exec_driver_sql("ALTER TABLE payment ADD COLUMN reviewer_user_id INTEGER")
        if "reviewed_at" not in columns:
            conn.exec_driver_sql("ALTER TABLE payment ADD COLUMN reviewed_at DATETIME")
        if "comment" not in columns:
            conn.exec_driver_sql("ALTER TABLE payment ADD COLUMN comment VARCHAR(255)")


def ensure_apartment_schema():
    with db.engine.connect() as conn:
        columns = {row[1] for row in conn.exec_driver_sql("PRAGMA table_info(apartment)")}
        if "credit_balance" not in columns:
            conn.exec_driver_sql("ALTER TABLE apartment ADD COLUMN credit_balance NUMERIC NOT NULL DEFAULT 0")
        if "rooms" not in columns:
            conn.exec_driver_sql("ALTER TABLE apartment ADD COLUMN rooms INTEGER")


def ensure_apartment_preset_schema():
    with db.engine.connect() as conn:
        tables = {row[0] for row in conn.exec_driver_sql("SELECT name FROM sqlite_master WHERE type='table'")}
        if "apartment_preset" not in tables:
            db.create_all()


def ensure_building_schema():
    """Create building table if missing and add building_id column to apartment."""
    inspector = sa_inspect(db.engine)
    if not inspector.has_table("building"):
        db.create_all()
    # Add building_id to apartment if not present (for existing DBs)
    with db.engine.connect() as conn:
        apartment_cols = {row[1] for row in conn.exec_driver_sql("PRAGMA table_info(apartment)")}
        if "building_id" not in apartment_cols:
            conn.exec_driver_sql("ALTER TABLE apartment ADD COLUMN building_id INTEGER REFERENCES building(id)")


def ensure_money_numeric_schema():
    """
    SQLite migration: rebuild tables so monetary fields use NUMERIC instead of FLOAT/REAL.
    Existing values are preserved via CAST(... AS NUMERIC).
    """
    with db.engine.begin() as conn:
        dialect = conn.dialect.name
        if dialect != "sqlite":
            return

        table_defs = {
            "apartment": {
                "money_columns": {"credit_balance"},
                "create_sql": """
                    CREATE TABLE apartment_new (
                        id INTEGER NOT NULL PRIMARY KEY,
                        number VARCHAR(20) NOT NULL UNIQUE,
                        floor INTEGER NOT NULL,
                        area FLOAT NOT NULL,
                        owner_user_id INTEGER NOT NULL,
                        credit_balance NUMERIC(12,2) NOT NULL DEFAULT 0,
                        FOREIGN KEY(owner_user_id) REFERENCES user (id)
                    )
                """,
                "copy_sql": """
                    INSERT INTO apartment_new (id, number, floor, area, owner_user_id, credit_balance)
                    SELECT id, number, floor, area, owner_user_id, CAST(credit_balance AS NUMERIC)
                    FROM apartment
                """,
            },
            "tariff": {
                "money_columns": {"amount"},
                "create_sql": """
                    CREATE TABLE tariff_new (
                        id INTEGER NOT NULL PRIMARY KEY,
                        name VARCHAR(120) NOT NULL,
                        type VARCHAR(20) NOT NULL,
                        amount NUMERIC(12,2) NOT NULL,
                        is_active BOOLEAN NOT NULL
                    )
                """,
                "copy_sql": """
                    INSERT INTO tariff_new (id, name, type, amount, is_active)
                    SELECT id, name, type, CAST(amount AS NUMERIC), is_active
                    FROM tariff
                """,
            },
            "invoice": {
                "money_columns": {"amount", "paid_amount"},
                "create_sql": """
                    CREATE TABLE invoice_new (
                        id INTEGER NOT NULL PRIMARY KEY,
                        apartment_id INTEGER NOT NULL,
                        period VARCHAR(7) NOT NULL,
                        amount NUMERIC(12,2) NOT NULL,
                        paid_amount NUMERIC(12,2) NOT NULL DEFAULT 0,
                        status VARCHAR(20) NOT NULL,
                        created_at DATETIME,
                        FOREIGN KEY(apartment_id) REFERENCES apartment (id)
                    )
                """,
                "copy_sql": """
                    INSERT INTO invoice_new (id, apartment_id, period, amount, paid_amount, status, created_at)
                    SELECT id, apartment_id, period, CAST(amount AS NUMERIC), CAST(paid_amount AS NUMERIC), status, created_at
                    FROM invoice
                """,
            },
            "payment": {
                "money_columns": {"amount"},
                "create_sql": """
                    CREATE TABLE payment_new (
                        id INTEGER NOT NULL PRIMARY KEY,
                        invoice_id INTEGER NOT NULL,
                        amount NUMERIC(12,2) NOT NULL,
                        comment VARCHAR(255),
                        status VARCHAR(20) NOT NULL DEFAULT 'pending',
                        reviewer_user_id INTEGER,
                        reviewed_at DATETIME,
                        created_at DATETIME,
                        FOREIGN KEY(invoice_id) REFERENCES invoice (id),
                        FOREIGN KEY(reviewer_user_id) REFERENCES user (id)
                    )
                """,
                "copy_sql": """
                    INSERT INTO payment_new (id, invoice_id, amount, comment, status, reviewer_user_id, reviewed_at, created_at)
                    SELECT id, invoice_id, CAST(amount AS NUMERIC), comment, status, reviewer_user_id, reviewed_at, created_at
                    FROM payment
                """,
            },
            "expense_template": {
                "money_columns": {"default_amount"},
                "create_sql": """
                    CREATE TABLE expense_template_new (
                        id INTEGER NOT NULL PRIMARY KEY,
                        name VARCHAR(120) NOT NULL,
                        category VARCHAR(64),
                        default_amount NUMERIC(12,2) NOT NULL DEFAULT 0,
                        is_recurring BOOLEAN NOT NULL,
                        is_active BOOLEAN NOT NULL,
                        created_at DATETIME
                    )
                """,
                "copy_sql": """
                    INSERT INTO expense_template_new (id, name, category, default_amount, is_recurring, is_active, created_at)
                    SELECT id, name, category, CAST(default_amount AS NUMERIC), is_recurring, is_active, created_at
                    FROM expense_template
                """,
            },
            "expense": {
                "money_columns": {"amount"},
                "create_sql": """
                    CREATE TABLE expense_new (
                        id INTEGER NOT NULL PRIMARY KEY,
                        period VARCHAR(7) NOT NULL,
                        name VARCHAR(120) NOT NULL,
                        category VARCHAR(64),
                        amount NUMERIC(12,2) NOT NULL,
                        is_paid BOOLEAN NOT NULL DEFAULT 0,
                        paid_at DATETIME,
                        template_id INTEGER,
                        created_by_user_id INTEGER,
                        created_at DATETIME,
                        FOREIGN KEY(template_id) REFERENCES expense_template (id),
                        FOREIGN KEY(created_by_user_id) REFERENCES user (id)
                    )
                """,
                "copy_sql": """
                    INSERT INTO expense_new (id, period, name, category, amount, is_paid, paid_at, template_id, created_by_user_id, created_at)
                    SELECT id, period, name, category, CAST(amount AS NUMERIC), is_paid, paid_at, template_id, created_by_user_id, created_at
                    FROM expense
                """,
            },
            "balance_top_up": {
                "money_columns": {"amount"},
                "create_sql": """
                    CREATE TABLE balance_top_up_new (
                        id INTEGER NOT NULL PRIMARY KEY,
                        amount NUMERIC(12,2) NOT NULL,
                        comment VARCHAR(255),
                        created_by_user_id INTEGER,
                        created_at DATETIME,
                        FOREIGN KEY(created_by_user_id) REFERENCES user (id)
                    )
                """,
                "copy_sql": """
                    INSERT INTO balance_top_up_new (id, amount, comment, created_by_user_id, created_at)
                    SELECT id, CAST(amount AS NUMERIC), comment, created_by_user_id, created_at
                    FROM balance_top_up
                """,
            },
        }

        conn.exec_driver_sql("PRAGMA foreign_keys=OFF")
        try:
            tables = {row[0] for row in conn.exec_driver_sql("SELECT name FROM sqlite_master WHERE type='table'")}
            for table_name, cfg in table_defs.items():
                if table_name not in tables:
                    continue

                pragma_rows = conn.exec_driver_sql(f"PRAGMA table_info({table_name})").fetchall()
                col_type = {row[1]: str(row[2] or "").upper() for row in pragma_rows}
                needs_rebuild = any(
                    ("FLOAT" in col_type.get(col, "") or "REAL" in col_type.get(col, ""))
                    for col in cfg["money_columns"]
                )
                if not needs_rebuild:
                    continue

                conn.exec_driver_sql(cfg["create_sql"])
                conn.exec_driver_sql(cfg["copy_sql"])
                conn.exec_driver_sql(f"DROP TABLE {table_name}")
                conn.exec_driver_sql(f"ALTER TABLE {table_name}_new RENAME TO {table_name}")
        finally:
            conn.exec_driver_sql("PRAGMA foreign_keys=ON")


def ensure_system_schema():
    # Lightweight SQLite migration for already-created DB.
    with db.engine.connect() as conn:
        columns = {row[1] for row in conn.exec_driver_sql("PRAGMA table_info(smtp_config)")}
        if "system_name" not in columns:
            conn.exec_driver_sql("ALTER TABLE smtp_config ADD COLUMN system_name VARCHAR(120)")
        if "house_address" not in columns:
            conn.exec_driver_sql("ALTER TABLE smtp_config ADD COLUMN house_address VARCHAR(255)")
        if "commandant_name" not in columns:
            conn.exec_driver_sql("ALTER TABLE smtp_config ADD COLUMN commandant_name VARCHAR(120)")
        if "contact_phone" not in columns:
            conn.exec_driver_sql("ALTER TABLE smtp_config ADD COLUMN contact_phone VARCHAR(50)")


def ensure_user_role_migration():
    # Rename role value: commandant -> komendant
    with db.engine.connect() as conn:
        conn.exec_driver_sql("UPDATE user SET role='komendant' WHERE role='commandant'")


_did_role_migration = False
_did_expense_schema = False
_did_money_migration = False
_did_default_superadmin_seed = False
_did_apartment_schema_migration = False
_did_tariff_scope_schema = False
_did_building_schema = False


def ensure_default_superadmin_seed():
    # Ensure there is always a bootstrap superadmin in a fresh DB.
    if User.query.filter_by(role="superadmin").first():
        return
    db.session.add(
        User(
            full_name="Admin",
            phone="+000000",
            email="admin@emtk.itg.az",
            password_hash=generate_password_hash("admin"),
            role="superadmin",
        )
    )
    db.session.commit()


@app.before_request
def _run_role_migration_once():
    global _did_role_migration
    global _did_expense_schema
    global _did_money_migration
    global _did_default_superadmin_seed
    global _did_apartment_schema_migration
    global _did_tariff_scope_schema
    global _did_building_schema
    # Expense sütunları əvvəl (NUMERIC rebuild üçün category mövcud olsun).
    if not _did_expense_schema:
        try:
            ensure_expense_schema()
        finally:
            _did_expense_schema = True
    if not _did_money_migration:
        try:
            ensure_money_numeric_schema()
        finally:
            _did_money_migration = True
    if not _did_role_migration:
        try:
            ensure_user_role_migration()
        finally:
            _did_role_migration = True
    if not _did_apartment_schema_migration:
        try:
            ensure_apartment_schema()
            ensure_apartment_preset_schema()
        finally:
            _did_apartment_schema_migration = True
    # Runs on every worker (unlike if __name__ == "__main__"); needed for gunicorn / missing tables.
    if not _did_tariff_scope_schema:
        try:
            ensure_tariff_scope_schema()
        finally:
            _did_tariff_scope_schema = True
    if not _did_building_schema:
        try:
            ensure_building_schema()
        finally:
            _did_building_schema = True
    if _did_default_superadmin_seed:
        return
    try:
        ensure_default_superadmin_seed()
    finally:
        _did_default_superadmin_seed = True


def ensure_expense_schema():
    """Create expense tables if missing; add columns for legacy DBs (SQLite + PostgreSQL)."""
    inspector = sa_inspect(db.engine)
    if not inspector.has_table("expense_template") or not inspector.has_table("expense"):
        db.create_all()
        return

    expense_cols = {c["name"] for c in inspector.get_columns("expense")}
    et_cols = {c["name"] for c in inspector.get_columns("expense_template")}
    dialect = db.engine.dialect.name

    with db.engine.begin() as conn:
        if "is_paid" not in expense_cols:
            if dialect == "sqlite":
                conn.exec_driver_sql("ALTER TABLE expense ADD COLUMN is_paid BOOLEAN NOT NULL DEFAULT 1")
            else:
                conn.exec_driver_sql("ALTER TABLE expense ADD COLUMN is_paid BOOLEAN NOT NULL DEFAULT TRUE")
        if "paid_at" not in expense_cols:
            paid_type = "TIMESTAMP" if dialect == "postgresql" else "DATETIME"
            conn.exec_driver_sql(f"ALTER TABLE expense ADD COLUMN paid_at {paid_type}")

        # Legacy cleanup (SQLite uses 0/1; PostgreSQL uses TRUE/FALSE).
        if dialect == "sqlite":
            conn.exec_driver_sql("UPDATE expense SET is_paid=0 WHERE is_paid IS NULL")
        else:
            conn.exec_driver_sql("UPDATE expense SET is_paid = FALSE WHERE is_paid IS NULL")

        if "category" not in et_cols:
            conn.exec_driver_sql("ALTER TABLE expense_template ADD COLUMN category VARCHAR(64)")
        if "category" not in expense_cols:
            conn.exec_driver_sql("ALTER TABLE expense ADD COLUMN category VARCHAR(64)")


def ensure_balance_schema():
    inspector = sa_inspect(db.engine)
    if not inspector.has_table("balance_top_up"):
        db.create_all()


def ensure_tariff_scope_schema():
    # Use Inspector so this works on SQLite and PostgreSQL (not only sqlite_master).
    inspector = sa_inspect(db.engine)
    if not inspector.has_table("tariff_apartment"):
        db.create_all()


def compute_invoice_amount(apartment, active_tariffs, scope_map):
    total = 0.0
    for t in active_tariffs:
        scope = scope_map.get(t.id)
        if scope is not None and apartment.id not in scope:
            continue
        tariff_amount = float(t.amount or 0)
        total += tariff_amount * float(apartment.area or 0) if t.type == "per_m2" else tariff_amount
    return round(total, 2)


def get_smtp_config():
    cfg = SmtpConfig.query.first()
    if not cfg:
        cfg = SmtpConfig()
        db.session.add(cfg)
        db.session.commit()
    return cfg


def send_email(subject, body, recipients, html_body=None):
    cfg = get_smtp_config()
    if not cfg.host or not cfg.sender_email or not recipients:
        return False
    try:
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = cfg.sender_email
        msg["To"] = ", ".join(recipients)
        msg.set_content(body)
        if html_body:
            msg.add_alternative(html_body, subtype="html")

        with smtplib.SMTP(cfg.host, cfg.port, timeout=20) as server:
            if cfg.use_tls:
                server.starttls()
            if cfg.username:
                server.login(cfg.username, cfg.password or "")
            server.send_message(msg)
        return True
    except Exception:
        return False


def notify_residents(subject, body):
    recipients = [u.email for u in User.query.filter_by(role="resident").all() if u.email]
    return send_email(subject, body, recipients)


def build_invoice_email(invoice, resident, cfg):
    system_name = (cfg.system_name or "").strip() or "eMTK"
    house_address = (cfg.house_address or "").strip()
    commandant_line = f"{cfg.commandant_name}".strip() if cfg.commandant_name else ""
    phone_line = f"{cfg.contact_phone}".strip() if cfg.contact_phone else ""
    header = f"{system_name}"
    if house_address:
        header += f" | {house_address}"
    subject = f"{system_name} - Hesab-faktura {invoice.period} - {invoice.apartment.number}"
    debt_raw = round(invoice.amount - invoice.paid_amount, 2)
    debt = max(0.0, debt_raw)
    credit = max(0.0, -debt_raw)
    body_lines = [
        header,
        "-" * len(header),
        f"Sakin: {resident.full_name}",
        f"Menzil: {invoice.apartment.number}",
        f"Period: {invoice.period}",
        "",
        f"Hesablanıb: {invoice.amount:.2f} AZN",
        f"Ödənilib: {invoice.paid_amount:.2f} AZN",
        f"Borc: {debt:.2f} AZN",
        f"Kredit: {credit:.2f} AZN",
        f"Status: {invoice.status}",
    ]
    if commandant_line or phone_line:
        body_lines += ["", "Elaqe:"]
        if commandant_line:
            body_lines.append(f"Komendant: {commandant_line}")
        if phone_line:
            body_lines.append(f"Telefon: {phone_line}")
    plain_body = "\n".join(body_lines) + "\n"
    html_body = render_template(
        "invoice_email.html",
        invoice=invoice,
        resident=resident,
        cfg=cfg,
        system_name=system_name,
        issue_date=utc_to_local(invoice.created_at),
    )
    return subject, plain_body, html_body


def build_receipt_email(payment, cfg):
    invoice = payment.invoice
    resident = invoice.apartment.owner
    system_name = (cfg.system_name or "").strip() or "eMTK"
    subject = f"{system_name} - Ödəniş qəbzi #{payment.id} - {invoice.apartment.number}"
    balance = float(invoice.paid_amount or 0) - float(invoice.amount or 0)
    plain_body = (
        f"{system_name}\n"
        f"Ödəniş qəbzi #{payment.id}\n"
        f"Sakin: {resident.full_name}\n"
        f"Mənzil: {invoice.apartment.number}\n"
        f"Period: {invoice.period}\n"
        f"Ödəniş: {float(payment.amount):.2f} AZN\n"
        f"Hesablanıb: {float(invoice.amount):.2f} AZN\n"
        f"Ödənilib: {float(invoice.paid_amount):.2f} AZN\n"
        f"Balans: {balance:.2f} AZN\n"
        f"Tarix: {(payment.created_at or datetime.now(timezone.utc)).strftime('%d.%m.%Y %H:%M')}\n"
    )
    html_body = render_template("receipt_email.html", payment=payment, cfg=cfg, system_name=system_name)
    return subject, plain_body, html_body


@app.context_processor
def inject_system_config():
    cfg = get_smtp_config()
    system_name = (cfg.system_name or "").strip() or "eMTK"
    return {"system_name": system_name}


def payment_status_label(status: str) -> str:
    return {
        "pending": "Gözləmədə",
        "confirmed": "Təsdiqlənib",
        "rejected": "İmtina edilib",
    }.get(status or "", status or "")


def payment_status_badge(status: str) -> str:
    return {
        "pending": "warning",
        "confirmed": "success",
        "rejected": "danger",
    }.get(status or "", "secondary")


@app.context_processor
def inject_helpers():
    return {
        "payment_status_label": payment_status_label,
        "payment_status_badge": payment_status_badge,
    }


def get_selected_apartment(user):
    apartments = Apartment.query.filter_by(owner_user_id=user.id).order_by(Apartment.number).all()
    if not apartments:
        return None, []
    selected_id = session.get("selected_apartment_id")
    selected = next((a for a in apartments if a.id == selected_id), apartments[0])
    session["selected_apartment_id"] = selected.id
    return selected, apartments


def normalize_az_phone(phone_raw: str):
    phone = (phone_raw or "").strip()
    if not phone:
        return None
    return phone if re.fullmatch(r"\+994\d{9}", phone) else None


def _parse_int_field(raw_value, *, min_value: int, max_value: int):
    raw = (raw_value or "").strip()
    if not raw:
        return None
    value = int(raw)
    if value < min_value or value > max_value:
        raise ValueError
    return value


def parse_login_identifier(raw: str):
    """Classify login input: email (contains @) or Azerbaijani phone (+994…)."""
    s = (raw or "").strip()
    if not s:
        return None, None
    if "@" in s:
        return "email", s.lower()
    phone = normalize_az_phone(s)
    if phone:
        return "phone", phone
    return None, None


def current_user():
    user_id = session.get("user_id")
    if not user_id:
        return None
    return db.session.get(User, user_id)


def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not current_user():
            return redirect(url_for("login"))
        return f(*args, **kwargs)

    return wrapper


def role_required(*roles):
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            user = current_user()
            if not user or user.role not in roles:
                flash("Bu emeliyyat ucun icazeniz yoxdur.", "danger")
                return redirect(url_for("dashboard"))
            return f(*args, **kwargs)

        return wrapper

    return decorator


@app.route("/")
def root():
    if current_user():
        return redirect(url_for("dashboard"))
    return redirect(url_for("login"))


@app.route("/login", methods=["GET", "POST"])
@limiter.limit("10 per minute")
def login():
    if request.method == "POST":
        raw = (request.form.get("login") or request.form.get("email") or "").strip()
        password = request.form["password"]
        kind, value = parse_login_identifier(raw)
        if kind is None:
            if raw and "@" not in raw:
                flash("Telefon formatı +994XXXXXXXXX olmalıdır.", "danger")
            else:
                flash("Telefon və ya email daxil edin.", "danger")
            return render_template("login.html")
        if kind == "email":
            user = User.query.filter_by(email=value).first()
        else:
            user = User.query.filter_by(phone=value).first()
        if user and check_password_hash(user.password_hash, password):
            session.clear()
            session["user_id"] = user.id
            session["role"] = user.role
            session.pop("selected_apartment_id", None)
            return redirect(url_for("dashboard"))
        flash("Telefon/email və ya şifrə yanlışdır.", "danger")
    return render_template("login.html")


@app.route("/register", methods=["GET", "POST"])
@limiter.limit("5 per minute")
def register():
    if request.method == "POST":
        full_name = request.form["full_name"].strip()
        phone = request.form.get("phone", "").strip()
        email = request.form["email"].strip().lower()
        password = request.form["password"]

        if phone and not normalize_az_phone(phone):
            flash("Telefon formatı +994XXXXXXXXX olmalıdır.", "danger")
            return redirect(url_for("register"))

        if User.query.filter_by(email=email).first():
            flash("Bu email ile qeydiyyatdan kecmis istifadeci var.", "warning")
            return redirect(url_for("register"))

        resident = User(
            full_name=full_name,
            phone=phone,
            email=email,
            password_hash=generate_password_hash(password),
            role="resident",
        )
        db.session.add(resident)
        db.session.commit()
        flash("Qeydiyyat tamamlandi. Menzil komendant ve ya administrator terefinden teyin edilir.", "success")
        return redirect(url_for("login"))

    return render_template("register.html")


@app.route("/logout", methods=["POST"])
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/change-password", methods=["GET", "POST"])
@login_required
def change_password():
    user = current_user()
    if request.method == "POST":
        current_pw = request.form.get("current_password", "")
        new_pw = request.form.get("new_password", "")
        confirm_pw = request.form.get("confirm_password", "")
        if not check_password_hash(user.password_hash, current_pw):
            flash("Mövcud şifrə yanlışdır.", "danger")
            return render_template("change_password.html")
        if len(new_pw) < 6:
            flash("Yeni şifrə ən az 6 simvol olmalıdır.", "danger")
            return render_template("change_password.html")
        if new_pw != confirm_pw:
            flash("Yeni şifrələr uyğun gəlmir.", "danger")
            return render_template("change_password.html")
        user.password_hash = generate_password_hash(new_pw)
        db.session.commit()
        audit("Şifrə dəyişdirildi")
        flash("Şifrə uğurla dəyişdirildi.", "success")
        return redirect(url_for("dashboard"))
    return render_template("change_password.html")


@app.route("/dashboard")
@login_required
def dashboard():
    user = current_user()
    if user.role == "resident":
        apartment, apartments = get_selected_apartment(user)
        invoices = Invoice.query.filter_by(apartment_id=apartment.id).order_by(Invoice.created_at.desc()).all() if apartment else []

        base_debt = sum(float(i.amount) - float(i.paid_amount) for i in invoices)
        credit_balance = float(apartment.credit_balance or 0) if apartment else 0.0
        debt = round(base_debt - credit_balance, 2)

        # Building-wide metrics (read-only for residents).
        # Use the same db.case guard as the admin dashboard to avoid negative
        # per-invoice contributions in the SQL aggregate (consistent formula).
        debt_expr = db.case((Invoice.amount - Invoice.paid_amount > 0, Invoice.amount - Invoice.paid_amount), else_=0.0)
        house_total_debt = db.session.query(db.func.sum(debt_expr)).scalar() or 0
        house_credit_total = db.session.query(db.func.sum(Apartment.credit_balance)).scalar() or 0
        house_total_debt = float(house_total_debt or 0) - float(house_credit_total or 0)
        income_total = (
            db.session.query(db.func.sum(Payment.amount)).filter(Payment.status == "confirmed").scalar() or 0
        )
        topup_total = db.session.query(db.func.sum(BalanceTopUp.amount)).scalar() or 0
        paid_expenses_total = (
            db.session.query(db.func.sum(Expense.amount)).filter(Expense.is_paid == True).scalar() or 0
        )
        unpaid_expenses = db.session.query(db.func.sum(Expense.amount)).filter(Expense.is_paid == False).scalar() or 0
        house_balance = round(float(income_total) + float(topup_total) - float(paid_expenses_total), 2)
        recent_expenses = Expense.query.order_by(Expense.created_at.desc()).limit(100).all()
        receipt_by_invoice = {}
        for i in invoices:
            confirmed = sorted([p for p in i.payments if p.status == "confirmed"], key=lambda x: x.created_at, reverse=True)
            receipt_by_invoice[i.id] = confirmed[0].id if confirmed else None
        payment_history = []
        if apartment:
            payment_history = (
                Payment.query.join(Invoice, Payment.invoice_id == Invoice.id)
                .filter(Invoice.apartment_id == apartment.id)
                .order_by(Payment.created_at.desc())
                .limit(100)
                .all()
            )
        works = WorkLog.query.order_by(WorkLog.created_at.desc()).limit(5).all()
        return render_template(
            "resident_dashboard.html",
            apartment=apartment,
            apartments=apartments,
            invoices=invoices,
            debt=round(debt, 2),
            credit_balance=round(credit_balance, 2),
            house_total_debt=round(float(house_total_debt or 0), 2),
            house_balance=house_balance,
            unpaid_expenses=unpaid_expenses,
            recent_expenses=recent_expenses,
            receipt_by_invoice=receipt_by_invoice,
            works=works,
            payment_history=payment_history,
        )

    from_date = request.args.get("from_date")
    to_date = request.args.get("to_date")
    from_dt = datetime.strptime(from_date, "%Y-%m-%d") if from_date else datetime(date.today().year, date.today().month, 1)
    to_dt = datetime.strptime(to_date, "%Y-%m-%d") if to_date else datetime.now(timezone.utc)
    apartments_count = Apartment.query.count()
    # Total debt should not be reduced by overpayments (credit) inside invoices,
    # but should be reduced by apartment credit balances.
    debt_expr = db.case((Invoice.amount - Invoice.paid_amount > 0, Invoice.amount - Invoice.paid_amount), else_=0.0)
    debt = db.session.query(db.func.sum(debt_expr)).scalar() or 0
    house_credit_total = db.session.query(db.func.sum(Apartment.credit_balance)).scalar() or 0
    debt = float(debt or 0) - float(house_credit_total or 0)
    pending_invoices = Invoice.query.filter(Invoice.status != "odenilib").count()
    recent_logs = AuditLog.query.order_by(AuditLog.created_at.desc()).limit(10).all()
    period_payments = Payment.query.filter(
        Payment.status == "confirmed", Payment.created_at >= from_dt, Payment.created_at <= to_dt
    ).order_by(Payment.created_at.asc()).all()
    payment_table = []
    for p in period_payments:
        payment_table.append(
            {
                "apartment": p.invoice.apartment.number,
                "period": p.invoice.period,
                "amount": p.amount,
                "date": p.created_at.strftime("%d.%m.%Y"),
            }
        )
    payments_by_day = {}
    for p in period_payments:
        key = p.created_at.strftime("%Y-%m-%d")
        payments_by_day[key] = payments_by_day.get(key, 0) + float(p.amount)

    period_topups = BalanceTopUp.query.filter(BalanceTopUp.created_at >= from_dt, BalanceTopUp.created_at <= to_dt).order_by(BalanceTopUp.created_at.asc()).all()
    topups_by_day = {}
    for t in period_topups:
        key = t.created_at.strftime("%Y-%m-%d")
        topups_by_day[key] = topups_by_day.get(key, 0) + float(t.amount)
    for d, v in topups_by_day.items():
        payments_by_day[d] = payments_by_day.get(d, 0) + float(v)

    period_expenses = (
        Expense.query.filter(
            Expense.created_at >= from_dt, Expense.created_at <= to_dt, Expense.is_paid == True
        )
        .order_by(Expense.created_at.asc())
        .all()
    )
    expenses_by_day = {}
    for e in period_expenses:
        key = e.created_at.strftime("%Y-%m-%d")
        expenses_by_day[key] = expenses_by_day.get(key, 0) + float(e.amount)

    chart_labels = sorted(set(payments_by_day.keys()) | set(expenses_by_day.keys()))
    chart_payments = []
    chart_expenses = []
    chart_balance = []
    running = 0.0
    for d in chart_labels:
        income = float(payments_by_day.get(d, 0.0))
        out = float(expenses_by_day.get(d, 0.0))
        running = round(running + income - out, 2)
        chart_payments.append(round(income, 2))
        chart_expenses.append(round(out, 2))
        chart_balance.append(running)
    # Debt by apartment with credit applied.
    debt_rows = (
        db.session.query(Apartment.id, Apartment.number, db.func.sum(debt_expr), Apartment.credit_balance)
        .join(Invoice, Invoice.apartment_id == Apartment.id)
        .group_by(Apartment.id)
        .all()
    )
    debt_by_apartment = []
    for apt_id, apt_no, inv_debt_sum, credit_bal in debt_rows:
        v = float(inv_debt_sum or 0) - float(credit_bal or 0)
        debt_by_apartment.append((apt_no, round(v, 2)))
    debt_by_apartment.sort(key=lambda x: x[0])

    income_total = db.session.query(db.func.sum(Payment.amount)).filter(Payment.status == "confirmed").scalar() or 0
    topup_total = db.session.query(db.func.sum(BalanceTopUp.amount)).scalar() or 0
    paid_expenses_total = (
        db.session.query(db.func.sum(Expense.amount)).filter(Expense.is_paid == True).scalar() or 0
    )
    unpaid_expenses_total = (
        db.session.query(db.func.sum(Expense.amount)).filter(Expense.is_paid == False).scalar() or 0
    )
    apartment_payments_period = (
        db.session.query(db.func.sum(Payment.amount))
        .filter(Payment.status == "confirmed", Payment.created_at >= from_dt, Payment.created_at <= to_dt)
        .scalar()
        or 0
    )
    expenses_period = (
        db.session.query(db.func.sum(Expense.amount))
        .filter(Expense.is_paid == True, Expense.created_at >= from_dt, Expense.created_at <= to_dt)
        .scalar()
        or 0
    )
    house_balance = round(float(income_total) + float(topup_total) - float(paid_expenses_total), 2)

    return render_template(
        "admin_dashboard.html",
        apartments_count=apartments_count,
        total_debt=round(debt, 2),
        house_balance=house_balance,
        apartment_payments_period=round(float(apartment_payments_period or 0), 2),
        expenses_period=round(float(expenses_period or 0), 2),
        paid_expenses_total=round(float(paid_expenses_total or 0), 2),
        unpaid_expenses_total=round(float(unpaid_expenses_total or 0), 2),
        pending_invoices=pending_invoices,
        recent_logs=recent_logs,
        from_date=from_dt.strftime("%Y-%m-%d"),
        to_date=to_dt.strftime("%Y-%m-%d"),
        payment_table=payment_table,
        chart_labels=chart_labels,
        chart_payments=chart_payments,
        chart_expenses=chart_expenses,
        chart_balance=chart_balance,
        debt_by_apartment=debt_by_apartment,
    )


@app.route("/admin/balance/topup", methods=["POST"])
@login_required
@role_required("komendant", "superadmin")
def balance_topup():
    try:
        amount = float(request.form.get("amount", "0") or 0)
    except (TypeError, ValueError):
        flash("Məbləğ düzgün deyil.", "danger")
        return redirect(url_for("dashboard"))
    if amount <= 0:
        flash("Məbləğ sıfırdan böyük olmalıdır.", "danger")
        return redirect(url_for("dashboard"))
    comment = (request.form.get("comment", "") or "").strip() or None
    db.session.add(BalanceTopUp(amount=round(amount, 2), comment=comment, created_by_user_id=current_user().id))
    db.session.commit()
    audit(f"Balans artirildi {amount:.2f} AZN" + (f" ({comment})" if comment else ""))
    flash("Balans artirildi.", "success")
    return redirect(url_for("dashboard"))


@app.route("/resident/select-apartment", methods=["POST"])
@login_required
@role_required("resident")
def select_apartment():
    user = current_user()
    apartment_id = int(request.form["apartment_id"])
    owned = Apartment.query.filter_by(owner_user_id=user.id, id=apartment_id).first()
    if owned:
        session["selected_apartment_id"] = owned.id
    return redirect(url_for("dashboard"))


@app.route("/admin/apartments", methods=["GET", "POST"])
@login_required
@role_required("komendant", "superadmin")
def admin_apartments():
    if request.method == "POST":
        number = request.form["number"].strip()
        if not number or len(number) > 4:
            flash("Nömrə 1-4 simvol olmalıdır.", "danger")
            return redirect(url_for("admin_apartments"))
        try:
            floor = _parse_int_field(request.form.get("floor"), min_value=-999, max_value=999)
        except ValueError:
            flash("Mərtəbə -999 ilə 999 aralığında olmalıdır.", "danger")
            return redirect(url_for("admin_apartments"))
        preset_id_raw = (request.form.get("preset_id", "") or "").strip()
        preset = ApartmentPreset.query.get(int(preset_id_raw)) if preset_id_raw.isdigit() else None
        rooms_raw = (request.form.get("rooms", "") or "").strip()
        try:
            rooms = _parse_int_field(rooms_raw, min_value=1, max_value=999) if rooms_raw else None
        except ValueError:
            flash("Otaq sayı 1-999 aralığında olmalıdır.", "danger")
            return redirect(url_for("admin_apartments"))
        area_raw = (request.form.get("area", "") or "").strip()
        try:
            area = _parse_int_field(area_raw, min_value=1, max_value=9999) if area_raw else None
        except ValueError:
            flash("Sahə 1-9999 aralığında tam ədəd olmalıdır.", "danger")
            return redirect(url_for("admin_apartments"))
        if preset:
            rooms = int(preset.rooms)
            area = int(preset.area)
        if area is None:
            flash("Sahə daxil edilməlidir.", "danger")
            return redirect(url_for("admin_apartments"))
        if floor is None:
            flash("Mərtəbə daxil edilməlidir.", "danger")
            return redirect(url_for("admin_apartments"))
        owner_user_id = int(request.form["owner_user_id"])
        building_id_raw = (request.form.get("building_id", "") or "").strip()
        building_id = int(building_id_raw) if building_id_raw.isdigit() else None
        db.session.add(Apartment(number=number, floor=floor, rooms=rooms, area=area, owner_user_id=owner_user_id, building_id=building_id))
        db.session.commit()
        audit(f"Menzil yaradildi {number}")
        flash("Menzil elave edildi.", "success")
        return redirect(url_for("admin_apartments"))

    building_filter_raw = (request.args.get("building_id", "") or "").strip()
    building_filter_id = int(building_filter_raw) if building_filter_raw.isdigit() else None

    apartments_query = Apartment.query
    if building_filter_id:
        apartments_query = apartments_query.filter(Apartment.building_id == building_filter_id)
    apartments = apartments_query.order_by(Apartment.number).all()
    residents = User.query.filter_by(role="resident").all()
    apartment_presets = ApartmentPreset.query.order_by(ApartmentPreset.rooms.asc(), ApartmentPreset.area.asc()).all()
    buildings = Building.query.order_by(Building.name.asc()).all()
    debt_expr = db.case((Invoice.amount - Invoice.paid_amount > 0, Invoice.amount - Invoice.paid_amount), else_=0.0)
    debt_rows = (
        db.session.query(Invoice.apartment_id, db.func.sum(debt_expr))
        .group_by(Invoice.apartment_id)
        .all()
    )
    inv_balance_by_apartment_id = {apt_id: float(total or 0) for apt_id, total in debt_rows}
    debt_by_apartment_id = {}
    for a in apartments:
        inv_bal = float(inv_balance_by_apartment_id.get(a.id, 0) or 0)
        credit = float(a.credit_balance or 0)
        debt_by_apartment_id[a.id] = round(inv_bal - credit, 2)
    return render_template(
        "admin_apartments.html",
        apartments=apartments,
        residents=residents,
        apartment_presets=apartment_presets,
        debt_by_apartment_id=debt_by_apartment_id,
        buildings=buildings,
        building_filter_id=building_filter_id,
    )


@app.route("/admin/apartments/delete/<int:apartment_id>", methods=["POST"])
@login_required
@role_required("komendant", "superadmin")
def delete_apartment(apartment_id):
    apartment = Apartment.query.get_or_404(apartment_id)
    # SQLAlchemy 2.x: avoid legacy Query.count() (can raise in some setups); use EXISTS + scalar().
    has_invoices = bool(
        db.session.scalar(select(exists().where(Invoice.apartment_id == apartment.id)))
    )
    if has_invoices:
        flash("Menzili silmek olmur: bagli hesab var.", "warning")
        return redirect(url_for("admin_apartments"))

    apartment_number = apartment.number
    apt_pk = apartment.id
    # Core DELETEs: avoid ORM session.delete(apartment), which can touch relationships / flush order.
    try:
        # Polls are deprecated; cleanup orphan votes before apartment delete.
        db.session.execute(sa_delete(Vote).where(Vote.apartment_id == apt_pk))
        db.session.execute(sa_delete(TariffApartment).where(TariffApartment.apartment_id == apt_pk))
        db.session.execute(sa_delete(Apartment).where(Apartment.id == apt_pk))
        db.session.commit()
    except IntegrityError:
        db.session.rollback()
        app.logger.exception("delete_apartment integrity error apartment_id=%s", apartment_id)
        flash("Menzil silinmədi: bağlı qeydlər var (məsələn, hesab).", "danger")
        return redirect(url_for("admin_apartments"))
    except SQLAlchemyError:
        db.session.rollback()
        app.logger.exception("delete_apartment database error apartment_id=%s", apartment_id)
        flash("Menzil silinmədi: verilənlər bazası xətası.", "danger")
        return redirect(url_for("admin_apartments"))
    except Exception:
        db.session.rollback()
        app.logger.exception("delete_apartment unexpected error apartment_id=%s", apartment_id)
        flash("Menzil silinmədi: gözlənilməz xəta.", "danger")
        return redirect(url_for("admin_apartments"))

    try:
        audit(f"Menzil silindi {apartment_number}")
    except Exception:
        # Delete already committed; avoid 500 if audit log insert fails.
        app.logger.exception("audit after delete_apartment failed apartment_id=%s", apartment_id)
    flash("Menzil silindi.", "success")
    return redirect(url_for("admin_apartments"))


@app.route("/admin/apartments/update/<int:apartment_id>", methods=["POST"])
@login_required
@role_required("komendant", "superadmin")
def update_apartment(apartment_id):
    apartment = Apartment.query.get_or_404(apartment_id)
    number = request.form["number"].strip()
    if not number or len(number) > 4:
        flash("Nömrə 1-4 simvol olmalıdır.", "danger")
        return redirect(url_for("admin_apartments"))
    try:
        floor = _parse_int_field(request.form.get("floor"), min_value=-999, max_value=999)
    except ValueError:
        flash("Mərtəbə -999 ilə 999 aralığında olmalıdır.", "danger")
        return redirect(url_for("admin_apartments"))
    rooms_raw = (request.form.get("rooms", "") or "").strip()
    try:
        rooms = _parse_int_field(rooms_raw, min_value=1, max_value=999) if rooms_raw else None
    except ValueError:
        flash("Otaq sayı 1-999 aralığında olmalıdır.", "danger")
        return redirect(url_for("admin_apartments"))
    try:
        area = _parse_int_field(request.form.get("area"), min_value=1, max_value=9999)
    except ValueError:
        flash("Sahə 1-9999 aralığında tam ədəd olmalıdır.", "danger")
        return redirect(url_for("admin_apartments"))
    if area is None:
        flash("Sahə daxil edilməlidir.", "danger")
        return redirect(url_for("admin_apartments"))
    owner_user_id = int(request.form["owner_user_id"])

    duplicate = Apartment.query.filter(Apartment.number == number, Apartment.id != apartment.id).first()
    if duplicate:
        flash("Bu nomre ile menzil artiq movcuddur.", "warning")
        return redirect(url_for("admin_apartments"))

    building_id_raw = (request.form.get("building_id", "") or "").strip()
    building_id = int(building_id_raw) if building_id_raw.isdigit() else None

    apartment.number = number
    apartment.floor = floor
    apartment.rooms = rooms
    apartment.area = area
    apartment.owner_user_id = owner_user_id
    apartment.building_id = building_id
    db.session.commit()
    audit(f"Menzil yenilendi {number}")
    flash("Menzil yenilendi.", "success")
    return redirect(url_for("admin_apartments"))


@app.route("/admin/tariffs", methods=["GET", "POST"])
@login_required
@role_required("komendant", "superadmin")
def admin_tariffs():
    if request.method == "POST":
        tariff = Tariff(
            name=request.form["name"].strip(),
            type=request.form["type"],
            amount=float(request.form["amount"]),
            is_active=True,
        )
        db.session.add(tariff)
        db.session.commit()

        apply_all = request.form.get("apply_all") == "on"
        if not apply_all:
            apartment_ids = request.form.getlist("apartment_ids")
            for apt_id in apartment_ids:
                db.session.add(TariffApartment(tariff_id=tariff.id, apartment_id=int(apt_id)))
            db.session.commit()

        audit("Tarif elave edildi")
        flash("Tarif elave edildi.", "success")
        return redirect(url_for("admin_tariffs"))

    tariffs = Tariff.query.order_by(Tariff.id.desc()).all()
    apartments = Apartment.query.order_by(Apartment.number).all()
    scope_rows = TariffApartment.query.all()
    scope_map = {}
    for r in scope_rows:
        scope_map.setdefault(r.tariff_id, set()).add(r.apartment_id)
    return render_template("admin_tariffs.html", tariffs=tariffs, apartments=apartments, scope_map=scope_map)


@app.route("/admin/tariffs/delete/<int:tariff_id>", methods=["POST"])
@login_required
@role_required("komendant", "superadmin")
def delete_tariff(tariff_id):
    tariff = Tariff.query.get_or_404(tariff_id)
    tariff_name = tariff.name
    TariffApartment.query.filter_by(tariff_id=tariff.id).delete(synchronize_session=False)
    db.session.delete(tariff)
    db.session.commit()
    audit(f"Tarif silindi {tariff_name}")
    flash("Tarif silindi.", "success")
    return redirect(url_for("admin_tariffs"))


@app.route("/admin/tariffs/update/<int:tariff_id>", methods=["POST"])
@login_required
@role_required("komendant", "superadmin")
def update_tariff(tariff_id):
    tariff = Tariff.query.get_or_404(tariff_id)

    name = (request.form.get("name", "") or "").strip()
    tariff_type = (request.form.get("type", "") or "").strip()
    amount_raw = (request.form.get("amount", "") or "").strip()
    is_active = request.form.get("is_active") == "on"

    if not name:
        flash("Tarif adı boş ola bilməz.", "danger")
        return redirect(url_for("admin_tariffs"))
    if tariff_type not in ("per_m2", "fixed"):
        flash("Tarif tipi düzgün seçilməyib.", "danger")
        return redirect(url_for("admin_tariffs"))
    try:
        amount = float(amount_raw)
    except ValueError:
        flash("Məbləğ düzgün daxil edilməyib.", "danger")
        return redirect(url_for("admin_tariffs"))
    if amount <= 0:
        flash("Məbləğ sıfırdan böyük olmalıdır.", "danger")
        return redirect(url_for("admin_tariffs"))

    tariff.name = name
    tariff.type = tariff_type
    tariff.amount = amount
    tariff.is_active = is_active

    apply_all = request.form.get("apply_all") == "on"
    TariffApartment.query.filter_by(tariff_id=tariff.id).delete(synchronize_session=False)
    if not apply_all:
        apartment_ids = request.form.getlist("apartment_ids")
        for apt_id in apartment_ids:
            if apt_id.isdigit():
                db.session.add(TariffApartment(tariff_id=tariff.id, apartment_id=int(apt_id)))

    db.session.commit()
    audit(f"Tarif yenilendi {tariff.name}")
    flash("Tarif yeniləndi.", "success")
    return redirect(url_for("admin_tariffs"))


@app.route("/admin/invoices/generate", methods=["POST"])
@login_required
@role_required("komendant", "superadmin")
def generate_invoices():
    period_mode = (request.form.get("period_mode") or "current").strip().lower()
    today = date.today()
    current_period = today.strftime("%Y-%m")
    next_period = f"{(today.year + (1 if today.month == 12 else 0)):04d}-{(1 if today.month == 12 else today.month + 1):02d}"
    periods_to_generate = [current_period] if period_mode != "next" else [current_period, next_period]

    active_tariffs = Tariff.query.filter_by(is_active=True).all()
    apartments = Apartment.query.all()
    scope_rows = TariffApartment.query.all()
    scope_map = {}
    for r in scope_rows:
        scope_map.setdefault(r.tariff_id, set()).add(r.apartment_id)

    created_by_period = {}
    credit_applied_by_period = {}

    for period in periods_to_generate:
        created = 0
        for apartment in apartments:
            exists = Invoice.query.filter_by(apartment_id=apartment.id, period=period).first()
            if exists:
                continue
            total = compute_invoice_amount(apartment, active_tariffs, scope_map)
            db.session.add(Invoice(apartment_id=apartment.id, period=period, amount=total, status="gozlemede"))
            created += 1
        db.session.commit()
        created_by_period[period] = created

        # Apply apartment credit to all unpaid invoices for the period.
        unpaid_invoices = (
            Invoice.query.join(Apartment, Invoice.apartment_id == Apartment.id)
            .filter(Invoice.period == period, Invoice.status != "odenilib")
            .order_by(Apartment.number.asc(), Invoice.id.asc())
            .all()
        )
        applied_total = 0.0
        for inv in unpaid_invoices:
            applied_total += _apply_credit_to_invoice(inv)
        if applied_total > 0:
            db.session.commit()
            audit(f"Kredit avtomatik tetbiq olundu: {applied_total:.2f} AZN period {period}")
        credit_applied_by_period[period] = round(applied_total, 2)

        templates = ExpenseTemplate.query.filter_by(is_active=True, is_recurring=True).all()
        created_exp = 0
        for t in templates:
            exists = Expense.query.filter_by(period=period, template_id=t.id).first()
            if exists:
                continue
            amount = float(t.default_amount or 0)
            if amount <= 0:
                continue
            db.session.add(
                Expense(
                    period=period,
                    name=t.name,
                    category=t.category,
                    amount=round(amount, 2),
                    is_paid=False,
                    paid_at=None,
                    template_id=t.id,
                    created_by_user_id=current_user().id,
                )
            )
            created_exp += 1
        if created_exp:
            db.session.commit()
            audit(f"Aylıq xərclər yaradıldı: {created_exp} period {period}")
    if len(periods_to_generate) == 2:
        audit(
            f"Hesablar yaradildi ardicil periodlar: {periods_to_generate[0]}={created_by_period.get(periods_to_generate[0], 0)}, "
            f"{periods_to_generate[1]}={created_by_period.get(periods_to_generate[1], 0)}"
        )
        flash(
            "Hesablar yaradildi. "
            f"{periods_to_generate[0]}: {created_by_period.get(periods_to_generate[0], 0)} hesab, "
            f"{periods_to_generate[1]}: {created_by_period.get(periods_to_generate[1], 0)} hesab.",
            "success",
        )
    else:
        period = periods_to_generate[0]
        audit(f"Hesablar yaradildi: {created_by_period.get(period, 0)} period {period}")
        flash(f"Hesablar yaradildi: {created_by_period.get(period, 0)} eded.", "success")

    if any((credit_applied_by_period.get(p, 0) > 0 for p in periods_to_generate)):
        db.session.commit()
    return redirect(url_for("admin_invoices"))


@app.route("/admin/invoices/recalculate", methods=["POST"])
@login_required
@role_required("komendant", "superadmin")
def recalculate_invoices():
    period = (request.form.get("period") or "").strip()
    if not period or len(period) != 7:
        flash("Period duzgun deyil (YYYY-MM).", "danger")
        return redirect(url_for("admin_tariffs"))

    active_tariffs = Tariff.query.filter_by(is_active=True).all()
    apartments = Apartment.query.all()
    scope_rows = TariffApartment.query.all()
    scope_map = {}
    for r in scope_rows:
        scope_map.setdefault(r.tariff_id, set()).add(r.apartment_id)

    updated = 0
    created = 0
    for apartment in apartments:
        total = compute_invoice_amount(apartment, active_tariffs, scope_map)
        inv = Invoice.query.filter_by(apartment_id=apartment.id, period=period).first()
        if not inv:
            db.session.add(Invoice(apartment_id=apartment.id, period=period, amount=total, status="gozlemede"))
            created += 1
            continue
        inv.amount = total
        inv.status = "odenilib" if float(inv.paid_amount or 0) >= float(inv.amount or 0) else "gozlemede"
        overflow = _move_invoice_overpay_to_credit(inv)
        if overflow > 0:
            audit(f"Kredit yarandi (yeniden hesabla) invoice#{inv.id}: +{overflow:.2f} AZN")
        updated += 1

    db.session.commit()

    # FIX (warning #3): After recalculating amounts, apply any accumulated
    # apartment credit to unpaid invoices in this period. Previously the
    # credit was left untouched even if the new amount could be fully or
    # partially covered by it.
    unpaid_invoices = (
        Invoice.query.join(Apartment, Invoice.apartment_id == Apartment.id)
        .filter(Invoice.period == period, Invoice.status != "odenilib")
        .order_by(Apartment.number.asc(), Invoice.id.asc())
        .all()
    )
    applied_total = sum(_apply_credit_to_invoice(inv) for inv in unpaid_invoices)
    if applied_total > 0:
        db.session.commit()
        audit(f"Kredit avtomatik tetbiq olundu (yeniden hesabla): {applied_total:.2f} AZN period {period}")

    audit(f"Hesablar yeniden hesablandi period {period}: updated {updated}, created {created}")
    flash(f"Yeniden hesablandi: {updated}, yaradildi: {created}.", "success")
    return redirect(url_for("admin_invoices"))


@app.route("/admin/expenses", methods=["GET", "POST"])
@login_required
@role_required("komendant", "superadmin")
def admin_expenses():
    if request.method == "POST":
        form_type = request.form.get("form_type", "")
        if form_type == "add_template":
            name = request.form.get("name", "").strip()
            category = _parse_expense_category(request.form.get("category"))
            default_amount = float(request.form.get("default_amount", "0") or 0)
            is_recurring = request.form.get("is_recurring") == "on"
            if not name:
                flash("Ad bos ola bilmez.", "danger")
                return redirect(url_for("admin_expenses"))
            if not category:
                flash("Kateqoriya seçin.", "danger")
                return redirect(url_for("admin_expenses"))
            db.session.add(
                ExpenseTemplate(
                    name=name,
                    category=category,
                    default_amount=round(default_amount, 2),
                    is_recurring=is_recurring,
                    is_active=True,
                )
            )
            db.session.commit()
            audit(f"Xərc şablonu yaradıldı: {name}")
            flash("Xərc şablonu əlavə edildi.", "success")
            return redirect(url_for("admin_expenses"))
        if form_type == "toggle_template":
            template_id = int(request.form["template_id"])
            t = ExpenseTemplate.query.get_or_404(template_id)
            t.is_active = not t.is_active
            db.session.commit()
            audit(f"Xərc şablonu statusu dəyişdi #{template_id}")
            return redirect(url_for("admin_expenses"))
        if form_type == "update_template":
            template_id = int(request.form["template_id"])
            t = ExpenseTemplate.query.get_or_404(template_id)
            name = (request.form.get("name", "") or "").strip()
            category = _parse_expense_category(request.form.get("category"))
            default_amount = float(request.form.get("default_amount", "0") or 0)
            is_recurring = request.form.get("is_recurring") == "on"
            if not name:
                flash("Ad bos ola bilmez.", "danger")
                return redirect(url_for("admin_expenses"))
            if not category:
                flash("Kateqoriya seçin.", "danger")
                return redirect(url_for("admin_expenses"))
            if default_amount < 0:
                flash("Məbləğ mənfi ola bilməz.", "danger")
                return redirect(url_for("admin_expenses"))
            t.name = name
            t.category = category
            t.default_amount = round(default_amount, 2)
            t.is_recurring = is_recurring
            db.session.commit()
            audit(f"Xərc şablonu yeniləndi #{template_id}: {name}")
            flash("Xərc şablonu yeniləndi.", "success")
            return redirect(url_for("admin_expenses"))
        if form_type == "delete_template":
            template_id = int(request.form["template_id"])
            t = ExpenseTemplate.query.get_or_404(template_id)
            Expense.query.filter_by(template_id=t.id).update({"template_id": None}, synchronize_session=False)
            db.session.delete(t)
            db.session.commit()
            audit(f"Xərc şablonu silindi #{template_id}")
            flash("Xərc şablonu silindi.", "success")
            return redirect(url_for("admin_expenses"))
        if form_type == "add_expense":
            period = (request.form.get("period", "") or "").strip()
            name = (request.form.get("name", "") or "").strip()
            category = _parse_expense_category(request.form.get("category"))
            amount = float(request.form.get("amount", "0") or 0)
            if not period or len(period) != 7:
                flash("Period duzgun deyil (YYYY-MM).", "danger")
                return redirect(url_for("admin_expenses"))
            if not name:
                flash("Ad bos ola bilmez.", "danger")
                return redirect(url_for("admin_expenses"))
            if not category:
                flash("Kateqoriya seçin.", "danger")
                return redirect(url_for("admin_expenses"))
            if amount <= 0:
                flash("Məbləğ sıfırdan böyük olmalıdır.", "danger")
                return redirect(url_for("admin_expenses"))
            db.session.add(
                Expense(
                    period=period,
                    name=name,
                    category=category,
                    amount=round(amount, 2),
                    is_paid=False,
                    paid_at=None,
                    created_by_user_id=current_user().id,
                )
            )
            db.session.commit()
            audit(f"Xərc daxil edildi: {name} {amount:.2f} AZN period {period}")
            flash("Xərc daxil edildi.", "success")
            return redirect(url_for("admin_expenses"))

    period = request.args.get("period") or date.today().strftime("%Y-%m")
    return render_template("admin_expenses.html", **_get_admin_expenses_view_data(period))


@app.route("/admin/expenses/update/<int:expense_id>", methods=["POST"])
@login_required
@role_required("komendant", "superadmin")
def update_expense(expense_id):
    e = Expense.query.get_or_404(expense_id)
    period = (request.form.get("period", "") or "").strip()
    name = (request.form.get("name", "") or "").strip()
    category = _parse_expense_category(request.form.get("category"))
    amount = float(request.form.get("amount", "0") or 0)
    if not period or len(period) != 7:
        flash("Period duzgun deyil (YYYY-MM).", "danger")
        return redirect(url_for("admin_expenses", period=e.period))
    if not name:
        flash("Ad bos ola bilmez.", "danger")
        return redirect(url_for("admin_expenses", period=e.period))
    if not category:
        flash("Kateqoriya seçin.", "danger")
        return redirect(url_for("admin_expenses", period=e.period))
    if amount <= 0:
        flash("Məbləğ sıfırdan böyük olmalıdır.", "danger")
        return redirect(url_for("admin_expenses", period=e.period))

    old = f"{e.period} {e.name} {float(e.amount):.2f}"
    e.period = period
    e.name = name
    e.category = category
    e.amount = round(amount, 2)
    db.session.commit()
    audit(f"Xərc yeniləndi #{e.id}: {old} -> {e.period} {e.name} {float(e.amount):.2f}")
    flash("Xərc yeniləndi.", "success")
    return redirect(url_for("admin_expenses", period=period))


@app.route("/admin/expenses/delete/<int:expense_id>", methods=["POST"])
@login_required
@role_required("komendant", "superadmin")
def delete_expense(expense_id):
    e = Expense.query.get_or_404(expense_id)
    period = e.period
    desc = f"{e.name} {float(e.amount):.2f} period {e.period}"
    db.session.delete(e)
    db.session.commit()
    audit(f"Xərc silindi #{expense_id}: {desc}")
    flash("Xərc silindi.", "success")
    return redirect(url_for("admin_expenses", period=period))


@app.route("/admin/expenses/toggle-paid/<int:expense_id>", methods=["POST"])
@login_required
@role_required("komendant", "superadmin")
def toggle_expense_paid(expense_id):
    e = Expense.query.get_or_404(expense_id)
    e.is_paid = not bool(e.is_paid)
    e.paid_at = datetime.now(timezone.utc) if e.is_paid else None
    db.session.commit()
    audit(f"Xərc {'ödəndi' if e.is_paid else 'ödənilmədi'} #{e.id}: {e.name} {float(e.amount):.2f} period {e.period}")
    flash("Xərc statusu yeniləndi.", "success")
    return redirect(url_for("admin_expenses", period=e.period))


@app.route("/admin/invoices")
@login_required
@role_required("komendant", "superadmin")
def admin_invoices():
    selected_period = (request.args.get("period", "") or "").strip()
    building_id_raw = (request.args.get("building_id") or "").strip()
    building_id = int(building_id_raw) if building_id_raw.isdigit() else None
    return render_template("admin_invoices.html", **_get_admin_invoices_view_data(selected_period, building_id))


@app.route("/admin/payments/confirm/<int:payment_id>", methods=["POST"])
@login_required
@role_required("komendant", "superadmin")
def confirm_payment(payment_id):
    payment = Payment.query.get_or_404(payment_id)
    if payment.status != "pending":
        flash("Bu muraciet artiq emal olunub.", "warning")
        period = (request.form.get("period", "") or "").strip()
        return redirect(url_for("admin_invoices", period=period) if period else url_for("admin_invoices"))

    invoice = payment.invoice
    apply_amount = float(payment.amount)

    # FIX (warning #1): wrap the mutable operations and the status change in a
    # try/except so that a mid-flight exception cannot leave the in-memory
    # objects mutated without a matching DB commit.
    try:
        result = _apply_payment_delta(invoice, apply_amount)
        payment.status = "confirmed"
        payment.reviewer_user_id = current_user().id
        payment.reviewed_at = datetime.now(timezone.utc)
        db.session.commit()
    except Exception:
        db.session.rollback()
        flash("Xəta baş verdi. Ödəniş təsdiqlənmədi.", "danger")
        period = (request.form.get("period", "") or "").strip()
        return redirect(url_for("admin_invoices", period=period) if period else url_for("admin_invoices"))

    moved = float(result.get("moved_to_credit") or 0)
    removed = float(result.get("removed_from_credit") or 0)
    if moved > 0:
        audit(f"Odenis tesdiqlendi #{payment.id} {apply_amount:.2f} AZN (kredit +{moved:.2f})")
    elif removed > 0:
        audit(f"Odenis tesdiqlendi #{payment.id} {apply_amount:.2f} AZN (kredit -{removed:.2f})")
    else:
        audit(f"Odenis tesdiqlendi #{payment.id} {apply_amount:.2f} AZN")
    resident = invoice.apartment.owner
    if resident and resident.email:
        cfg = get_smtp_config()
        subject, body, html_body = build_receipt_email(payment, cfg)
        send_email(subject, body, [resident.email], html_body=html_body)
    flash("Odenis tesdiqlendi.", "success")
    period = (request.form.get("period", "") or "").strip()
    return redirect(url_for("admin_invoices", period=period) if period else url_for("admin_invoices"))


@app.route("/admin/payments/reject/<int:payment_id>", methods=["POST"])
@login_required
@role_required("komendant", "superadmin")
def reject_payment(payment_id):
    payment = Payment.query.get_or_404(payment_id)
    if payment.status != "pending":
        flash("Bu muraciet artiq emal olunub.", "warning")
        period = (request.form.get("period", "") or "").strip()
        return redirect(url_for("admin_invoices", period=period) if period else url_for("admin_invoices"))

    payment.status = "rejected"
    payment.reviewer_user_id = current_user().id
    payment.reviewed_at = datetime.now(timezone.utc)
    db.session.commit()
    audit(f"Odenis imtina edildi #{payment.id}")
    flash("Odenis muracieti imtina edildi.", "warning")
    period = (request.form.get("period", "") or "").strip()
    return redirect(url_for("admin_invoices", period=period) if period else url_for("admin_invoices"))

@app.route("/admin/payments/add/<int:invoice_id>", methods=["POST"])
@login_required
@role_required("komendant", "superadmin")
def add_payment(invoice_id):
    invoice = Invoice.query.get_or_404(invoice_id)
    try:
        amount = float(request.form["amount"])
    except (TypeError, ValueError):
        flash("Məbləğ düzgün deyil.", "danger")
        period = (request.form.get("period", "") or "").strip()
        return redirect(url_for("admin_invoices", period=period) if period else url_for("admin_invoices"))
    comment = (request.form.get("comment", "") or "").strip() or None

    if amount == 0:
        flash("Məbləğ sıfır ola bilməz.", "danger")
        period = (request.form.get("period", "") or "").strip()
        return redirect(url_for("admin_invoices", period=period) if period else url_for("admin_invoices"))

    apply_amount = amount
    now = datetime.now(timezone.utc)

    # FIX (warning #1): same try/except guard as confirm_payment.
    try:
        result = _apply_payment_delta(
            invoice, apply_amount, debt_adjustment=(apply_amount < 0)
        )
        payment = Payment(
            invoice_id=invoice.id,
            amount=apply_amount,
            comment=comment,
            status="confirmed",
            reviewer_user_id=current_user().id,
            reviewed_at=now,
            created_at=now,
        )
        db.session.add(payment)
        db.session.commit()
    except Exception:
        db.session.rollback()
        flash("Xəta baş verdi. Ödəniş daxil edilmədi.", "danger")
        period = (request.form.get("period", "") or "").strip()
        return redirect(url_for("admin_invoices", period=period) if period else url_for("admin_invoices"))

    moved = float(result.get("moved_to_credit") or 0)
    removed = float(result.get("removed_from_credit") or 0)
    if moved > 0:
        audit(f"Odenis daxil edildi invoice#{invoice.id} {apply_amount:.2f} AZN (kredit +{moved:.2f})")
    elif removed > 0:
        audit(f"Odenis daxil edildi invoice#{invoice.id} {apply_amount:.2f} AZN (kredit -{removed:.2f})")
    else:
        audit(f"Odenis daxil edildi invoice#{invoice.id} {apply_amount:.2f} AZN")
    resident = invoice.apartment.owner
    if resident and resident.email:
        cfg = get_smtp_config()
        subject, body, html_body = build_receipt_email(payment, cfg)
        send_email(subject, body, [resident.email], html_body=html_body)
    flash("Odenis daxil edildi.", "success")
    period = (request.form.get("period", "") or "").strip()
    return redirect(url_for("admin_invoices", period=period) if period else url_for("admin_invoices"))


@app.route("/admin/history")
@login_required
@role_required("komendant", "superadmin")
def admin_history():
    selected_month = (request.args.get("month") or "").strip()
    building_id_raw = (request.args.get("building_id") or "").strip()
    building_id = int(building_id_raw) if building_id_raw.isdigit() else None
    return render_template("admin_history.html", **_get_admin_history_view_data(selected_month, building_id))


def _iter_months_inclusive(from_period: str, to_period: str):
    def parse(p):
        y, m = p.split("-")
        return int(y), int(m)

    fy, fm = parse(from_period)
    ty, tm = parse(to_period)
    y, m = fy, fm
    while (y, m) <= (ty, tm):
        yield f"{y:04d}-{m:02d}"
        m += 1
        if m == 13:
            m = 1
            y += 1


def _safe_filename(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", (name or "").strip())


def _amount_to_text(value) -> str:
    return f"{float(value or 0):.2f} AZN"


def _build_xlsx(title: str, headers: list[str], rows: list[list[str]]) -> BytesIO:
    wb = Workbook()
    ws = wb.active
    ws.title = "Report"
    ws.append([title])
    ws.append(headers)
    for row in rows:
        ws.append(row)

    for idx in range(1, len(headers) + 1):
        col = ws.cell(row=2, column=idx).column_letter
        max_len = max(
            len(str(ws.cell(row=r, column=idx).value or ""))
            for r in range(1, ws.max_row + 1)
        )
        ws.column_dimensions[col].width = min(max(12, max_len + 2), 40)

    ws.freeze_panes = "A3"
    buffer = BytesIO()
    wb.save(buffer)
    buffer.seek(0)
    return buffer


def _get_admin_expenses_view_data(period: str):
    templates = ExpenseTemplate.query.order_by(ExpenseTemplate.is_active.desc(), ExpenseTemplate.name.asc()).all()
    expenses = Expense.query.filter_by(period=period).order_by(Expense.created_at.desc()).all()
    template_expense_by_template_id = {e.template_id: e for e in expenses if e.template_id}
    one_off_expenses = [e for e in expenses if not e.template_id]
    total = round(sum(e.amount for e in expenses), 2)
    return {
        "templates": templates,
        "expenses": expenses,
        "one_off_expenses": one_off_expenses,
        "period": period,
        "expenses_total": total,
        "template_expense_by_template_id": template_expense_by_template_id,
        "expense_categories": EXPENSE_CATEGORIES,
    }


def _get_admin_invoices_view_data(selected_period: str, building_id: Optional[int] = None):
    period_rows = (
        db.session.query(Invoice.period)
        .filter(Invoice.period.isnot(None), Invoice.period != "")
        .group_by(Invoice.period)
        .order_by(Invoice.period.desc())
        .all()
    )
    available_periods = [p for (p,) in period_rows]
    if not selected_period and available_periods:
        selected_period = available_periods[0]
    if selected_period and selected_period not in available_periods:
        selected_period = available_periods[0] if available_periods else ""

    apartment_number_sort = db.cast(Apartment.number, db.Integer)
    invoices_query = Invoice.query.join(Apartment, Invoice.apartment_id == Apartment.id)
    if selected_period:
        invoices_query = invoices_query.filter(Invoice.period == selected_period)
    if building_id:
        invoices_query = invoices_query.filter(Apartment.building_id == building_id)
    building_null_sort = db.case((Apartment.building_id.is_(None), 1), else_=0)
    invoices = invoices_query.order_by(building_null_sort.asc(), Apartment.building_id.asc(), apartment_number_sort.asc(), Apartment.number.asc()).all()

    dirty = False
    for inv in invoices:
        expected = "odenilib" if float(inv.paid_amount or 0) >= float(inv.amount or 0) else "gozlemede"
        if inv.status != expected:
            inv.status = expected
            dirty = True
    if dirty:
        db.session.commit()

    prev_period = None
    next_period = None
    if selected_period and selected_period in available_periods:
        idx = available_periods.index(selected_period)
        if idx > 0:
            next_period = available_periods[idx - 1]
        if idx < len(available_periods) - 1:
            prev_period = available_periods[idx + 1]

    return {
        "invoices": invoices,
        "selected_period": selected_period,
        "available_periods": available_periods,
        "prev_period": prev_period,
        "next_period": next_period,
        "building_id": building_id,
        "buildings_all": Building.query.order_by(Building.name.asc()).all(),
    }


def _get_admin_history_view_data(selected_month: str, building_id: Optional[int] = None):
    month_rows = (
        db.session.query(db.func.strftime("%Y-%m", Payment.created_at).label("month"))
        .filter(Payment.status == "confirmed", Payment.created_at.isnot(None))
        .union(
            db.session.query(db.func.strftime("%Y-%m", BalanceTopUp.created_at).label("month")).filter(
                BalanceTopUp.created_at.isnot(None)
            ),
            db.session.query(db.func.strftime("%Y-%m", Expense.created_at).label("month")).filter(
                Expense.created_at.isnot(None)
            ),
        )
        .all()
    )
    available_months = sorted([m for (m,) in month_rows if m], reverse=True)

    if not selected_month and available_months:
        selected_month = available_months[0]
    if selected_month and selected_month not in available_months:
        selected_month = available_months[0] if available_months else ""

    month_start = None
    month_end = None
    if selected_month:
        year, month = [int(x) for x in selected_month.split("-")]
        month_start = datetime(year, month, 1)
        month_end = datetime(year + 1, 1, 1) if month == 12 else datetime(year, month + 1, 1)

    limit = 500
    payments_query = (
        Payment.query.join(Invoice, Payment.invoice_id == Invoice.id)
        .join(Apartment, Invoice.apartment_id == Apartment.id)
        .filter(Payment.status == "confirmed")
    )
    topups_query = BalanceTopUp.query
    expenses_query = Expense.query
    if month_start and month_end:
        payments_query = payments_query.filter(Payment.created_at >= month_start, Payment.created_at < month_end)
        topups_query = topups_query.filter(BalanceTopUp.created_at >= month_start, BalanceTopUp.created_at < month_end)
        expenses_query = expenses_query.filter(Expense.created_at >= month_start, Expense.created_at < month_end)

    if building_id:
        payments_query = payments_query.filter(Apartment.building_id == building_id)

    confirmed_payments = payments_query.order_by(Payment.created_at.desc()).limit(limit).all()
    topups = topups_query.order_by(BalanceTopUp.created_at.desc()).limit(limit).all()
    expenses = expenses_query.order_by(Expense.created_at.desc()).limit(limit).all()

    events = []
    for p in confirmed_payments:
        inv = p.invoice
        apt = inv.apartment if inv else None
        apt_label = None
        if apt:
            apt_label = f"{apt.building.name} / {apt.number}" if apt.building else apt.number
        events.append(
            {
                "dt": p.created_at,
                "type": "payment",
                "amount": float(p.amount),
                "apartment": apt_label,
                "comment": (p.comment or "").strip() or None,
                "period": inv.period if inv else None,
            }
        )
    for t in topups:
        events.append(
            {
                "dt": t.created_at,
                "type": "topup",
                "amount": float(t.amount),
                "apartment": None,
                "comment": (t.comment or "").strip() or None,
                "period": None,
            }
        )
    for e in expenses:
        events.append(
            {
                "dt": e.created_at,
                "type": "expense",
                "amount": -float(e.amount),
                "apartment": None,
                "comment": (e.name or "").strip() or None,
                "period": e.period,
                "paid": bool(e.is_paid),
            }
        )

    events.sort(key=lambda x: x["dt"] or datetime.min, reverse=True)
    events = events[:limit]

    prev_month = None
    next_month = None
    if selected_month and selected_month in available_months:
        idx = available_months.index(selected_month)
        if idx > 0:
            next_month = available_months[idx - 1]
        if idx < len(available_months) - 1:
            prev_month = available_months[idx + 1]

    return {
        "events": events,
        "selected_month": selected_month,
        "available_months": available_months,
        "prev_month": prev_month,
        "next_month": next_month,
        "building_id": building_id,
        "buildings_all": Building.query.order_by(Building.name.asc()).all(),
    }


def _get_admin_payments_report_view_data(from_period: str, to_period: str, apartment_id_raw: str, building_id_raw: str = ""):
    apartment_id = int(apartment_id_raw) if apartment_id_raw.isdigit() else None
    building_id = int(building_id_raw) if building_id_raw.isdigit() else None
    periods = list(_iter_months_inclusive(from_period, to_period))
    if len(periods) > 24:
        periods = periods[-24:]
        from_period = periods[0]

    apartment_number_sort = db.cast(Apartment.number, db.Integer)
    building_null_sort = db.case((Apartment.building_id.is_(None), 1), else_=0)
    apartments_query = Apartment.query.order_by(building_null_sort.asc(), Apartment.building_id.asc(), apartment_number_sort.asc(), Apartment.number.asc())
    if apartment_id:
        apartments_query = apartments_query.filter(Apartment.id == apartment_id)
    if building_id:
        apartments_query = apartments_query.filter(Apartment.building_id == building_id)
    apartments = apartments_query.all()

    sums_query = (
        db.session.query(Apartment.id, Invoice.period, db.func.sum(Payment.amount))
        .join(Invoice, Invoice.apartment_id == Apartment.id)
        .join(Payment, Payment.invoice_id == Invoice.id)
        .filter(Payment.status == "confirmed", Invoice.period >= from_period, Invoice.period <= to_period)
    )
    if apartment_id:
        sums_query = sums_query.filter(Apartment.id == apartment_id)
    if building_id:
        sums_query = sums_query.filter(Apartment.building_id == building_id)
    sums = sums_query.group_by(Apartment.id, Invoice.period).all()
    amount_by_apt_period = {(apt_id, period): float(total or 0) for apt_id, period, total in sums}

    rows = []
    for a in apartments:
        row = {
            "apartment": a.number,
            "building": a.building.name if a.building else None,
            "amounts": [],
            "row_total": 0.0,
        }
        for p in periods:
            v = amount_by_apt_period.get((a.id, p), 0.0)
            row["amounts"].append(v)
            row["row_total"] += float(v)
        row["row_total"] = round(row["row_total"], 2)
        rows.append(row)

    col_totals = [round(sum(r["amounts"][idx] for r in rows), 2) for idx in range(len(periods))]
    grand_total = round(sum(col_totals), 2)
    buildings_all = Building.query.order_by(Building.name.asc()).all()
    return {
        "from_period": from_period,
        "to_period": to_period,
        "apartment_id": apartment_id,
        "building_id": building_id,
        "apartments_all": Apartment.query.order_by(Apartment.number).all(),
        "buildings_all": buildings_all,
        "periods": periods,
        "rows": rows,
        "col_totals": col_totals,
        "grand_total": grand_total,
    }


@app.route("/admin/payments-report")
@login_required
@role_required("komendant", "superadmin")
def admin_payments_report():
    # Payments per apartment per month (confirmed payments).
    today = date.today()
    default_to = today.strftime("%Y-%m")
    default_from = f"{today.year:04d}-{max(1, today.month - 5):02d}"

    from_period = (request.args.get("from_period") or default_from).strip()
    to_period = (request.args.get("to_period") or default_to).strip()
    apartment_id_raw = (request.args.get("apartment_id") or "").strip()
    building_id_raw = (request.args.get("building_id") or "").strip()
    if len(from_period) != 7 or len(to_period) != 7:
        flash("Period duzgun deyil (YYYY-MM).", "danger")
        return redirect(url_for("dashboard"))
    return render_template("admin_payments_report.html", **_get_admin_payments_report_view_data(from_period, to_period, apartment_id_raw, building_id_raw))


@app.route("/admin/export/payments-report/xlsx")
@login_required
@role_required("komendant", "superadmin")
def export_payments_report():
    today = date.today()
    default_to = today.strftime("%Y-%m")
    default_from = f"{today.year:04d}-{max(1, today.month - 5):02d}"
    from_period = (request.args.get("from_period") or default_from).strip()
    to_period = (request.args.get("to_period") or default_to).strip()
    apartment_id_raw = (request.args.get("apartment_id") or "").strip()
    building_id_raw = (request.args.get("building_id") or "").strip()
    data = _get_admin_payments_report_view_data(from_period, to_period, apartment_id_raw, building_id_raw)

    headers = ["Korpus", "Mənzil"] + data["periods"] + ["Cəmi"]
    rows = []
    for r in data["rows"]:
        rows.append([r["building"] or "", r["apartment"], *[_amount_to_text(v) for v in r["amounts"]], _amount_to_text(r["row_total"])])
    rows.append(["Cəmi", *[_amount_to_text(v) for v in data["col_totals"]], _amount_to_text(data["grand_total"])])
    title = f"Ödənişlər (mənzil/ay): {data['from_period']} - {data['to_period']}"
    filename = _safe_filename(f"payments_report_{data['from_period']}_{data['to_period']}.xlsx")
    payload = _build_xlsx(title, headers, rows)
    return send_file(payload, as_attachment=True, download_name=filename, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


@app.route("/admin/export/expenses/xlsx")
@login_required
@role_required("komendant", "superadmin")
def export_expenses():
    period = (request.args.get("period") or date.today().strftime("%Y-%m")).strip()
    data = _get_admin_expenses_view_data(period)
    headers = ["Tarix", "Tip", "Kateqoriya", "Ad", "Məbləğ", "Status"]
    rows = [
        [
            e.created_at.strftime("%d.%m.%Y %H:%M") if e.created_at else "",
            "Aylıq" if e.template_id else "Birdəfəlik",
            e.category or "",
            e.name,
            _amount_to_text(e.amount),
            "Ödənilib" if e.is_paid else "Gözləmədə",
        ]
        for e in data["expenses"]
    ]
    rows.append(["", "", "", "Cəmi", _amount_to_text(data["expenses_total"]), "", ""])
    title = f"Xərclər: {period}"
    filename = _safe_filename(f"expenses_{period}.xlsx")
    payload = _build_xlsx(title, headers, rows)
    return send_file(payload, as_attachment=True, download_name=filename, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


@app.route("/admin/export/invoices/xlsx")
@login_required
@role_required("komendant", "superadmin")
def export_invoices():
    selected_period = (request.args.get("period", "") or "").strip()
    building_id_raw = (request.args.get("building_id") or "").strip()
    building_id = int(building_id_raw) if building_id_raw.isdigit() else None
    data = _get_admin_invoices_view_data(selected_period, building_id)
    headers = ["Korpus", "Mənzil", "Period", "Hesablanıb", "Ödənilib", "Balans", "Status"]
    rows = []
    for i in data["invoices"]:
        balance = float(i.paid_amount or 0) - float(i.amount or 0)
        status = "ödənilib" if float(i.paid_amount or 0) >= float(i.amount or 0) else "ödənilməyib"
        rows.append(
            [
                i.apartment.building.name if i.apartment and i.apartment.building else "",
                i.apartment.number if i.apartment else "",
                i.period,
                _amount_to_text(i.amount),
                _amount_to_text(i.paid_amount),
                _amount_to_text(balance),
                status,
            ]
        )
    title = f"Ödənişlər: {data['selected_period'] or 'bütün periodlar'}"
    filename = _safe_filename(f"invoices_{(data['selected_period'] or 'all')}.xlsx")
    payload = _build_xlsx(title, headers, rows)
    return send_file(payload, as_attachment=True, download_name=filename, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


@app.route("/admin/export/history/xlsx")
@login_required
@role_required("komendant", "superadmin")
def export_history():
    selected_month = (request.args.get("month") or "").strip()
    building_id_raw = (request.args.get("building_id") or "").strip()
    building_id = int(building_id_raw) if building_id_raw.isdigit() else None
    data = _get_admin_history_view_data(selected_month, building_id)
    headers = ["Tarix", "Növ", "Mənzil", "Period", "Status", "Məbləğ", "Qeyd"]
    rows = []
    for e in data["events"]:
        event_type = "Ödəniş" if e["type"] == "payment" else ("Mədaxil" if e["type"] == "topup" else "Xərc")
        status = "-"
        if e["type"] == "expense":
            status = "Ödənilib" if e.get("paid") else "Gözləmədə"
        rows.append(
            [
                e["dt"].strftime("%d.%m.%Y %H:%M") if e.get("dt") else "",
                event_type,
                e.get("apartment") or "-",
                e.get("period") or "-",
                status,
                _amount_to_text(e.get("amount")),
                e.get("comment") or "",
            ]
        )
    title = f"Tarixçə: {data['selected_month'] or 'bütün aylar'}"
    filename = _safe_filename(f"history_{(data['selected_month'] or 'all')}.xlsx")
    payload = _build_xlsx(title, headers, rows)
    return send_file(payload, as_attachment=True, download_name=filename, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


def _render_print_report(title: str, headers: list[str], rows: list[list[str]]):
    return render_template("admin_report_print.html", title=title, headers=headers, rows=rows)


@app.route("/admin/print/payments-report")
@login_required
@role_required("komendant", "superadmin")
def print_payments_report():
    today = date.today()
    default_to = today.strftime("%Y-%m")
    default_from = f"{today.year:04d}-{max(1, today.month - 5):02d}"
    from_period = (request.args.get("from_period") or default_from).strip()
    to_period = (request.args.get("to_period") or default_to).strip()
    apartment_id_raw = (request.args.get("apartment_id") or "").strip()
    building_id_raw = (request.args.get("building_id") or "").strip()
    data = _get_admin_payments_report_view_data(from_period, to_period, apartment_id_raw, building_id_raw)
    headers = ["Korpus", "Mənzil"] + data["periods"] + ["Cəmi"]
    rows = [[r["building"] or "", r["apartment"], *[_amount_to_text(v) for v in r["amounts"]], _amount_to_text(r["row_total"])] for r in data["rows"]]
    rows.append(["", "Cəmi", *[_amount_to_text(v) for v in data["col_totals"]], _amount_to_text(data["grand_total"])])
    return _render_print_report(f"Ödənişlər (mənzil/ay): {data['from_period']} - {data['to_period']}", headers, rows)


@app.route("/admin/print/expenses")
@login_required
@role_required("komendant", "superadmin")
def print_expenses():
    period = (request.args.get("period") or date.today().strftime("%Y-%m")).strip()
    data = _get_admin_expenses_view_data(period)
    headers = ["Tarix", "Tip", "Kateqoriya", "Ad", "Məbləğ", "Status"]
    rows = [
        [
            e.created_at.strftime("%d.%m.%Y %H:%M") if e.created_at else "",
            "Aylıq" if e.template_id else "Birdəfəlik",
            e.category or "",
            e.name,
            _amount_to_text(e.amount),
            "Ödənilib" if e.is_paid else "Gözləmədə",
        ]
        for e in data["expenses"]
    ]
    rows.append(["", "", "", "Cəmi", _amount_to_text(data["expenses_total"]), "", ""])
    return _render_print_report(f"Xərclər: {period}", headers, rows)


@app.route("/admin/print/invoices")
@login_required
@role_required("komendant", "superadmin")
def print_invoices():
    selected_period = (request.args.get("period", "") or "").strip()
    building_id_raw = (request.args.get("building_id") or "").strip()
    building_id = int(building_id_raw) if building_id_raw.isdigit() else None
    data = _get_admin_invoices_view_data(selected_period, building_id)
    headers = ["Korpus", "Mənzil", "Period", "Hesablanıb", "Ödənilib", "Balans", "Status"]
    rows = []
    for i in data["invoices"]:
        balance = float(i.paid_amount or 0) - float(i.amount or 0)
        rows.append(
            [
                i.apartment.building.name if i.apartment and i.apartment.building else "",
                i.apartment.number if i.apartment else "",
                i.period,
                _amount_to_text(i.amount),
                _amount_to_text(i.paid_amount),
                _amount_to_text(balance),
                "ödənilib" if float(i.paid_amount or 0) >= float(i.amount or 0) else "ödənilməyib",
            ]
        )
    return _render_print_report(f"Ödənişlər: {data['selected_period'] or 'bütün periodlar'}", headers, rows)


@app.route("/admin/print/history")
@login_required
@role_required("komendant", "superadmin")
def print_history():
    selected_month = (request.args.get("month") or "").strip()
    building_id_raw = (request.args.get("building_id") or "").strip()
    building_id = int(building_id_raw) if building_id_raw.isdigit() else None
    data = _get_admin_history_view_data(selected_month, building_id)
    headers = ["Tarix", "Növ", "Mənzil", "Period", "Status", "Məbləğ", "Qeyd"]
    rows = []
    for e in data["events"]:
        event_type = "Ödəniş" if e["type"] == "payment" else ("Mədaxil" if e["type"] == "topup" else "Xərc")
        status = "-"
        if e["type"] == "expense":
            status = "Ödənilib" if e.get("paid") else "Gözləmədə"
        rows.append(
            [
                e["dt"].strftime("%d.%m.%Y %H:%M") if e.get("dt") else "",
                event_type,
                e.get("apartment") or "-",
                e.get("period") or "-",
                status,
                _amount_to_text(e.get("amount")),
                e.get("comment") or "",
            ]
        )
    return _render_print_report(f"Tarixçə: {data['selected_month'] or 'bütün aylar'}", headers, rows)


@app.route("/resident/receipt/<int:payment_id>")
@login_required
@role_required("resident")
def resident_receipt(payment_id):
    payment = Payment.query.get_or_404(payment_id)
    user = current_user()
    if payment.invoice.apartment.owner_user_id != user.id:
        flash("Qəbzə giriş mümkün deyil.", "danger")
        return redirect(url_for("dashboard"))
    return render_template("receipt.html", payment=payment)


@app.route("/admin/content", methods=["GET", "POST"])
@login_required
@role_required("komendant", "superadmin")
def admin_content():
    content_type = request.args.get("type", "work")
    if request.method == "POST":
        form_type = request.form.get("form_type")
        if form_type == "work":
            before_photo = save_uploaded_image(request.files.get("before_photo_file"))
            after_photo = save_uploaded_image(request.files.get("after_photo_file"))
            db.session.add(
                WorkLog(
                    title=request.form["title"].strip(),
                    description=request.form["description"].strip(),
                    before_photo_url=before_photo,
                    after_photo_url=after_photo,
                )
            )
            audit("Is jurnalina qeyd elave edildi")
            flash("Is elave edildi.", "success")
            sysname = (get_smtp_config().system_name or "").strip() or "eMTK"
            notify_residents(f"{sysname}: Yeni isler", f"Yeni is elave edildi: {request.form['title'].strip()}")
        elif form_type == "announcement":
            title = request.form.get("title", "").strip()
            text = request.form.get("text", "").strip()
            if not title or not text:
                flash("Başlıq və mətn vacibdir.", "danger")
                return redirect(url_for("admin_content", type="announcement"))
            db.session.add(Announcement(title=title, text=text))
            audit("Yeni elan elave edildi")
            flash("Elan elave edildi.", "success")
            sysname = (get_smtp_config().system_name or "").strip() or "eMTK"
            notify_residents(f"{sysname}: Yeni elan", f"Yeni elan: {title}")
        db.session.commit()
        return redirect(url_for("admin_content", type=content_type))

    works = WorkLog.query.order_by(WorkLog.created_at.desc()).all()
    announcements = Announcement.query.order_by(Announcement.created_at.desc()).all()
    return render_template("admin_content.html", works=works, announcements=announcements, content_type=content_type)


@app.route("/admin/announcements/update/<int:announcement_id>", methods=["POST"])
@login_required
@role_required("komendant", "superadmin")
def update_announcement(announcement_id):
    announcement = Announcement.query.get_or_404(announcement_id)
    announcement.title = request.form["title"].strip()
    announcement.text = request.form["text"].strip()
    db.session.commit()
    audit(f"Elan yenilendi #{announcement_id}")
    flash("Elan yenilendi.", "success")
    return redirect(url_for("admin_content", type="announcement"))


@app.route("/admin/announcements/delete/<int:announcement_id>", methods=["POST"])
@login_required
@role_required("komendant", "superadmin")
def delete_announcement(announcement_id):
    announcement = Announcement.query.get_or_404(announcement_id)
    db.session.delete(announcement)
    db.session.commit()
    audit(f"Elan silindi #{announcement_id}")
    flash("Elan silindi.", "success")
    return redirect(url_for("admin_content", type="announcement"))


@app.route("/admin/worklogs/update/<int:worklog_id>", methods=["POST"])
@login_required
@role_required("komendant", "superadmin")
def update_worklog(worklog_id):
    w = WorkLog.query.get_or_404(worklog_id)
    w.title = request.form["title"].strip()
    w.description = request.form["description"].strip()

    before_photo = save_uploaded_image(request.files.get("before_photo_file"))
    after_photo = save_uploaded_image(request.files.get("after_photo_file"))
    if before_photo:
        w.before_photo_url = before_photo
    if after_photo:
        w.after_photo_url = after_photo

    db.session.commit()
    audit(f"Is yenilendi #{worklog_id}")
    flash("Is yenilendi.", "success")
    return redirect(url_for("admin_content", type="work"))


@app.route("/admin/worklogs/delete/<int:worklog_id>", methods=["POST"])
@login_required
@role_required("komendant", "superadmin")
def delete_worklog(worklog_id):
    w = WorkLog.query.get_or_404(worklog_id)
    db.session.delete(w)
    db.session.commit()
    audit(f"Is silindi #{worklog_id}")
    flash("Is silindi.", "success")
    return redirect(url_for("admin_content", type="work"))


@app.route("/admin/users")
@login_required
@role_required("komendant", "superadmin")
def admin_users():
    users = User.query.order_by(User.id.desc()).all()
    return render_template("admin_users.html", users=users)


@app.route("/admin/users/create", methods=["POST"])
@login_required
@role_required("komendant", "superadmin")
def admin_user_create():
    full_name = (request.form.get("full_name") or "").strip()
    phone = (request.form.get("phone") or "").strip() or None
    email = (request.form.get("email") or "").strip().lower()
    role = (request.form.get("role") or "resident").strip()
    password = request.form.get("password") or ""

    if not full_name or not email or not password:
        flash("Zorunlu sahələr boş ola bilməz.", "danger")
        return redirect(url_for("admin_users"))
    if phone and not normalize_az_phone(phone):
        flash("Telefon formatı +994XXXXXXXXX olmalıdır.", "danger")
        return redirect(url_for("admin_users"))
    if role == "commandant":
        role = "komendant"
    if role not in ("resident", "komendant", "superadmin"):
        flash("Rol duzgun deyil.", "danger")
        return redirect(url_for("admin_users"))
    me = current_user()
    if role == "superadmin" and (not me or me.role != "superadmin"):
        flash("Yalnız superadmin superadmin yarada bilər.", "danger")
        return redirect(url_for("admin_users"))
    if User.query.filter_by(email=email).first():
        flash("Bu email ile artiq istifadəçi var.", "warning")
        return redirect(url_for("admin_users"))

    db.session.add(
        User(
            full_name=full_name,
            phone=phone,
            email=email,
            password_hash=generate_password_hash(password),
            role=role,
        )
    )
    db.session.commit()
    audit(f"İstifadəçi yaradıldı {email} ({role})")
    flash("İstifadəçi yaradıldı.", "success")
    return redirect(url_for("admin_users"))


@app.route("/admin/users/update/<int:user_id>", methods=["POST"])
@login_required
@role_required("komendant", "superadmin")
def admin_user_update(user_id):
    target = User.query.get_or_404(user_id)

    full_name = (request.form.get("full_name") or "").strip()
    phone = (request.form.get("phone") or "").strip() or None
    email = (request.form.get("email") or "").strip().lower()
    role = (request.form.get("role") or target.role).strip()
    new_password = (request.form.get("password") or "").strip()

    if not full_name or not email:
        flash("Ad və email boş ola bilməz.", "danger")
        return redirect(url_for("admin_users"))
    if phone and not normalize_az_phone(phone):
        flash("Telefon formatı +994XXXXXXXXX olmalıdır.", "danger")
        return redirect(url_for("admin_users"))
    if role == "commandant":
        role = "komendant"
    if role not in ("resident", "komendant", "superadmin"):
        flash("Rol duzgun deyil.", "danger")
        return redirect(url_for("admin_users"))
    me = current_user()
    if (not me or me.role != "superadmin") and (
        target.role == "superadmin" or role == "superadmin"
    ):
        flash("Superadmin hesablarını yalnız superadmin idarə edə bilər.", "danger")
        return redirect(url_for("admin_users"))
    duplicate = User.query.filter(User.email == email, User.id != target.id).first()
    if duplicate:
        flash("Bu email başqa istifadəçidə var.", "warning")
        return redirect(url_for("admin_users"))

    # Prevent self-demoting to resident (locks admin out).
    if me and me.id == target.id and target.role in ("komendant", "superadmin") and role == "resident":
        flash("Öz rolunuzu resident edə bilməzsiniz.", "warning")
        return redirect(url_for("admin_users"))

    target.full_name = full_name
    target.phone = phone
    target.email = email
    target.role = role
    if new_password:
        target.password_hash = generate_password_hash(new_password)

    db.session.commit()
    audit(f"İstifadəçi yeniləndi #{target.id} {target.email} ({target.role})")
    flash("İstifadəçi yeniləndi.", "success")
    return redirect(url_for("admin_users"))


@app.route("/admin/users/delete/<int:user_id>", methods=["POST"])
@login_required
@role_required("komendant", "superadmin")
def admin_user_delete(user_id):
    target = User.query.get_or_404(user_id)
    me = current_user()
    if me and me.id == target.id:
        flash("Özünüzü silə bilməzsiniz.", "warning")
        return redirect(url_for("admin_users"))
    if me and me.role != "superadmin" and target.role == "superadmin":
        flash("Superadmin hesabını yalnız superadmin silə bilər.", "danger")
        return redirect(url_for("admin_users"))

    has_apartments = Apartment.query.filter_by(owner_user_id=target.id).first() is not None
    referenced = (
        Payment.query.filter_by(reviewer_user_id=target.id).first() is not None
        or BalanceTopUp.query.filter_by(created_by_user_id=target.id).first() is not None
        or Expense.query.filter_by(created_by_user_id=target.id).first() is not None
        or AuditLog.query.filter_by(actor_user_id=target.id).first() is not None
    )
    if has_apartments or referenced:
        flash("İstifadəçini silmək olmur: bağlı məlumatlar var.", "warning")
        return redirect(url_for("admin_users"))

    email = target.email
    db.session.delete(target)
    db.session.commit()
    audit(f"İstifadəçi silindi {email}")
    flash("İstifadəçi silindi.", "success")
    return redirect(url_for("admin_users"))


@app.route("/polls", methods=["GET", "POST"])
@login_required
def polls():
    flash("Sorğular və səsvermələr funksiyası deaktiv edilib.", "info")
    return redirect(url_for("dashboard"))


@app.route("/admin/invoices/send/<int:invoice_id>", methods=["POST"])
@login_required
@role_required("komendant", "superadmin")
def send_invoice_email(invoice_id):
    invoice = Invoice.query.get_or_404(invoice_id)
    resident = invoice.apartment.owner
    if not resident.email:
        flash("Sakinin email adresi yoxdur.", "warning")
        return redirect(url_for("admin_invoices"))
    cfg = get_smtp_config()
    subject, body, html_body = build_invoice_email(invoice, resident, cfg)
    if send_email(subject, body, [resident.email], html_body=html_body):
        flash("Hesab-faktura email ilə göndərildi.", "success")
    else:
        flash("Email gonderilemedi. SMTP ayarlarini yoxlayin.", "danger")
    return redirect(url_for("admin_invoices"))


@app.route("/admin/invoices/print/<int:invoice_id>")
@login_required
@role_required("komendant", "superadmin")
def print_invoice(invoice_id):
    invoice = Invoice.query.get_or_404(invoice_id)
    cfg = get_smtp_config()
    return render_template(
        "invoice_print.html",
        invoice=invoice,
        resident=invoice.apartment.owner,
        cfg=cfg,
        system_name=(cfg.system_name or "").strip() or "eMTK",
        issue_date=utc_to_local(invoice.created_at),
    )


@app.route("/admin/settings", methods=["GET", "POST"])
@login_required
@role_required("superadmin", "komendant")
def admin_settings():
    cfg = get_smtp_config()
    if request.method == "POST":
        form_type = request.form.get("form_type", "save_smtp")
        if form_type == "save_system":
            cfg.system_name = request.form.get("system_name", "").strip() or None
            cfg.house_address = request.form.get("house_address", "").strip() or None
            cfg.commandant_name = request.form.get("commandant_name", "").strip() or None
            cfg.contact_phone = request.form.get("contact_phone", "").strip() or None
            db.session.commit()
            audit("Sistem ayarlari yenilendi")
            flash("Sistem ayarlari saxlanildi.", "success")
        elif form_type == "save_smtp":
            cfg.host = request.form.get("host", "").strip() or None
            cfg.port = int(request.form.get("port", "587"))
            cfg.username = request.form.get("username", "").strip() or None
            new_password = request.form.get("password", "").strip()
            if new_password:
                cfg.password = new_password
            cfg.sender_email = request.form.get("sender_email", "").strip() or None
            cfg.use_tls = request.form.get("use_tls") == "on"
            db.session.commit()
            audit("SMTP ayarlari yenilendi")
            flash("SMTP ayarlari saxlanildi.", "success")
        elif form_type == "test_email":
            to_email = request.form.get("test_email", "").strip()
            sysname = (cfg.system_name or "").strip() or "eMTK"
            if send_email(f"{sysname} SMTP test", "Bu test mesajidir.", [to_email]):
                flash("Test email ugurla gonderildi.", "success")
            else:
                flash("Test email gonderilmedi. SMTP ayarlarini yoxlayin.", "danger")
        elif form_type == "add_apartment_preset":
            name = (request.form.get("name", "") or "").strip()
            rooms_raw = (request.form.get("rooms", "") or "").strip()
            area_raw = (request.form.get("area", "") or "").strip()
            if not name or not rooms_raw or not area_raw:
                flash("Preset üçün ad, otaq və sahə vacibdir.", "danger")
                return redirect(url_for("admin_settings"))
            try:
                rooms = _parse_int_field(rooms_raw, min_value=1, max_value=999)
                area = _parse_int_field(area_raw, min_value=1, max_value=9999)
            except ValueError:
                flash("Preset üçün otaq 1-999, sahə isə 1-9999 aralığında tam ədəd olmalıdır.", "danger")
                return redirect(url_for("admin_settings"))
            if rooms is None or area is None:
                flash("Otaq və sahə sıfırdan böyük olmalıdır.", "danger")
                return redirect(url_for("admin_settings"))
            db.session.add(ApartmentPreset(name=name, rooms=rooms, area=area))
            db.session.commit()
            audit(f"Mənzil preset əlavə edildi: {name} ({rooms} otaq, {area} m2)")
            flash("Mənzil preset əlavə edildi.", "success")
        elif form_type == "update_apartment_preset":
            preset_id = int(request.form["preset_id"])
            preset = ApartmentPreset.query.get_or_404(preset_id)
            name = (request.form.get("name", "") or "").strip()
            rooms_raw = (request.form.get("rooms", "") or "").strip()
            area_raw = (request.form.get("area", "") or "").strip()
            try:
                rooms = _parse_int_field(rooms_raw, min_value=1, max_value=999)
                area = _parse_int_field(area_raw, min_value=1, max_value=9999)
            except ValueError:
                flash("Preset məlumatları düzgün deyil: otaq 1-999, sahə 1-9999 olmalıdır.", "danger")
                return redirect(url_for("admin_settings"))
            if not name or rooms is None or area is None:
                flash("Preset məlumatları düzgün deyil.", "danger")
                return redirect(url_for("admin_settings"))
            preset.name = name
            preset.rooms = rooms
            preset.area = area
            db.session.commit()
            audit(f"Mənzil preset yeniləndi #{preset_id}")
            flash("Mənzil preset yeniləndi.", "success")
        elif form_type == "delete_apartment_preset":
            preset_id = int(request.form["preset_id"])
            preset = ApartmentPreset.query.get_or_404(preset_id)
            preset_name = preset.name
            db.session.delete(preset)
            db.session.commit()
            audit(f"Mənzil preset silindi #{preset_id}: {preset_name}")
            flash("Mənzil preset silindi.", "success")
        elif form_type == "add_building":
            name = (request.form.get("building_name", "") or "").strip()
            address = (request.form.get("building_address", "") or "").strip() or None
            if not name:
                flash("Korpus adı vacibdir.", "danger")
                return redirect(url_for("admin_settings"))
            if Building.query.filter_by(name=name).first():
                flash("Bu adda korpus artıq mövcuddur.", "warning")
                return redirect(url_for("admin_settings"))
            db.session.add(Building(name=name, address=address))
            db.session.commit()
            audit(f"Korpus əlavə edildi: {name}")
            flash("Korpus əlavə edildi.", "success")
        elif form_type == "update_building":
            building_id = int(request.form["building_id"])
            building = Building.query.get_or_404(building_id)
            name = (request.form.get("building_name", "") or "").strip()
            address = (request.form.get("building_address", "") or "").strip() or None
            if not name:
                flash("Korpus adı vacibdir.", "danger")
                return redirect(url_for("admin_settings"))
            duplicate = Building.query.filter(Building.name == name, Building.id != building_id).first()
            if duplicate:
                flash("Bu adda korpus artıq mövcuddur.", "warning")
                return redirect(url_for("admin_settings"))
            building.name = name
            building.address = address
            db.session.commit()
            audit(f"Korpus yeniləndi #{building_id}: {name}")
            flash("Korpus yeniləndi.", "success")
        elif form_type == "delete_building":
            building_id = int(request.form["building_id"])
            building = Building.query.get_or_404(building_id)
            if building.apartments:
                flash("Korpusu silmək olmur: bağlı mənzillər var.", "warning")
                return redirect(url_for("admin_settings"))
            building_name = building.name
            db.session.delete(building)
            db.session.commit()
            audit(f"Korpus silindi #{building_id}: {building_name}")
            flash("Korpus silindi.", "success")
        return redirect(url_for("admin_settings"))
    apartment_presets = ApartmentPreset.query.order_by(ApartmentPreset.rooms.asc(), ApartmentPreset.area.asc()).all()
    buildings = Building.query.order_by(Building.name.asc()).all()
    return render_template("admin_settings.html", cfg=cfg, apartment_presets=apartment_presets, buildings=buildings)


@app.route("/admin/reset-financial", methods=["POST"])
@login_required
@role_required("superadmin")
def admin_reset_financial():
    # Reload from DB to get fresh password_hash (avoids SQLAlchemy identity-map stale data)
    user = db.session.get(User, session.get("user_id"))
    if not user:
        flash("İstifadəçi tapılmadı.", "danger")
        return redirect(url_for("admin_settings"))
    password = request.form.get("confirm_password", "")
    if not check_password_hash(user.password_hash, password):
        flash("Şifrə yanlışdır. Sıfırlama ləğv edildi.", "danger")
        return redirect(url_for("admin_settings"))

    # Delete in dependency order to avoid FK violations
    Payment.query.delete(synchronize_session=False)
    # Remove all invoices (including future months) and expenses.
    Invoice.query.delete(synchronize_session=False)
    Expense.query.delete(synchronize_session=False)
    # Reset apartment credit balances
    for apt in Apartment.query.all():
        apt.credit_balance = Decimal("0.00")
    # Delete standalone financial history
    BalanceTopUp.query.delete(synchronize_session=False)
    AuditLog.query.delete(synchronize_session=False)
    db.session.commit()

    # Write a fresh audit entry after the wipe
    db.session.add(AuditLog(actor_user_id=user.id, action="Bütün maliyyə məlumatları sıfırlandı (superadmin)"))
    db.session.commit()

    flash("Bütün ödəniş, hesab, xərc, borc və tarixçə məlumatları sıfırlandı.", "success")
    return redirect(url_for("admin_settings"))


@app.route("/admin/health/money-schema")
@login_required
@role_required("superadmin", "komendant")
def admin_health_money_schema():
    expected = {
        "apartment": {"credit_balance"},
        "tariff": {"amount"},
        "invoice": {"amount", "paid_amount"},
        "payment": {"amount"},
        "expense_template": {"default_amount"},
        "expense": {"amount"},
        "balance_top_up": {"amount"},
    }
    result = {"ok": True, "tables": {}}
    with db.engine.connect() as conn:
        for table_name, money_columns in expected.items():
            pragma_rows = conn.exec_driver_sql(f"PRAGMA table_info({table_name})").fetchall()
            col_type = {row[1]: str(row[2] or "").upper() for row in pragma_rows}
            table_info = {}
            for column in sorted(money_columns):
                actual = col_type.get(column, "")
                is_numeric = "NUMERIC" in actual
                table_info[column] = {"actual_type": actual, "is_numeric": is_numeric}
                if not is_numeric:
                    result["ok"] = False
            result["tables"][table_name] = table_info
    return result


@app.route("/admin/health/calculation-smoke")
@login_required
@role_required("superadmin", "komendant")
def admin_health_calculation_smoke():
    """
    Run deterministic calculation smoke-checks without DB writes.
    """
    checks = []

    def _check(name: str, actual: float, expected: float):
        actual_rounded = round(float(actual), 2)
        expected_rounded = round(float(expected), 2)
        ok = actual_rounded == expected_rounded
        checks.append(
            {
                "name": name,
                "ok": ok,
                "actual": actual_rounded,
                "expected": expected_rounded,
            }
        )

    # Scenario 1: credit auto-applies to unpaid invoice.
    apt1 = Apartment(area=80.0, credit_balance=Decimal("15.00"))
    inv1 = Invoice(amount=Decimal("100.00"), paid_amount=Decimal("20.00"), status="gozlemede")
    inv1.apartment = apt1
    applied = _apply_credit_to_invoice(inv1)
    _check("credit_applied_amount", applied, 15.00)
    _check("credit_applied_paid_amount", inv1.paid_amount, 35.00)
    _check("credit_applied_credit_left", apt1.credit_balance, 0.00)

    # Scenario 2: payment overpay moves overflow into apartment credit.
    apt2 = Apartment(area=60.0, credit_balance=Decimal("0.00"))
    inv2 = Invoice(amount=Decimal("100.00"), paid_amount=Decimal("95.00"), status="gozlemede")
    inv2.apartment = apt2
    delta_info_overpay = _apply_payment_delta(inv2, 10.00)
    _check("overpay_paid_capped_to_invoice_amount", inv2.paid_amount, 100.00)
    _check("overpay_moved_to_credit", delta_info_overpay["moved_to_credit"], 5.00)
    _check("overpay_credit_balance", apt2.credit_balance, 5.00)

    # Scenario 3: negative correction first consumes apartment credit.
    apt3 = Apartment(area=60.0, credit_balance=Decimal("4.00"))
    inv3 = Invoice(amount=Decimal("100.00"), paid_amount=Decimal("90.00"), status="gozlemede")
    inv3.apartment = apt3
    delta_info_reversal = _apply_payment_delta(inv3, -6.00)
    _check("reversal_removed_from_credit", delta_info_reversal["removed_from_credit"], 4.00)
    _check("reversal_credit_after_consume", apt3.credit_balance, 0.00)
    _check("reversal_paid_after_remainder", inv3.paid_amount, 88.00)

    # Scenario 3b: negative Mədaxil (debt adjustment) lowers paid_amount; balance follows.
    apt3b = Apartment(area=60.0, credit_balance=Decimal("0.00"))
    inv3b = Invoice(amount=Decimal("100.00"), paid_amount=Decimal("49.00"), status="gozlemede")
    inv3b.apartment = apt3b
    _apply_payment_delta(inv3b, -50.00, debt_adjustment=True)
    _check("debt_adjust_paid_amount", inv3b.paid_amount, -1.00)
    _check("debt_adjust_invoice_balance", float(inv3b.paid_amount) - float(inv3b.amount), -101.00)

    # Scenario 4: resident debt uses max(0, base_debt - credit).
    base_debt = max(0.0, 100.0 - 100.0) + max(0.0, 50.0 - 30.0)
    credit_balance = 25.0
    resident_debt = max(0.0, round(base_debt - credit_balance, 2))
    _check("resident_debt_clamped", resident_debt, 0.00)

    # Scenario 5: invoice amount uses tariff scope and types.
    apt4 = Apartment(id=101, area=50.0)
    t1 = Tariff(id=201, type="per_m2", amount=Decimal("1.20"))
    t2 = Tariff(id=202, type="fixed", amount=Decimal("10.00"))
    t3 = Tariff(id=203, type="fixed", amount=Decimal("7.00"))
    scope_map = {203: {999}}  # excluded for this apartment
    amount_total = compute_invoice_amount(apt4, [t1, t2, t3], scope_map)
    _check("invoice_amount_from_tariffs", amount_total, 70.00)

    ok = all(c["ok"] for c in checks)
    return {"ok": ok, "checks": checks}


@app.route("/init")
def init_data():
    if os.getenv("ENABLE_INIT_ROUTE", "0") != "1":
        abort(404)
    db.create_all()
    ensure_money_numeric_schema()
    if User.query.count() == 0:
        superadmin = User(
            full_name="Admin",
            phone="+000000",
            email="admin@emtk.itg.az",
            password_hash=generate_password_hash("admin"),
            role="superadmin",
        )
        komendant = User(
            full_name="Komendant",
            phone="+111111",
            email="commandant@smartzhk.local",
            password_hash=generate_password_hash("commandant123"),
            role="komendant",
        )
        resident = User(
            full_name="Жилец 1",
            phone="+222222",
            email="resident@smartzhk.local",
            password_hash=generate_password_hash("resident123"),
            role="resident",
        )
        db.session.add_all([superadmin, komendant, resident])
        db.session.commit()

        apt = Apartment(number="A-101", floor=1, area=82.5, owner_user_id=resident.id)
        db.session.add(apt)
        db.session.add_all(
            [
                Tariff(name="Эксплуатация", type="per_m2", amount=0.8),
                Tariff(name="Охрана", type="fixed", amount=25),
            ]
        )
        db.session.add(WorkLog(title="Ремонт плитки в лобби", description="Заменили поврежденную плитку, зона открыта."))
        db.session.add(Announcement(title="Отключение воды", text="Во вторник с 10:00 до 14:00 запланированы работы."))
        db.session.commit()
    return "Initialized. Open /login"


if __name__ == "__main__":
    with app.app_context():
        db.create_all()
        ensure_money_numeric_schema()
        ensure_poll_schema()
        ensure_payment_schema()
        ensure_apartment_schema()
        ensure_apartment_preset_schema()
        ensure_system_schema()
        ensure_expense_schema()
        ensure_balance_schema()
        ensure_tariff_scope_schema()
        ensure_building_schema()
        ensure_user_role_migration()
        ensure_default_superadmin_seed()
        get_smtp_config()
    host = os.getenv("FLASK_HOST", "0.0.0.0")
    port = int(os.getenv("FLASK_PORT", "5000"))
    debug = os.getenv("FLASK_DEBUG", "0") == "1"
    app.run(host=host, port=port, debug=debug)
