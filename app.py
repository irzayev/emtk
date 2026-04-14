import os
import smtplib
import uuid
from datetime import date, datetime
from email.message import EmailMessage
from functools import wraps
from pathlib import Path

from flask import Flask, abort, flash, redirect, render_template, request, session, url_for
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_sqlalchemy import SQLAlchemy
from flask_wtf.csrf import CSRFProtect
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename


app = Flask(__name__)
# Persist SQLite DB in /app/instance (volume-mounted in compose).
app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("DATABASE_URL", "sqlite:////app/instance/smart_zhk.db")
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "change-this-in-production")
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


@app.context_processor
def inject_now():
    return {"now": datetime.utcnow}


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


class Apartment(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    number = db.Column(db.String(20), unique=True, nullable=False)
    floor = db.Column(db.Integer, nullable=False)
    area = db.Column(db.Float, nullable=False)
    owner_user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    owner = db.relationship("User", backref="apartments")
    credit_balance = db.Column(db.Float, nullable=False, default=0)


class Tariff(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False)
    type = db.Column(db.String(20), nullable=False)  # per_m2 | fixed
    amount = db.Column(db.Float, nullable=False)
    is_active = db.Column(db.Boolean, nullable=False, default=True)


class TariffApartment(db.Model):
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
    amount = db.Column(db.Float, nullable=False)
    paid_amount = db.Column(db.Float, nullable=False, default=0)
    status = db.Column(db.String(20), nullable=False, default="gozlemede")
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class Payment(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    invoice_id = db.Column(db.Integer, db.ForeignKey("invoice.id"), nullable=False)
    invoice = db.relationship("Invoice", backref="payments")
    amount = db.Column(db.Float, nullable=False)
    comment = db.Column(db.String(255), nullable=True)
    status = db.Column(db.String(20), nullable=False, default="pending")  # pending | confirmed | rejected
    reviewer_user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=True)
    reviewed_at = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


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
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class Announcement(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(200), nullable=False)
    text = db.Column(db.Text, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class Poll(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(200), nullable=False)
    is_anonymous = db.Column(db.Boolean, nullable=False, default=True)
    is_open = db.Column(db.Boolean, nullable=False, default=True)
    result_visibility = db.Column(db.String(20), nullable=False, default="immediate")
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class Vote(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    poll_id = db.Column(db.Integer, db.ForeignKey("poll.id"), nullable=False)
    apartment_id = db.Column(db.Integer, db.ForeignKey("apartment.id"), nullable=False)
    choice = db.Column(db.String(50), nullable=False)


class AuditLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    actor_user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=False)
    action = db.Column(db.String(255), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class ExpenseTemplate(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False)
    default_amount = db.Column(db.Float, nullable=False, default=0)
    is_recurring = db.Column(db.Boolean, nullable=False, default=True)
    is_active = db.Column(db.Boolean, nullable=False, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class Expense(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    period = db.Column(db.String(7), nullable=False)  # YYYY-MM
    name = db.Column(db.String(120), nullable=False)
    amount = db.Column(db.Float, nullable=False)
    is_paid = db.Column(db.Boolean, nullable=False, default=False)
    paid_at = db.Column(db.DateTime, nullable=True)
    template_id = db.Column(db.Integer, db.ForeignKey("expense_template.id"), nullable=True)
    template = db.relationship("ExpenseTemplate", backref="expenses")
    created_by_user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class BalanceTopUp(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    amount = db.Column(db.Float, nullable=False)
    comment = db.Column(db.String(255), nullable=True)
    created_by_user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


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


def _apply_payment_delta(invoice: "Invoice", delta: float) -> dict:
    """
    Apply a payment delta to an invoice/apartment.
    Positive delta increases paid; negative delta is a correction (decrease).
    Returns dict with keys: applied_to_invoice, moved_to_credit, removed_from_credit.
    """
    apt = invoice.apartment
    if not apt:
        invoice.paid_amount = round(float(invoice.paid_amount or 0) + float(delta), 2)
        invoice.paid_amount = max(0.0, invoice.paid_amount)
        invoice.status = "odenilib" if float(invoice.paid_amount or 0) >= float(invoice.amount or 0) else "gozlemede"
        return {"applied_to_invoice": round(delta, 2), "moved_to_credit": 0.0, "removed_from_credit": 0.0}

    delta = float(delta or 0)
    moved_to_credit = 0.0
    removed_from_credit = 0.0

    if delta >= 0:
        invoice.paid_amount = round(float(invoice.paid_amount or 0) + delta, 2)
        moved_to_credit = _move_invoice_overpay_to_credit(invoice)
    else:
        need = -delta
        credit = float(apt.credit_balance or 0)
        take_credit = min(credit, need)
        if take_credit > 0:
            apt.credit_balance = round(credit - take_credit, 2)
            removed_from_credit = round(take_credit, 2)
            need -= take_credit

        if need > 0:
            invoice.paid_amount = round(float(invoice.paid_amount or 0) - need, 2)
            if invoice.paid_amount < 0:
                invoice.paid_amount = 0.0
        invoice.status = "odenilib" if float(invoice.paid_amount or 0) >= float(invoice.amount or 0) else "gozlemede"

    applied_to_invoice = round(float(delta) + float(removed_from_credit) - float(moved_to_credit), 2)
    return {
        "applied_to_invoice": applied_to_invoice,
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
            conn.exec_driver_sql("ALTER TABLE apartment ADD COLUMN credit_balance FLOAT NOT NULL DEFAULT 0")


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


@app.before_request
def _run_role_migration_once():
    global _did_role_migration
    if _did_role_migration:
        return
    try:
        ensure_user_role_migration()
    finally:
        _did_role_migration = True


def ensure_expense_schema():
    # Create new tables for expenses if missing.
    with db.engine.connect() as conn:
        tables = {row[0] for row in conn.exec_driver_sql("SELECT name FROM sqlite_master WHERE type='table'")}
        if "expense_template" not in tables or "expense" not in tables:
            db.create_all()
            return

        columns = {row[1] for row in conn.exec_driver_sql("PRAGMA table_info(expense)")}
        if "is_paid" not in columns:
            conn.exec_driver_sql("ALTER TABLE expense ADD COLUMN is_paid BOOLEAN NOT NULL DEFAULT 1")
        if "paid_at" not in columns:
            conn.exec_driver_sql("ALTER TABLE expense ADD COLUMN paid_at DATETIME")

        # New rows should default to unpaid; keep legacy rows paid.
        conn.exec_driver_sql("UPDATE expense SET is_paid=0 WHERE is_paid IS NULL")

def ensure_balance_schema():
    with db.engine.connect() as conn:
        tables = {row[0] for row in conn.exec_driver_sql("SELECT name FROM sqlite_master WHERE type='table'")}
        if "balance_top_up" not in tables:
            db.create_all()


def ensure_tariff_scope_schema():
    with db.engine.connect() as conn:
        tables = {row[0] for row in conn.exec_driver_sql("SELECT name FROM sqlite_master WHERE type='table'")}
        if "tariff_apartment" not in tables:
            db.create_all()


def compute_invoice_amount(apartment, active_tariffs, scope_map):
    total = 0.0
    for t in active_tariffs:
        scope = scope_map.get(t.id)
        if scope is not None and apartment.id not in scope:
            continue
        total += t.amount * apartment.area if t.type == "per_m2" else t.amount
    return round(total, 2)


def get_smtp_config():
    cfg = SmtpConfig.query.first()
    if not cfg:
        cfg = SmtpConfig()
        db.session.add(cfg)
        db.session.commit()
    return cfg


def send_email(subject, body, recipients):
    cfg = get_smtp_config()
    if not cfg.host or not cfg.sender_email or not recipients:
        return False
    try:
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = cfg.sender_email
        msg["To"] = ", ".join(recipients)
        msg.set_content(body)

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


def can_view_poll_results(user, poll_obj, voted):
    if user.role in ("komendant", "superadmin"):
        return True
    if poll_obj.result_visibility == "immediate":
        return True
    if poll_obj.result_visibility == "after_vote":
        return voted
    if poll_obj.result_visibility == "after_close":
        return not poll_obj.is_open
    return False


def current_user():
    user_id = session.get("user_id")
    if not user_id:
        return None
    return User.query.get(user_id)


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
        email = request.form["email"].strip().lower()
        password = request.form["password"]
        user = User.query.filter_by(email=email).first()
        if user and check_password_hash(user.password_hash, password):
            session["user_id"] = user.id
            session["role"] = user.role
            session.pop("selected_apartment_id", None)
            return redirect(url_for("dashboard"))
        flash("Email ve ya sifre yanlisdir.", "danger")
    return render_template("login.html")


@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        full_name = request.form["full_name"].strip()
        phone = request.form.get("phone", "").strip()
        email = request.form["email"].strip().lower()
        password = request.form["password"]

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


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/dashboard")
@login_required
def dashboard():
    user = current_user()
    if user.role == "resident":
        apartment, apartments = get_selected_apartment(user)
        invoices = Invoice.query.filter_by(apartment_id=apartment.id).order_by(Invoice.created_at.desc()).all() if apartment else []
        # For resident view we show both debt and credit (overpayment).
        base_debt = sum((i.amount - i.paid_amount) for i in invoices)
        credit_balance = float(apartment.credit_balance or 0) if apartment else 0.0
        debt = round(float(base_debt) - credit_balance, 2)
        # Building-wide metrics (read-only for residents).
        debt_expr = db.case((Invoice.amount - Invoice.paid_amount > 0, Invoice.amount - Invoice.paid_amount), else_=0.0)
        house_total_debt = db.session.query(db.func.sum(debt_expr)).scalar() or 0
        house_credit_total = db.session.query(db.func.sum(Apartment.credit_balance)).scalar() or 0
        house_total_debt = max(0.0, float(house_total_debt or 0) - float(house_credit_total or 0))
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
        polls = Poll.query.filter_by(is_open=True).order_by(Poll.created_at.desc()).all()
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
            polls=polls,
            payment_history=payment_history,
        )

    from_date = request.args.get("from_date")
    to_date = request.args.get("to_date")
    from_dt = datetime.strptime(from_date, "%Y-%m-%d") if from_date else datetime(date.today().year, date.today().month, 1)
    to_dt = datetime.strptime(to_date, "%Y-%m-%d") if to_date else datetime.utcnow()
    apartments_count = Apartment.query.count()
    # Total debt should not be reduced by overpayments (credit) inside invoices,
    # but should be reduced by apartment credit balances.
    debt_expr = db.case((Invoice.amount - Invoice.paid_amount > 0, Invoice.amount - Invoice.paid_amount), else_=0.0)
    debt = db.session.query(db.func.sum(debt_expr)).scalar() or 0
    house_credit_total = db.session.query(db.func.sum(Apartment.credit_balance)).scalar() or 0
    debt = max(0.0, float(debt or 0) - float(house_credit_total or 0))
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
        v = max(0.0, float(inv_debt_sum or 0) - float(credit_bal or 0))
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
    income_period = (
        db.session.query(db.func.sum(Payment.amount))
        .filter(Payment.status == "confirmed", Payment.created_at >= from_dt, Payment.created_at <= to_dt)
        .scalar()
        or 0
    )
    topup_period = db.session.query(db.func.sum(BalanceTopUp.amount)).filter(BalanceTopUp.created_at >= from_dt, BalanceTopUp.created_at <= to_dt).scalar() or 0
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
        expenses_period=round(float(expenses_period or 0), 2),
        paid_expenses_total=round(float(paid_expenses_total or 0), 2),
        unpaid_expenses_total=round(float(unpaid_expenses_total or 0), 2),
        income_period=round(float(income_period or 0) + float(topup_period or 0), 2),
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
        floor = int(request.form["floor"])
        area = float(request.form["area"])
        owner_user_id = int(request.form["owner_user_id"])
        db.session.add(Apartment(number=number, floor=floor, area=area, owner_user_id=owner_user_id))
        db.session.commit()
        audit(f"Menzil yaradildi {number}")
        flash("Menzil elave edildi.", "success")
        return redirect(url_for("admin_apartments"))

    apartments = Apartment.query.order_by(Apartment.number).all()
    residents = User.query.filter_by(role="resident").all()
    debt_rows = (
        db.session.query(Invoice.apartment_id, db.func.sum(Invoice.amount - Invoice.paid_amount))
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
        debt_by_apartment_id=debt_by_apartment_id,
    )


@app.route("/admin/apartments/delete/<int:apartment_id>", methods=["POST"])
@login_required
@role_required("komendant", "superadmin")
def delete_apartment(apartment_id):
    apartment = Apartment.query.get_or_404(apartment_id)
    has_invoices = Invoice.query.filter_by(apartment_id=apartment.id).first() is not None
    has_votes = Vote.query.filter_by(apartment_id=apartment.id).first() is not None
    if has_invoices or has_votes:
        flash("Menzili silmek olmur: bagli hesab ve ya sesler var.", "warning")
        return redirect(url_for("admin_apartments"))

    apartment_number = apartment.number
    db.session.delete(apartment)
    db.session.commit()
    audit(f"Menzil silindi {apartment_number}")
    flash("Menzil silindi.", "success")
    return redirect(url_for("admin_apartments"))


@app.route("/admin/apartments/update/<int:apartment_id>", methods=["POST"])
@login_required
@role_required("komendant", "superadmin")
def update_apartment(apartment_id):
    apartment = Apartment.query.get_or_404(apartment_id)
    number = request.form["number"].strip()
    floor = int(request.form["floor"])
    area = float(request.form["area"])
    owner_user_id = int(request.form["owner_user_id"])

    duplicate = Apartment.query.filter(Apartment.number == number, Apartment.id != apartment.id).first()
    if duplicate:
        flash("Bu nomre ile menzil artiq movcuddur.", "warning")
        return redirect(url_for("admin_apartments"))

    apartment.number = number
    apartment.floor = floor
    apartment.area = area
    apartment.owner_user_id = owner_user_id
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
    db.session.delete(tariff)
    db.session.commit()
    audit(f"Tarif silindi {tariff_name}")
    flash("Tarif silindi.", "success")
    return redirect(url_for("admin_tariffs"))


@app.route("/admin/invoices/generate")
@login_required
@role_required("komendant", "superadmin")
def generate_invoices():
    period = date.today().strftime("%Y-%m")
    active_tariffs = Tariff.query.filter_by(is_active=True).all()
    apartments = Apartment.query.all()
    created = 0

    scope_rows = TariffApartment.query.all()
    scope_map = {}
    for r in scope_rows:
        scope_map.setdefault(r.tariff_id, set()).add(r.apartment_id)

    for apartment in apartments:
        exists = Invoice.query.filter_by(apartment_id=apartment.id, period=period).first()
        if exists:
            continue
        total = compute_invoice_amount(apartment, active_tariffs, scope_map)
        db.session.add(Invoice(apartment_id=apartment.id, period=period, amount=total, status="gozlemede"))
        created += 1

    db.session.commit()

    # Auto-apply apartment credit to newly created invoices.
    if created:
        new_invoices = (
            Invoice.query.join(Apartment, Invoice.apartment_id == Apartment.id)
            .filter(Invoice.period == period)
            .order_by(Apartment.number.asc(), Invoice.id.asc())
            .all()
        )
        applied_total = 0.0
        for inv in new_invoices:
            applied_total += _apply_credit_to_invoice(inv)
        if applied_total > 0:
            db.session.commit()
            audit(f"Kredit avtomatik tetbiq olundu: {applied_total:.2f} AZN period {period}")

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

    audit(f"Hesablar yaradildi: {created} period {period}")
    flash(f"Hesablar yaradildi: {created} eded.", "success")
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
            default_amount = float(request.form.get("default_amount", "0") or 0)
            is_recurring = request.form.get("is_recurring") == "on"
            if not name:
                flash("Ad bos ola bilmez.", "danger")
                return redirect(url_for("admin_expenses"))
            db.session.add(
                ExpenseTemplate(
                    name=name,
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
        if form_type == "add_expense":
            period = (request.form.get("period", "") or "").strip()
            name = (request.form.get("name", "") or "").strip()
            amount = float(request.form.get("amount", "0") or 0)
            if not period or len(period) != 7:
                flash("Period duzgun deyil (YYYY-MM).", "danger")
                return redirect(url_for("admin_expenses"))
            if not name:
                flash("Ad bos ola bilmez.", "danger")
                return redirect(url_for("admin_expenses"))
            if amount <= 0:
                flash("Məbləğ sıfırdan böyük olmalıdır.", "danger")
                return redirect(url_for("admin_expenses"))
            db.session.add(Expense(period=period, name=name, amount=round(amount, 2), is_paid=False, paid_at=None, created_by_user_id=current_user().id))
            db.session.commit()
            audit(f"Xərc daxil edildi: {name} {amount:.2f} AZN period {period}")
            flash("Xərc daxil edildi.", "success")
            return redirect(url_for("admin_expenses"))

    period = request.args.get("period") or date.today().strftime("%Y-%m")
    templates = ExpenseTemplate.query.order_by(ExpenseTemplate.is_active.desc(), ExpenseTemplate.name.asc()).all()
    expenses = Expense.query.filter_by(period=period).order_by(Expense.created_at.desc()).all()
    template_expense_by_template_id = {e.template_id: e for e in expenses if e.template_id}
    total = round(sum(e.amount for e in expenses), 2)
    return render_template(
        "admin_expenses.html",
        templates=templates,
        expenses=expenses,
        period=period,
        expenses_total=total,
        template_expense_by_template_id=template_expense_by_template_id,
    )


@app.route("/admin/expenses/update/<int:expense_id>", methods=["POST"])
@login_required
@role_required("komendant", "superadmin")
def update_expense(expense_id):
    e = Expense.query.get_or_404(expense_id)
    period = (request.form.get("period", "") or "").strip()
    name = (request.form.get("name", "") or "").strip()
    amount = float(request.form.get("amount", "0") or 0)
    if not period or len(period) != 7:
        flash("Period duzgun deyil (YYYY-MM).", "danger")
        return redirect(url_for("admin_expenses", period=e.period))
    if not name:
        flash("Ad bos ola bilmez.", "danger")
        return redirect(url_for("admin_expenses", period=e.period))
    if amount <= 0:
        flash("Məbləğ sıfırdan böyük olmalıdır.", "danger")
        return redirect(url_for("admin_expenses", period=e.period))

    old = f"{e.period} {e.name} {float(e.amount):.2f}"
    e.period = period
    e.name = name
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
    e.paid_at = datetime.utcnow() if e.is_paid else None
    db.session.commit()
    audit(f"Xərc {'ödəndi' if e.is_paid else 'ödənilmədi'} #{e.id}: {e.name} {float(e.amount):.2f} period {e.period}")
    flash("Xərc statusu yeniləndi.", "success")
    return redirect(url_for("admin_expenses", period=e.period))


@app.route("/admin/invoices")
@login_required
@role_required("komendant", "superadmin")
def admin_invoices():
    invoices = Invoice.query.order_by(Invoice.created_at.desc()).all()
    return render_template("admin_invoices.html", invoices=invoices)


@app.route("/admin/payments/confirm/<int:payment_id>", methods=["POST"])
@login_required
@role_required("komendant", "superadmin")
def confirm_payment(payment_id):
    payment = Payment.query.get_or_404(payment_id)
    if payment.status != "pending":
        flash("Bu muraciet artiq emal olunub.", "warning")
        return redirect(url_for("admin_invoices"))

    invoice = payment.invoice
    apply_amount = float(payment.amount)
    result = _apply_payment_delta(invoice, apply_amount)
    payment.status = "confirmed"
    payment.reviewer_user_id = current_user().id
    payment.reviewed_at = datetime.utcnow()
    db.session.commit()
    moved = float(result.get("moved_to_credit") or 0)
    removed = float(result.get("removed_from_credit") or 0)
    if moved > 0:
        audit(f"Odenis tesdiqlendi #{payment.id} {apply_amount:.2f} AZN (kredit +{moved:.2f})")
    elif removed > 0:
        audit(f"Odenis tesdiqlendi #{payment.id} {apply_amount:.2f} AZN (kredit -{removed:.2f})")
    else:
        audit(f"Odenis tesdiqlendi #{payment.id} {apply_amount:.2f} AZN")
    flash("Odenis tesdiqlendi.", "success")
    return redirect(url_for("admin_invoices"))


@app.route("/admin/payments/reject/<int:payment_id>", methods=["POST"])
@login_required
@role_required("komendant", "superadmin")
def reject_payment(payment_id):
    payment = Payment.query.get_or_404(payment_id)
    if payment.status != "pending":
        flash("Bu muraciet artiq emal olunub.", "warning")
        return redirect(url_for("admin_invoices"))

    payment.status = "rejected"
    payment.reviewer_user_id = current_user().id
    payment.reviewed_at = datetime.utcnow()
    db.session.commit()
    audit(f"Odenis imtina edildi #{payment.id}")
    flash("Odenis muracieti imtina edildi.", "warning")
    return redirect(url_for("admin_invoices"))

@app.route("/admin/payments/add/<int:invoice_id>", methods=["POST"])
@login_required
@role_required("komendant", "superadmin")
def add_payment(invoice_id):
    invoice = Invoice.query.get_or_404(invoice_id)
    try:
        amount = float(request.form["amount"])
    except (TypeError, ValueError):
        flash("Məbləğ düzgün deyil.", "danger")
        return redirect(url_for("admin_invoices"))
    comment = (request.form.get("comment", "") or "").strip() or None

    if amount == 0:
        flash("Məbləğ sıfır ola bilməz.", "danger")
        return redirect(url_for("admin_invoices"))

    apply_amount = amount
    now = datetime.utcnow()
    result = _apply_payment_delta(invoice, apply_amount)
    db.session.add(
        Payment(
            invoice_id=invoice.id,
            amount=apply_amount,
            comment=comment,
            status="confirmed",
            reviewer_user_id=current_user().id,
            reviewed_at=now,
            created_at=now,
        )
    )
    db.session.commit()
    moved = float(result.get("moved_to_credit") or 0)
    removed = float(result.get("removed_from_credit") or 0)
    if moved > 0:
        audit(f"Odenis daxil edildi invoice#{invoice.id} {apply_amount:.2f} AZN (kredit +{moved:.2f})")
    elif removed > 0:
        audit(f"Odenis daxil edildi invoice#{invoice.id} {apply_amount:.2f} AZN (kredit -{removed:.2f})")
    else:
        audit(f"Odenis daxil edildi invoice#{invoice.id} {apply_amount:.2f} AZN")
    flash("Odenis daxil edildi.", "success")
    return redirect(url_for("admin_invoices"))


@app.route("/admin/history")
@login_required
@role_required("komendant", "superadmin")
def admin_history():
    # Unified history: payments (confirmed), balance top-ups, expenses.
    limit = 300

    confirmed_payments = (
        Payment.query.join(Invoice, Payment.invoice_id == Invoice.id)
        .join(Apartment, Invoice.apartment_id == Apartment.id)
        .filter(Payment.status == "confirmed")
        .order_by(Payment.created_at.desc())
        .limit(limit)
        .all()
    )
    topups = BalanceTopUp.query.order_by(BalanceTopUp.created_at.desc()).limit(limit).all()
    expenses = Expense.query.order_by(Expense.created_at.desc()).limit(limit).all()

    events = []
    for p in confirmed_payments:
        inv = p.invoice
        events.append(
            {
                "dt": p.created_at,
                "type": "payment",
                "amount": float(p.amount),
                "apartment": inv.apartment.number if inv and inv.apartment else None,
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
    return render_template("admin_history.html", events=events)


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
    if len(from_period) != 7 or len(to_period) != 7:
        flash("Period duzgun deyil (YYYY-MM).", "danger")
        return redirect(url_for("dashboard"))

    periods = list(_iter_months_inclusive(from_period, to_period))
    if len(periods) > 24:
        periods = periods[-24:]
        from_period = periods[0]

    apartments = Apartment.query.order_by(Apartment.number).all()
    sums = (
        db.session.query(Apartment.id, Invoice.period, db.func.sum(Payment.amount))
        .join(Invoice, Invoice.apartment_id == Apartment.id)
        .join(Payment, Payment.invoice_id == Invoice.id)
        .filter(Payment.status == "confirmed", Invoice.period >= from_period, Invoice.period <= to_period)
        .group_by(Apartment.id, Invoice.period)
        .all()
    )
    amount_by_apt_period = {(apt_id, period): float(total or 0) for apt_id, period, total in sums}

    rows = []
    for a in apartments:
        row = {"apartment": a.number, "amounts": [], "row_total": 0.0}
        for p in periods:
            v = amount_by_apt_period.get((a.id, p), 0.0)
            row["amounts"].append(v)
            row["row_total"] += float(v)
        row["row_total"] = round(row["row_total"], 2)
        rows.append(row)

    col_totals = []
    for idx in range(len(periods)):
        col_totals.append(round(sum(r["amounts"][idx] for r in rows), 2))
    grand_total = round(sum(col_totals), 2)

    return render_template(
        "admin_payments_report.html",
        from_period=from_period,
        to_period=to_period,
        periods=periods,
        rows=rows,
        col_totals=col_totals,
        grand_total=grand_total,
    )


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
        if request.form["form_type"] == "work":
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
        else:
            db.session.add(Announcement(title=request.form["title"].strip(), text=request.form["text"].strip()))
            audit("Elan elave edildi")
            flash("Elan elave edildi.", "success")
            sysname = (get_smtp_config().system_name or "").strip() or "eMTK"
            notify_residents(f"{sysname}: Yeni elan", f"Yeni elan: {request.form['title'].strip()}")
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
    if role == "commandant":
        role = "komendant"
    if role not in ("resident", "komendant", "superadmin"):
        flash("Rol duzgun deyil.", "danger")
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
    if role == "commandant":
        role = "komendant"
    if role not in ("resident", "komendant", "superadmin"):
        flash("Rol duzgun deyil.", "danger")
        return redirect(url_for("admin_users"))
    duplicate = User.query.filter(User.email == email, User.id != target.id).first()
    if duplicate:
        flash("Bu email başqa istifadəçidə var.", "warning")
        return redirect(url_for("admin_users"))

    # Prevent self-demoting to resident (locks admin out).
    me = current_user()
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
    user = current_user()
    apartment = Apartment.query.filter_by(owner_user_id=user.id).first() if user.role == "resident" else None

    if request.method == "POST":
        if user.role in ("komendant", "superadmin") and request.form["form_type"] == "create_poll":
            db.session.add(
                Poll(
                    title=request.form["title"].strip(),
                    is_anonymous=request.form.get("is_anonymous") == "on",
                    is_open=True,
                    result_visibility=request.form.get("result_visibility", "immediate"),
                )
            )
            db.session.commit()
            audit("Sorgu yaradildi")
            flash("Sorgu yaradildi.", "success")
            sysname = (get_smtp_config().system_name or "").strip() or "eMTK"
            notify_residents(f"{sysname}: Yeni sorgu", f"Yeni sorgu yaradildi: {request.form['title'].strip()}")
        elif user.role in ("komendant", "superadmin") and request.form["form_type"] == "toggle_poll_status":
            poll_id = int(request.form["poll_id"])
            poll = Poll.query.get_or_404(poll_id)
            poll.is_open = not poll.is_open
            db.session.commit()
            audit(f"{'Acilib' if poll.is_open else 'Baglanib'} sorgu #{poll_id}")
            flash("Sorgunun statusu yenilendi.", "success")
        elif user.role == "resident":
            poll_id = int(request.form["poll_id"])
            choice = request.form["choice"]
            existing = Vote.query.filter_by(poll_id=poll_id, apartment_id=apartment.id).first()
            if existing:
                flash("Bu sorguda artiq ses vermisiniz.", "warning")
            else:
                db.session.add(Vote(poll_id=poll_id, apartment_id=apartment.id, choice=choice))
                db.session.commit()
                flash("Sizin sesiniz qeyde alindi.", "success")
        return redirect(url_for("polls"))

    poll_rows = Poll.query.order_by(Poll.created_at.desc()).all()
    poll_data = []
    for p in poll_rows:
        yes_votes = Vote.query.filter_by(poll_id=p.id, choice="yes").count()
        no_votes = Vote.query.filter_by(poll_id=p.id, choice="no").count()
        voted = False
        user_choice = None
        if apartment:
            vote = Vote.query.filter_by(poll_id=p.id, apartment_id=apartment.id).first()
            voted = vote is not None
            user_choice = vote.choice if vote else None
        poll_data.append(
            {
                "poll": p,
                "yes": yes_votes,
                "no": no_votes,
                "voted": voted,
                "user_choice": user_choice,
                "can_view_results": can_view_poll_results(user, p, voted),
            }
        )

    return render_template("polls.html", poll_data=poll_data, user=user)


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
    body = "\n".join(body_lines) + "\n"
    if send_email(subject, body, [resident.email]):
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
    debt = max(0.0, round(invoice.amount - invoice.paid_amount, 2))
    return render_template(
        "invoice_print.html",
        invoice=invoice,
        resident=invoice.apartment.owner,
        cfg=cfg,
        system_name=(cfg.system_name or "").strip() or "eMTK",
        debt=debt,
        issue_date=datetime.utcnow(),
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
            cfg.password = request.form.get("password", "").strip() or None
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
        return redirect(url_for("admin_settings"))
    return render_template("admin_settings.html", cfg=cfg)


@app.route("/init")
def init_data():
    if os.getenv("ENABLE_INIT_ROUTE", "0") != "1":
        abort(404)
    db.create_all()
    if User.query.count() == 0:
        superadmin = User(
            full_name="Суперадмин",
            phone="+000000",
            email="admin@smartzhk.local",
            password_hash=generate_password_hash("admin123"),
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
        ensure_poll_schema()
        ensure_payment_schema()
        ensure_apartment_schema()
        ensure_system_schema()
        ensure_expense_schema()
        ensure_balance_schema()
        ensure_tariff_scope_schema()
        ensure_user_role_migration()
        get_smtp_config()
    # Fail fast in production if SECRET_KEY is not set.
    if os.getenv("FLASK_DEBUG", "0") != "1" and app.config["SECRET_KEY"] == "change-this-in-production":
        raise RuntimeError("SECRET_KEY must be set in production.")
    host = os.getenv("FLASK_HOST", "0.0.0.0")
    port = int(os.getenv("FLASK_PORT", "5000"))
    debug = os.getenv("FLASK_DEBUG", "1") == "1"
    app.run(host=host, port=port, debug=debug)
