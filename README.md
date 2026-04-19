# eMTK

`Flask + SQLAlchemy + SQLite + Bootstrap 5` üzərində qurulmuş yaşayış kompleksi idarəetmə tətbiqi.

Layihə MTK / bina idarəçiliyinin əsas proseslərini əhatə edir: istifadəçilər və mənzillər, tariflər, hesab-fakturalar, ödənişlər, xərclər, hesabatlar, email və WhatsApp bildirişləri, həmçinin verilənlər bazası ilə bağlı inzibati əməliyyatlar.

## İmkanlar

- Rollar və autentifikasiya: `resident`, `komendant`, `admin`
- Sakin qeydiyyatı və mənzil təyinatı
- Korpus / bina dəstəyi
- Mənzil, sakin və tarif reyestri
- İki növ tarif: `per_m2` və `fixed`
- Aylıq hesab-fakturaların yaradılması və yenidən hesablanması
- Qismən ödəniş, artıq ödəniş və mənzil kredit balansının uçotu
- Bina üzrə xərclərin idarə olunması
- Sakin və komendant üçün ayrıca dashboard-lar
- Ödəniş tarixçəsi, maliyyə tarixçəsi və audit log
- `xlsx` ixracı və çap üçün səhifələr
- SMTP üzərindən email bildirişləri
- WhatsApp bildirişləri üçün `Evolution API` inteqrasiyası
- Şəkil yükləmə dəstəyi ilə görülən işlər jurnalı
- Elanlar, sorğular və səsvermə
- Admin paneldən SQLite backup export/import
- Maliyyə sxemi və hesablamalar üçün daxili health-check marşrutları

## Texnologiyalar

- Backend: `Flask`
- ORM: `Flask-SQLAlchemy`
- Formlar / CSRF: `Flask-WTF`
- Rate limiting: `Flask-Limiter`
- Excel export: `openpyxl`
- HTTP inteqrasiyaları: `requests`
- Verilənlər bazası: standart olaraq `SQLite`
- Frontend: server-rendered `Jinja2` + `Bootstrap 5`

## Layihə strukturu

```text
app.py                 əsas Flask tətbiqi, modellər, marşrutlar, daxili miqrasiyalar
templates/             HTML şablonları
static/styles.css      xüsusi stillər
static/uploads/        yüklənən şəkillər
requirements.txt       Python asılılıqları
Dockerfile             tətbiq imici
docker-compose.yml     lokal konteyner işə salınması
```

## Mühit dəyişənləri

Əsas konfiqurasiya:

- `SECRET_KEY` - production üçün vacibdir
- `DATABASE_URL` - SQLAlchemy bağlantı sətri
- `FLASK_HOST` - standart olaraq `0.0.0.0`
- `FLASK_PORT` - standart olaraq `5000`
- `FLASK_DEBUG` - lokal inkişaf üçün `1`
- `TZ` - standart olaraq `Asia/Baku`
- `SESSION_COOKIE_SECURE` - lokal inkişafda adətən `0`
- `SESSION_COOKIE_SAMESITE` - standart olaraq `Lax`
- `PREFERRED_URL_SCHEME` - reverse proxy arxasında adətən `https`
- `TRUSTED_PROXIES` - etibar edilən reverse proxy sayı

Docker olmadan lokal işə salmaq üçün tövsiyə olunan dəyərlər:

```powershell
$env:SECRET_KEY="dev-secret"
$env:FLASK_DEBUG="1"
$env:SESSION_COOKIE_SECURE="0"
$env:PREFERRED_URL_SCHEME="http"
$env:DATABASE_URL="sqlite:///instance/emtk.db"
```

## Lokal işə salınma

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python app.py
```

Tətbiq [http://127.0.0.1:5000](http://127.0.0.1:5000) ünvanında açılacaq.

İlk işə salınmada tətbiq:

- cədvəlləri avtomatik yaradır
- daxili idempotent miqrasiyaları tətbiq edir
- bazada `admin` yoxdursa bootstrap admin yaradır

## Docker ilə işə salınma

```bash
docker compose up --build
```

`docker-compose.yml` aşağıdakı qovluqları mount edir:

- `./instance -> /app/instance` SQLite verilənlər bazası üçün
- `./static/uploads -> /app/static/uploads` istifadəçi faylları üçün

İşə salmazdan əvvəl həm `SECRET_KEY`, həm `SESSION_COOKIE_SECURE` ötürülməlidir
(məsələn `.env` faylı vasitəsilə). Dəyərlər təyin olunmasa, `docker compose` açıq
xəta ilə bitir və prod sessiyaları təsadüfən insecure cookie ilə başlamır.

Nümunə `.env`:

```
SECRET_KEY=change-me-to-a-long-random-string
# HTTPS arxasında 1, lokal HTTP üçün 0
SESSION_COOKIE_SECURE=1
```

## Standart giriş məlumatları

### Avtomatik bootstrap admin

Baza boş olduqda tətbiq avtomatik olaraq bu istifadəçini yaradır:

- `admin@emtk.itg.az` / `admin`

Bu istifadəçi əlavə marşrut çağırmadan tətbiqin startında yaradılır.

## Əsas bölmələr

- `/login`, `/register`, `/change-password`
- `/dashboard` - roldan asılı olaraq sakin və ya admin paneli
- `/admin/apartments` - mənzillər
- `/admin/users` - istifadəçilər
- `/admin/tariffs` - tariflər
- `/admin/invoices` - hesablar və ödənişlər
- `/admin/expenses` - xərclər
- `/admin/history` - əməliyyat tarixçəsi
- `/admin/payments-report` - ödənişlər üzrə yekun hesabat
- `/admin/content` - işlər və elanlar
- `/admin/settings` - sistem, SMTP, WhatsApp və DB ayarları
- `/admin/whatsapp/logs` - WhatsApp webhook logları

## Email və WhatsApp bildirişləri

Admin paneldə aşağıdakılar mövcuddur:

- SMTP ayarlarının idarəsi və test email göndərişi
- WhatsApp bildirişləri üçün `Evolution API` ayarları
- test WhatsApp mesajı göndərilməsi
- hesab-fakturalar üzrə təkli və toplu bildiriş göndərişi
- webhook marşrutu: `/whatsapp/webhook`

WhatsApp hissəsi `Evolution API` üzərindən işləyir və aşağıdakı ssenariləri dəstəkləyir:

- sakinlərə hesab-faktura bildirişlərinin göndərilməsi
- toplu WhatsApp növbəsi və rate-limit məntiqi
- webhook vasitəsilə istifadəçi telefonunun uyğunlaşdırılması
- sakinin WhatsApp bildirişlərinə qoşulma axını
- webhook diaqnostika loglarının saxlanması

## Verilənlər bazası və miqrasiyalar

Layihədə hələlik `Alembic` kimi ayrıca migration tooling yoxdur. Bunun əvəzinə `app.py` daxilində daxili sxem yoxlamaları və idempotent miqrasiyalar tətbiq olunur; bunlar həm start zamanı, həm də sorğular zamanı çağırılır.

SQLite üçün admin paneldə bunlar mövcuddur:

- bazanın export edilməsi
- backup fayldan import
- maliyyə məlumatlarının tam sıfırlanması

## Diaqnostika

Layihədə aşağıdakı diaqnostik marşrutlar var:

- `/admin/health/money-schema`
- `/admin/health/calculation-smoke`

Bu marşrutlar admin rolları üçün açıqdır və maliyyə sxeminin, eləcə də əsas hesablamaların düzgünlüyünü yoxlamağa kömək edir.

## Asılılıqlar

`requirements.txt` daxilində əsas paketlər:

- `Flask`
- `Flask-SQLAlchemy`
- `Flask-WTF`
- `Flask-Limiter`
- `openpyxl`
- `requests`

## Cari məhdudiyyətlər

- Əsas biznes məntiqinin böyük hissəsi hələ də `app.py` daxilindədir
- Repozitoridə hazırda avtomatlaşdırılmış testlər yoxdur
- Standart işçi verilənlər bazası ssenarisi `SQLite` üzərində qurulub

