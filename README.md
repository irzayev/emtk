MTK-портал управления ЖК на Flask + SQLite + Bootstrap.

## Что реализовано

- Авторизация и роли: `resident`, `commandant`, `superadmin`
- Реестр квартир
- Тарифы (`per_m2`, `fixed`)
- Генерация счетов за текущий месяц
- Частичная/полная оплата счетов
- Дашборды для жильца и коменданта
- Журнал работ и объявления
- Опросы и голосование (1 голос от квартиры)
- Аудит-лог административных действий

## Быстрый запуск

### Вариант 1: через Docker (рекомендуется)

```bash
cd emtk
docker compose up --build
```

После старта:
- `http://127.0.0.1:5000/init`
- `http://127.0.0.1:5000/login`

### HTTPS (Let's Encrypt) + автообновление

1. Убедитесь, что DNS `emtk.itg.az` указывает на IP сервера, и открыты порты `80` и `443`.
2. Задайте переменные окружения (можно в `.env` рядом с `docker-compose.yml`):

```env
SECRET_KEY=change-me
LETSENCRYPT_DOMAIN=emtk.itg.az
LETSENCRYPT_EMAIL=admin@itg.az
```

3. Запустите контейнеры:

```bash
docker compose up -d --build
```

4. Выпустите первый сертификат:

```bash
chmod +x ./scripts/certbot-init.sh
./scripts/certbot-init.sh emtk.itg.az admin@itg.az
```

Если домен уже сохранен в админке (Ayarlar -> Domen), можно не передавать его явно:

```bash
./scripts/certbot-init.sh "" admin@itg.az
```

После этого `nginx` автоматически переключится на HTTPS.  
Автопродление включено в сервисе `certbot` (проверка каждые 12 часов).

### Вариант 2: локально через Python

1. Открой терминал в папке проекта:

```bash
cd emtk
```

2. Установи зависимости:

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

3. Запусти приложение:

```bash
python app.py
```

4. Инициализируй тестовые данные:

- Открой в браузере: `http://127.0.0.1:5000/init`
- Потом: `http://127.0.0.1:5000/login`

## Тестовые аккаунты

- Суперадмин: `admin@smartzhk.local` / `admin123`
- Комендант: `commandant@smartzhk.local` / `commandant123`
- Жилец: `resident@smartzhk.local` / `resident123`

