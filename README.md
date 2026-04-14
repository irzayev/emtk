# Smart-ЖК MVP

Готовый MVP-портал управления ЖК на Flask + SQLite + Bootstrap.

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
cd smart-zhk-mvp
docker compose up --build
```

После старта:
- `http://127.0.0.1:5000/init`
- `http://127.0.0.1:5000/login`

### Вариант 2: локально через Python

1. Открой терминал в папке проекта:

```bash
cd smart-zhk-mvp
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

## Важно для production

- Замени `SECRET_KEY` в `app.py`
- Переключись на PostgreSQL
- Добавь HTTPS, резервное копирование и реальные платежные интеграции
