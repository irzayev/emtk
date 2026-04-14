#!/bin/sh
set -eu

DOMAIN="${1:-${LETSENCRYPT_DOMAIN:-}}"
EMAIL="${2:-${LETSENCRYPT_EMAIL:-}}"

if [ -z "$DOMAIN" ]; then
  DOMAIN="$(docker compose exec -T emtk python -c "import sqlite3; p='/app/instance/smart_zhk.db'; c=sqlite3.connect(p).cursor(); c.execute('SELECT domain FROM smtp_config WHERE domain IS NOT NULL AND TRIM(domain) != \"\" ORDER BY id DESC LIMIT 1'); r=c.fetchone(); print((r[0] if r and r[0] else '').strip())" 2>/dev/null || true)"
fi

if [ -z "$DOMAIN" ] || [ -z "$EMAIL" ]; then
  echo "Usage: DOMAIN EMAIL"
  echo "Example: ./scripts/certbot-init.sh emtk.itg.az admin@itg.az"
  echo "Tip: DOMAIN can be omitted if it is saved in Admin Settings -> Domain."
  exit 1
fi

docker compose run --rm certbot certonly \
  --webroot \
  -w /var/www/certbot \
  -d "$DOMAIN" \
  --email "$EMAIL" \
  --agree-tos \
  --no-eff-email

docker compose restart nginx
