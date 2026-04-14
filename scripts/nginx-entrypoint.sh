#!/bin/sh
set -eu

DOMAIN="${LETSENCRYPT_DOMAIN:-}"
if [ -z "$DOMAIN" ]; then
  DOMAIN="$(ls -1 /etc/letsencrypt/live 2>/dev/null | grep -v '^README$' | head -n 1 || true)"
fi

if [ -n "$DOMAIN" ] && [ -f "/etc/letsencrypt/live/${DOMAIN}/fullchain.pem" ]; then
  export DOMAIN
  envsubst '${DOMAIN}' < /etc/nginx/templates/default.https.conf > /etc/nginx/conf.d/default.conf
else
  export DOMAIN="_"
  envsubst '${DOMAIN}' < /etc/nginx/templates/default.http.conf > /etc/nginx/conf.d/default.conf
fi

# Reload periodically so nginx picks renewed certificates.
(
  while true; do
    sleep 6h
    nginx -s reload || true
  done
) &

exec nginx -g "daemon off;"
