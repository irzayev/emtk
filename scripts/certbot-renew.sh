#!/bin/sh
set -eu

while true; do
  certbot renew --webroot -w /var/www/certbot --quiet || true
  sleep 12h
done
