# DNX Store

Telegram Mini App магазина NFT-подарков: каталог, кейсы, инвентарь и игра.

## Состав

- `index.html` — фронтенд для GitHub Pages.
- `main.py` — Telegram-бот и HTTP API для Render.
- `requirements.txt` — минимальные Python-зависимости.
- `DEPLOYMENT_RU.md` — пошаговое развёртывание.

## Render

- Build Command: `pip install -r requirements.txt`
- Start Command: `python main.py`
- Health Check Path: `/health`

Все секреты задаются только в Render Environment. Полный список — в [DEPLOYMENT_RU.md](DEPLOYMENT_RU.md).

