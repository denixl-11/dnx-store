# DNX Store

Telegram Mini App магазина NFT-подарков: игра, каталог, кейсы и инвентарь. Интерфейс выполнен в тёмном purple/red neon стиле, расчётная валюта — TON.

## Что входит

- `index.html` — интерфейс GitHub Pages и TON Connect.
- `app-icon.png` — прежний знак приложения с неоновой обработкой для TON Connect.
- `tonconnect-manifest.json` — манифест подключения TON-кошельков.
- `terms.html`, `privacy.html` — публичные страницы для манифеста.
- `main.py` — Telegram-бот, асинхронный API, Neon PostgreSQL и автоматическая проверка TON-переводов.
- `DEPLOYMENT_RU.md` — подробная инструкция по обновлению.

## Render

- Build Command: `pip install -r requirements.txt`
- Start Command: `python main.py`
- Health Check Path: `/health`
- Instances: `1`

Секреты хранятся только в Render Environment. Seed-фраза и приватный ключ TON-кошелька приложению не нужны и никогда не должны добавляться в Render или GitHub.
