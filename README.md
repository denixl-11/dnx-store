# DNX Store

Telegram Mini App магазина NFT-подарков: игра, каталог, кейсы, инвентарь и история.

## Валюта и пополнение

- Внутренний баланс, цены, ставки и выигрыши — целые Telegram Stars.
- Прямая оплата — официальный Telegram invoice с валютой `XTR`.
- Альтернативное пополнение — TON Connect по фиксированному курсу `1 TON = 85 Stars`.
- Рубли отображаются как вариант `Скоро` и не имеют платёжной логики.

## Основные файлы

- `index.html` — интерфейс Mini App.
- `main.py` — Telegram-бот, API, Neon и обработчики платежей.
- `cosmic-bg.png` — общий фон.
- `game-waiting-bg.png` — оформление игрового поля ожидания.
- `history-empty-bg.png` — оформление пустой истории.
- `app-icon.png`, `tonconnect-manifest.json` — TON Connect.
- `DEPLOYMENT_RU.md` — подробная инструкция.

## Render

- Build Command: `pip install -r requirements.txt`
- Start Command: `python main.py`
- Health Check Path: `/health`
- Instances: `1`

Seed-фразы и приватные ключи проекту не нужны.
