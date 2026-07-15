# DNX Store: пошаговое обновление

## 1. Сделайте резервную копию

1. Откройте `https://github.com/denixl-11/dnx-store`.
2. Нажмите **Code** → **Download ZIP** и сохраните архив.
3. В Neon откройте нужный проект и убедитесь, что выбрана правильная production-ветка БД. Код сам создаст недостающие таблицы, колонки и индексы; существующие данные не удаляются.

## 2. Получите пулированную строку Neon

1. Откройте Neon Console → ваш проект.
2. На Dashboard нажмите **Connect**.
3. Выберите базу `neondb` и владельца `neondb_owner`.
4. Включите **Connection pooling**. В hostname появится `-pooler`.
5. Скопируйте всю строку вида `postgresql://...-pooler.../neondb?sslmode=require...`.
6. Никуда не вставляйте её в код и не публикуйте в GitHub.

## 3. Замените файлы в GitHub

Без команной строки:

1. Откройте репозиторий и нажмите **Add file** → **Upload files**.
2. Перетащите: `index.html`, `main.py`, `requirements.txt`, `.python-version`, `.gitignore`, `README.md`, `DEPLOYMENT_RU.md`.
3. GitHub покажет, что файлы с такими именами будут заменены.
4. В **Commit message** введите `Security and async database update`.
5. Выберите **Commit directly to the main branch** и нажмите **Commit changes**.
6. Не загружайте `.env`, `.venv`, архивы и файлы с паролями.

## 4. Проверьте GitHub Pages

1. В репозитории откройте **Settings** → **Pages**.
2. В **Build and deployment** выберите **Deploy from a branch**.
3. Выберите branch `main`, folder `/ (root)` и нажмите **Save**.
4. Дождитесь зелёного статуса. URL должен остать `https://denixl-11.github.io/dnx-store/`.

## 5. Настройте Render Environment

1. Откройте Render Dashboard → сервис `dnx-shop`.
2. В левом меню нажмите **Environment**.
3. Нажимайте **+ Add Environment Variable** и добавьте:

| Key | Value |
|---|---|
| `BOT_TOKEN` | Текущий токен BotFather |
| `ADMIN_ID` | Ваш числовой Telegram ID |
| `DATABASE_URL` | Полная pooled-строка из Neon |
| `PAYMENT_REQUISITES` | Реквизиты, которые видит клиент |
| `CASES_JSON` | Ваш текущий JSON кейсов без изменений |
| `WEBAPP_URL` | `https://denixl-11.github.io/dnx-store/` |
| `CORS_ORIGIN` | `https://denixl-11.github.io` |
| `DB_POOL_MIN_SIZE` | `1` |
| `DB_POOL_MAX_SIZE` | `10` |
| `INIT_DATA_MAX_AGE` | `86400` |
| `PYTHON_VERSION` | `3.14.3` |

`DB_PASSWORD` можно оставить на время перехода, но при наличии `DATABASE_URL` код использует именно его. После успешного запуска старый `DB_PASSWORD` можно удалить.

4. Нажмите **Save, rebuild, and deploy**.

## 6. Проверьте Render Settings

1. Откройте вкладку **Settings** сервиса.
2. Build Command: `pip install -r requirements.txt`.
3. Start Command: `python main.py`.
4. Health Check Path: `/health`.
5. Важно: для этого проекта оставьте **один Render instance**. Текущее состояние игрового раунда хранится в памяти одного процесса; второй instance создаст отдельную игру и второй polling бота.
6. Если deploy не начался, откройте **Events** → **Manual Deploy** → **Clear build cache & deploy**.

## 7. Проверка после deploy

1. Откройте `https://dnx-shop.onrender.com/health`. Ожидаемый ответ: `{"status":"ok"}`.
2. В Render откройте **Logs**. Не должно быть `DB Init Error`, `invalid_signature` при обычном открытии из Telegram или ошибок polling.
3. Откройте бота в Telegram → **Магазин**. Не тестируйте защищённые API простым открытием GitHub Pages в обычном браузере: там нет подписанного Telegram `initData`.
4. Проверьте по очереди: баланс; каталог; покупку одного тестового NFT; инвентарь; открытие одного дешёвого кейса; продажу выпавшего предмета; ставку двумя тестовыми аккаунтами; золотую подсветку победителя.
5. После покупки или кейса сверьте три вещи: баланс списался один раз, предмет один, в инвентаре нет дубля.

## 8. Если что-то не работает

- `invalid_signature`: закройте Mini App полностью и откройте её заново из кнопки бота; сверьте `BOT_TOKEN` в Render с токеном того же бота.
- `Database pool is not initialized` или `DB Init Error`: проверьте `DATABASE_URL`, пароль, ветку Neon и `sslmode=require`.
- Build пытается установить `psycopg2`: на Render ещё старый `requirements.txt`; повторите upload и **Clear build cache & deploy**.
- GitHub Pages обновился, Render нет: откройте Render **Events** → **Manual Deploy** → **Deploy latest commit**.
