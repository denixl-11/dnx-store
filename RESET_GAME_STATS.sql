-- 1. Остановите Web Service в Render перед выполнением этого файла.
-- 2. Запускайте файл в базе из DATABASE_URL именно этого Render-сервиса.

SELECT current_database() AS database_name, current_schema() AS schema_name;

SELECT
    (SELECT COUNT(*) FROM game_history) AS history_rows,
    (SELECT COUNT(*) FROM leaderboard) AS leaderboard_rows,
    (SELECT COUNT(*) FROM active_game_bets) AS wager_rows,
    (SELECT last_game_number FROM game_counter WHERE id = 1) AS game_number;

BEGIN;
DELETE FROM active_game_bets;
DELETE FROM game_history;
DELETE FROM leaderboard;
INSERT INTO game_counter (id, last_game_number)
VALUES (1, 0)
ON CONFLICT (id) DO UPDATE SET last_game_number = 0;
COMMIT;

-- После успешного COMMIT снова запустите сервис Render.
