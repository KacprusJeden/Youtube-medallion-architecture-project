SELECT * FROM pg_stg_silver_current

CREATE TABLE pg_stg_silver_current_20260424 AS SELECT '2026-04-24'::DATE AS date_of_data, '4973e00f76df4f9eab1ec85700b85d6f' AS run_id, sc.* 
FROM pg_stg_silver_current sc

SELECT COUNT(DISTINCT HK_CHANNEL), COUNT(distinct hk_video), count(distinct hk_comment), 
COUNT(CASE WHEN hk_comment is null THEN hk_video end) 
FROM youtube_statistics_obt
GROUP BY hk_channel;

SELECT hk_channel, hk_video FROM youtube_statistics_obt WHERE hk_comment IS NULL;
SELECT * FROM youtube_num_statistics_obt WHERE video_comment_count = 0;
SELECT * FROM youtube_statistics_obt WHERE is_public IS FALSE;


SELECT COUNT(DISTINCT HK_CHANNEL), COUNT(distinct hk_video), count(distinct hk_comment), 
COUNT(CASE WHEN hk_comment is null THEN hk_video end) 
FROM youtube_statistics_obt
GROUP BY hk_channel;

 
WITH cte AS (
	SELECT hk_channel, COUNT(distinct hk_video) videos, 
	COUNT(distinct hk_comment) videos_with_comments,
	COUNT(CASE WHEN hk_comment is null THEN hk_video end) videos_without_comments,
	COUNT(hk_comment) + SUM(comment_reply_count) all_comments_from_table_add_number_replies
	FROM youtube_num_statistics_obt
	WHERE date_of_data = '2026-04-24'
	GROUP BY hk_channel
), cte2 AS 
(
	SELECT DISTINCT hk_channel, hk_video, video_count AS video_count_from_stats, video_comment_count AS video_comment_count_from_stats 
	FROM youtube_num_statistics_obt
	WHERE date_of_data = '2026-04-24'
), cte3 AS (
	SELECT cte.hk_channel, cte.videos, cte.videos_with_comments, cte.videos_without_comments, 
	cte.all_comments_from_table_add_number_replies,
	cte2.video_count_from_stats, cte2.video_comment_count_from_stats
	FROM cte INNER JOIN cte2 ON cte.hk_channel = cte2.hk_channel
)
SELECT hk_channel, ANY_VALUE(videos_with_comments), ANY_VALUE(all_comments_from_table_add_number_replies),
SUM(video_comment_count_from_stats)
FROM cte3
GROUP BY hk_channel;


WITH video_stats AS (
    -- Agregujemy dane do poziomu WIDEO, aby uniknąć duplikowania liczników przy joinach
    SELECT 
        hk_channel,
        hk_video,
        MAX(video_comment_count) as official_video_comment_count, -- licznik z API
        COUNT(hk_comment) as actual_comments_downloaded,           -- ile mamy wierszy
        SUM(COALESCE(comment_reply_count, 0)) as total_replies_not_downloaded -- odpowiedzi, których nie masz
    FROM youtube_num_statistics_obt
    WHERE date_of_data = '2026-04-24'
    GROUP BY hk_channel, hk_video
)
SELECT 
    hk_channel,
    COUNT(hk_video) as total_videos,
    -- Ile komentarzy faktycznie masz w bazie (wiersze)
    SUM(actual_comments_downloaded) as my_total_comment_rows,
    -- Ile komentarzy powinieneś mieć wg API (główne + odpowiedzi)
    SUM(official_video_comment_count) as api_reported_comment_count,
    -- Procentowe pokrycie (ile Twoich danych to realne komentarze)
    ROUND(SUM(actual_comments_downloaded)::NUMERIC / NULLIF(SUM(official_video_comment_count), 0) * 100, 2) as coverage_pct,
    -- Różnica wynikająca z niepobranych odpowiedzi
    SUM(official_video_comment_count) - (SUM(actual_comments_downloaded) + SUM(total_replies_not_downloaded)) as unexplained_diff
FROM video_stats
GROUP BY hk_channel;

TRUNCATE TABLE youtube_num_statistics_obt;
TRUNCATE TABLE youtube_statistics_obt;


SELECT * FROM youtube_statistics_obt WHERE valid_from = '2026-05-05';
SELECT * FROM youtube_statistics_obt WHERE hk_comment IS NULL;
SELECT * FROM youtube_statistics_obt WHERE valid_from = '2026-05-05';
SELECT * FROM youtube_num_statistics_obt WHERE date_of_data = '2026-05-05';

SELECT * FROM youtube_statistics_obt WHERE valid_to IS NOT NULL;


SELECT * FROM youtube_statistics_obt s
WHERE NOT EXISTS (
	SELECT 1 FROM youtube_statistics_obt ns
	WHERE s.hk_channel = ns.hk_channel AND s.hk_VIDEO = ns.hk_VIDEO AND s.hk_COMMENT = ns.hk_COMMENT
) 

SELECT * FROM pg_stg_silver_current WHERE hk_video in(
'c9388638ed625d4d5e906146a76dc5d2','8a984001069ac05f6fc5769e11dd032e','eff993c5355790a5f002e33fca956715')


SELECT s.*, s.hk_channel AS join_key, NULL AS hash_diff_obt
FROM pg_stg_silver_current s
UNION ALL
SELECT s.*, s.hk_channel AS join_key, o.hash_diff AS hash_diff_obt 
FROM pg_stg_silver_current s
LEFT JOIN public.youtube_statistics_obt o ON s.hk_channel = o.hk_channel 
	 AND s.hk_video = o.hk_video AND s.hk_comment = o.hk_comment
WHERE o.is_current = TRUE AND s.hash_diff <> COALESCE(o.hash_diff, s.hash_diff)

SELECT COUNT(*) FROM youtube_num_statistics_obt; 
SELECT COUNT(*) FROM youtube_statistics_obt;

SELECT COUNT(CASE WHEN valid_from = '2026-04-24' THEN valid_from END), 
	COUNT(CASE WHEN valid_from = '2026-05-05' THEN valid_from END),
	COUNT(*)
FROM youtube_statistics_obt;