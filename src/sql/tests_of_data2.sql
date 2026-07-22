SELECT * FROM public.pg_stg_silver_current

SELECT * FROM public.youtube_num_statistics_obt WHERE date_of_data = '2026-06-12'

SELECT * FROM public.youtube_statistics_obt WHERE '2026-06-12' BETWEEN VALID_FROM AND COALESCE(VALID_TO, '9999-12-31')

select valid_from, COUNT(valid_from) AS valid_from_cnt, valid_to, count(valid_to) as valid_to_cnt  
FROM public.youtube_statistics_obt
GROUP BY valid_from, valid_to
UNION ALL
SELECT NULL AS VALID_FROM, count(*), NULL as valid_to, count(*) FROM public.youtube_statistics_obt

SELECT hash_diff FROM (
	SELECT hash_diff, valid_to, is_current
	FROM public.youtube_statistics_obt
	WHERE valid_to IS NULL OR is_current IS TRUE
) cte
GROUP BY hash_diff
HAVING COUNT(hash_diff) > 1

SELECT hash_diff, valid_from, valid_to, is_current
FROM public.youtube_statistics_obt obt
where hash_diff in (
	SELECT hash_diff FROM (
		SELECT hash_diff, valid_to, is_current
		FROM public.youtube_statistics_obt
		WHERE valid_to IS NULL OR is_current IS TRUE
	) cte
	WHERE obt.hash_diff = cte.hash_diff
	GROUP BY hash_diff
	HAVING COUNT(hash_diff) > 1
)

SELECT * FROM public.youtube_statistics_obt

TRUNCATE TABLE public.youtube_statistics_obt

SELECT date_of_data, COUNT(*) FROM public.youtube_num_statistics_obt GROUP BY date_of_data

SELECT COUNT(*) FROM public.youtube_num_statistics_obt where date_of_data = '2026-04-24'

SELECT * --hk_channel, hk_video, hk_comment, channel, channel_view_count, channel_subscrb_count
FROM public.youtube_num_statistics_obt where date_of_data = '2026-06-12'

SELECT * FROM public.youtube_statistics_obt WHERE valid_from = '2026-05-30' AND valid_TO IS NULL
SELECT * FROM public.youtube_statistics_obt WHERE valid_from = '2026-06-12' AND valid_to IS NULL

SELECT hash_diff, valid_from, valid_to, is_current,
ROW_NUMBER() OVER (PARTITION BY hash_diff ORDER BY hash_diff, valid_from, COALESCE(valid_to, '9999-12-31'))
FROM public.youtube_statistics_obt

SELECT hash_diff, valid_from, valid_to, is_current
FROM public.youtube_statistics_obt obt1
WHERE EXISTS (
	SELECT 1
	FROM public.youtube_statistics_obt obt2
	WHERE obt1.hash_diff = obt2.hash_diff
	GROUP BY hash_diff
	HAVING COUNT(is_current) > 1 OR COUNT(CASE WHEN valid_to IS NULL THEN 1 END) > 1 
)

SELECT hash_diff, ANY_VALUE(valid_from), ANY_VALUE(valid_to), ANY_VALUE(is_current)
FROM public.youtube_statistics_obt 
GROUP BY hash_diff
HAVING COUNT(is_current) > 1 OR COUNT(CASE WHEN valid_to IS NULL THEN 1 END) > 1 

SELECT hash_diff, valid_from, valid_to, is_current
FROM public.youtube_statistics_obt
ORDER BY hash_diff, valid_from, COALESCE(valid_to, '9999-12-31')


TRUNCATE TABLE public.youtube_num_statistics_obt;
TRUNCATE TABLE public.youtube_statistics_obt;

SELECT * FROM public.youtube_num_statistics_obt;
SELECT * FROM public.youtube_statistics_obt;

SELECT date_of_data, COUNT(*) FROM public.youtube_num_statistics_obt GROUP BY date_of_data ORDER BY date_of_data;
SELECT valid_from, valid_to, COUNT(*) FROM public.youtube_statistics_obt GROUP BY valid_from, valid_to;
SELECT COUNT(*) FROM public.youtube_statistics_obt;

SELECT * FROM public.youtube_statistics_obt WHERE valid_from = '2026-05-05';

SELECT * FROM public.youtube_num_statistics_obt ns
WHERE date_of_data = '2026-06-12'
AND NOT EXISTS (
	SELECT 1 FROM public.youtube_statistics_obt s
	WHERE ns.hk_channel = s.hk_channel AND ns.hk_video = s.hk_video AND COALESCE(ns.hk_comment, '') = COALESCE(s.hk_comment, '')
);

SELECT * FROM public.youtube_statistics_obt s
WHERE s.hk_video IN (
	'c9388638ed625d4d5e906146a76dc5d2', '8a984001069ac05f6fc5769e11dd032e', 'eff993c5355790a5f002e33fca956715')

SELECT * FROM public.pg_stg_silver_current

SELECT CAST('2026-04-24' AS DATE) AS valid_from,
                        g.next_val - 1 AS valid_to,
                        '{run_id}' AS run_id,
                        s.hk_channel, s.hk_video, s.hk_comment,
                        s.channel_name, s.custom_url, s.description,
                        s.channel_publish_date, s.video_publish_date, s.video_title, s.video_description,
                        s.video_category_id, s.video_duration, s.video_caption, s.video_dflt_language,
                        s.video_dflt_audio_language, s.comment_text, s.comment_original, s.comment_author,
                        s.comment_author_url, s.comment_publish_date, s.comment_update_date, s.is_public,
                        s.can_reply, CASE WHEN g.next_val IS NULL THEN TRUE ELSE FALSE END AS is_current, s.hash_diff
                FROM public.pg_stg_silver_current s
                LEFT JOIN (
                    SELECT hk_channel, hk_video, hk_comment, MIN(valid_from) as next_val
                    FROM public.youtube_statistics_obt
                    WHERE valid_from > CAST('2026-04-24' AS DATE)
                    GROUP BY 1, 2, 3
                ) g ON s.hk_channel = g.hk_channel AND s.hk_video = g.hk_video AND s.hk_comment = g.hk_comment
                WHERE EXISTS (
                    SELECT 1
                    FROM public.youtube_statistics_obt o
                    WHERE o.hk_channel IS NOT DISTINCT FROM s.hk_channel
                    AND o.hk_video   IS NOT DISTINCT FROM s.hk_video
                    AND o.hk_comment IS NOT DISTINCT FROM s.hk_comment
                    AND o.valid_to   = CAST('2026-04-24' AS DATE) - 1
                    AND o.is_current = FALSE
                )
				
				
SELECT 1 FROM youtube_statistics_obt LIMIT 1


CREATE TABLE youtube_statistics_obt_copy AS SELECT * FROM youtube_statistics_obt
CREATE TABLE youtube_num_statistics_obt_copy AS SELECT * FROM youtube_num_statistics_obt