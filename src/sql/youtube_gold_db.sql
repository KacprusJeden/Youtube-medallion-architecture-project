-- USER
CREATE USER youtube WITH PASSWORD 'youtubeKP2026';

-- TABLES
CREATE TABLE public.youtube_num_statistics_obt (
	date_of_data date NOT NULL,
    run_id varchar(32),
    hk_channel varchar(32) NOT NULL,
    hk_video varchar(32),
    hk_comment varchar(32),
    channel_view_count int,
    channel_subscrb_count int,
    channel_hidden_subscrb_count boolean,
    video_count int,
    video_view_count int,
    video_like_count int,
    video_favorite_count int,
    video_comment_count int,
    comment_like_count int,
    comment_reply_count int
) PARTITION BY LIST (date_of_data);


CREATE TABLE public.youtube_statistics_obt(
    valid_from date,
    valid_to date,
    run_id varchar(32),
    hk_channel varchar(32) NOT NULL,
    hk_video varchar(32),
    hk_comment varchar(32),
    channel_name varchar(50),
    custom_url varchar(50),
    description varchar(4000),
    channel_publish_date timestamp,
    video_publish_date timestamp,
    video_title varchar(4000),
    video_description varchar(4000),
    video_category_id int,
    video_duration interval,
    video_caption boolean,
    video_dflt_language char(5),
    video_dflt_audio_language char(5),
    comment_text varchar(2000),
    comment_original varchar(2000),
    comment_author varchar(100),
    comment_author_url varchar(300),
    comment_publish_date timestamp,
    comment_update_date timestamp,
    is_public boolean,
    can_reply boolean,
    is_current boolean
);


-- INDEXES, PARTITIONS
CREATE INDEX idx_yt_num_current
ON public.youtube_num_statistics_obt (hk_channel, hk_video, hk_comment);

CREATE INDEX idx_yt_stat_current
ON public.youtube_statistics_obt (valid_from, valid_to, hk_channel, hk_video, hk_comment)
WHERE is_current = TRUE;

-- FUNCTION, PARTITION MANAGING
CREATE OR REPLACE FUNCTION public.manage_partition(
    p_table_name TEXT,
    p_date_of_data VARCHAR(10),
    p_mode VARCHAR(15)
)
RETURNS VOID AS
$$
DECLARE
    v_partition_name TEXT;
    v_partition_exists BOOLEAN;
    v_part RECORD;
    v_is_partitioned BOOLEAN;
BEGIN

    SELECT c.relkind = 'p' INTO v_is_partitioned
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public' AND c.relname = p_table_name;

    IF NOT v_is_partitioned THEN
        IF p_mode IN ('full', 'reload') THEN
            EXECUTE format('TRUNCATE TABLE public.%I', p_table_name);
        END IF;
        RETURN;
    END IF;

    v_partition_name := format('%I_%s', p_table_name, REPLACE(p_date_of_data, '-', '_'));

    IF p_mode IN ('full', 'reload') THEN
        FOR v_part IN
            SELECT inhrelid::regclass AS part_name
            FROM pg_inherits
            WHERE inhparent = format('public.%I', p_table_name)::regclass
        LOOP
            EXECUTE format('DROP TABLE IF EXISTS %s CASCADE', v_part.part_name);
        END LOOP;
    END IF;

    SELECT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = lower(v_partition_name) AND n.nspname = 'public'
    ) INTO v_partition_exists;

    IF NOT v_partition_exists THEN
        EXECUTE format(
            'CREATE TABLE public.%I PARTITION OF public.%I FOR VALUES IN (%L)',
            v_partition_name, p_table_name, p_date_of_data
        );
		
		EXECUTE format(
			'CREATE INDEX IF NOT EXISTS %I_idx_current
			 ON public.%I (hk_channel, hk_video, hk_comment) %s',
			v_partition_name,
			v_partition_name,
			CASE WHEN EXISTS (SELECT 1 FROM information_schema.columns
				WHERE table_schema = 'public' 
  				AND table_name = p_table_name
				AND column_name = 'is_current'
			) THEN 'WHERE is_current = TRUE' ELSE '' END
		);
    ELSE
        EXECUTE format('TRUNCATE TABLE public.%I', v_partition_name);
    END IF;
END;
$$ LANGUAGE plpgsql;


SELECT public.manage_partition('youtube_num_statistics_obt', '2026-04-24', 'full');

-- ROLE
CREATE ROLE yt_role;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLE public.youtube_statistics_OBT TO yt_role;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLE public.youtube_num_statistics_OBT TO yt_role;
GRANT EXECUTE ON FUNCTION manage_partition (TEXT, VARCHAR(10), VARCHAR(15)) TO yt_role;
GRANT CREATE ON SCHEMA public TO yt_role;

GRANT yt_role TO youtube;

ALTER TABLE public.youtube_num_statistics_obt OWNER TO youtube;
ALTER TABLE public.youtube_statistics_obt ADD COLUMN hash_diff varchar(50);



SELECT COUNT(DISTINCT hk_channel) channels_cnt, COUNT(DISTINCT COALESCE(HK_VIDEO, 'NV')) videos_cnt, 
COUNT(DISTINCT COALESCE(HK_VIDEO, 'NCM')) comments_cnt
FROM youtube_statistics_obt

SELECT channel_name, COUNT(DISTINCT hk_channel) channels_cnt, COUNT(DISTINCT COALESCE(HK_VIDEO, 'NV')) videos_cnt, 
COUNT(DISTINCT COALESCE(HK_COMMENT, 'NCM')) comments_cnt
FROM youtube_statistics_obt
GROUP BY channel_name

SELECT * FROM youtube_statistics_obt WHERE hk_channel = 'c692d0d2d3f0191197456aa41d16f348';
SELECT * FROM youtube_num_statistics_obt WHERE hk_video = '04fccb020a303f7e1c4117b48e1e2aba';

SELECT * FROM youtube_statistics_obt WHERE valid_to IS NOT NULL

SELECT COUNT(DISTINCT COALESCE(HK_VIDEO, 'NV')), COUNT(DISTINCT HK_VIDEO)
FROM youtube_statistics_obt WHERE hk_channel = 'c692d0d2d3f0191197456aa41d16f348';

SELECT * FROM 
