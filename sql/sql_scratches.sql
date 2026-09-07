-- ============================================================================
-- 🧹 ASSET CLEANUP: Bottom 2 Quartiles (Lowest 50%) Ordered by File Size
-- ============================================================================
-- Splits imported assets for a device (or all devices) into 4 quartiles based on
-- score_normalized (quality/curation score), filters for the bottom 2 quartiles (Q1 & Q2 - bottom 50%),
-- and orders them largest-to-smallest so you can delete high-storage/low-value files first.
-- ============================================================================

-- 1. Attach Apple Photos database for Apple Moment suggestions
ATTACH DATABASE '/Volumes/Extreme Pro/Photos Library/All-Media.photoslibrary/database/Photos.sqlite' AS photos_db;
-- (Or local DB copy:)
-- ATTACH DATABASE '/Users/nickolaycohen/Photos Library DB/All-Media-Extreme/database/Photos.sqlite' AS photos_db;

WITH all_assets_with_moments AS (
    SELECT 
        a.asset_id,
        a.original_filename,
        a.month,
        a.date_created_utc,
        COALESCE(zea.ZCAMERAMODEL, 'Unknown') AS camera_model,
        COALESCE(zea.ZCAMERAMAKE, 'Unknown') AS camera_make,
        COALESCE(do.owner_name, 'Shared/Other') AS primary_owner,
        COALESCE(v.score_normalized, 0.0) AS score_normalized,
        a.aesthetic_score,
        a.google_favorite,
        a.mobile_apple_photos_featured_photos AS apple_featured,
        a.apple_photos_monthly_selection AS apple_monthly_sel,
        -- Assigned moment (or fallback to Apple Photos suggested moment with YYYY-MM-DD prefix)
        COALESCE(
            a.MomentsAlbumName, 
            a.curated_album, 
            a.to_be_curated_album, 
            ps.published_moments,
            CASE 
                WHEN m.ZTITLE IS NOT NULL AND m.ZTITLE != '' 
                THEN 'SUGG: ' || COALESCE(substr(a.date_created_utc, 1, 10), date(za.ZDATECREATED + 978307200, 'unixepoch'), a.month, '') || ' - ' || m.ZTITLE
                ELSE NULL 
            END,
            '—'
        ) AS assigned_moment,
        ps.last_asset_pub,
        aaa.ZORIGINALFILESIZE AS file_size_bytes,
        ROUND(COALESCE(aaa.ZORIGINALFILESIZE, 0) / 1048576.0, 2) AS file_size_mb,
        ROUND(COALESCE(aaa.ZORIGINALFILESIZE, 0) / 1073741824.0, 3) AS file_size_gb,
        -- Splits assets into 4 equal quartiles (1 = Lowest 25%, 2 = 25-50%, 3 = 50-75%, 4 = Top 25%)
        NTILE(4) OVER (
            PARTITION BY COALESCE(zea.ZCAMERAMODEL, 'Unknown')
            ORDER BY COALESCE(v.score_normalized, 0.0) ASC
        ) AS score_quartile,
        CASE 
            WHEN COALESCE(za_ext.ZKIND, za.ZKIND, 0) = 1 
                 OR lower(a.original_filename) LIKE '%.mov' 
                 OR lower(a.original_filename) LIKE '%.mp4' 
                 OR lower(a.original_filename) LIKE '%.m4v' 
                 OR lower(a.original_filename) LIKE '%.avi' 
            THEN 'video' 
            ELSE 'photo' 
        END AS media_type
    FROM assets a
    LEFT JOIN photos_db.ZASSET za_ext ON za_ext.ZUUID = a.asset_id
    LEFT JOIN photos_db.ZMOMENT m ON za_ext.ZMOMENT = m.Z_PK
    LEFT JOIN ZASSET za ON za.ZUUID = a.asset_id
    LEFT JOIN ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = za.Z_PK
    LEFT JOIN ZEXTENDEDATTRIBUTES zea ON zea.ZASSET = za.Z_PK
    LEFT JOIN device_owners do ON do.camera_model = zea.ZCAMERAMODEL
        AND (do.start_date IS NULL OR do.start_date <= date(za.ZDATECREATED + 978307200, 'unixepoch'))
        AND (do.end_date IS NULL OR do.end_date >= date(za.ZDATECREATED + 978307200, 'unixepoch'))
    LEFT JOIN (
        SELECT asset_id, GROUP_CONCAT(DISTINCT moment_name) AS published_moments, MAX(published_at_utc) AS last_asset_pub 
        FROM publications 
        GROUP BY asset_id
    ) ps ON ps.asset_id = a.asset_id
    LEFT JOIN ranked_assets_view v ON v.asset_id = a.asset_id
    WHERE zea.ZCAMERAMODEL IS NOT NULL 
      AND zea.ZCAMERAMODEL != ''
      AND COALESCE(a.removed_from_source, 0) = 0
),
moment_pub_stats AS (
    SELECT 
        moment_name,
        MAX(published_at_utc) AS last_published_at
    FROM publications
    GROUP BY moment_name
),
ranked_siblings AS (
    SELECT 
        assigned_moment,
        media_type,
        original_filename,
        score_normalized,
        file_size_mb,
        ROW_NUMBER() OVER (PARTITION BY assigned_moment, media_type ORDER BY score_normalized DESC) as rn
    FROM all_assets_with_moments
    WHERE assigned_moment != '—'
),
moment_photos AS (
    SELECT 
        assigned_moment,
        COUNT(*) AS photo_count,
        ROUND(MAX(score_normalized), 4) AS max_photo_score,
        GROUP_CONCAT(
            original_filename || ' (' || ROUND(score_normalized, 3) || ', ' || file_size_mb || 'MB)',
            ' | '
        ) AS top_photos
    FROM ranked_siblings
    WHERE media_type = 'photo' AND rn <= 3
    GROUP BY assigned_moment
),
moment_videos AS (
    SELECT 
        assigned_moment,
        COUNT(*) AS video_count,
        ROUND(MAX(score_normalized), 4) AS max_video_score,
        GROUP_CONCAT(
            original_filename || ' (' || ROUND(score_normalized, 3) || ', ' || file_size_mb || 'MB)',
            ' | '
        ) AS top_videos
    FROM ranked_siblings
    WHERE media_type = 'video' AND rn <= 3
    GROUP BY assigned_moment
)
SELECT 
    c.score_quartile,
    c.original_filename AS candidate_file,
    c.month,
    c.file_size_mb,
    ROUND(c.score_normalized, 4) AS candidate_score,
    c.assigned_moment,
    CASE 
        WHEN mp.last_published_at IS NOT NULL THEN '✅ ' || substr(mp.last_published_at, 1, 10)
        WHEN c.last_asset_pub IS NOT NULL THEN '✅ ' || substr(c.last_asset_pub, 1, 10)
        ELSE '—'
    END AS moment_published_date,
    COALESCE(p.photo_count, 0) + COALESCE(v.video_count, 0) AS total_assets_in_moment,
    MAX(COALESCE(p.max_photo_score, 0), COALESCE(v.max_video_score, 0)) AS best_score_in_moment,
    CASE 
        WHEN p.top_photos IS NOT NULL AND v.top_videos IS NOT NULL 
        THEN '📷 Photos: ' || p.top_photos || '  ||  🎥 Videos: ' || v.top_videos
        WHEN p.top_photos IS NOT NULL 
        THEN '📷 Photos: ' || p.top_photos
        WHEN v.top_videos IS NOT NULL 
        THEN '🎥 Videos: ' || v.top_videos
        ELSE '— (Standalone / No other assets)'
    END AS moment_top_assets_with_scores,
    c.date_created_utc,
    c.primary_owner,
    c.camera_model
FROM all_assets_with_moments c
LEFT JOIN moment_pub_stats mp ON mp.moment_name = c.assigned_moment AND c.assigned_moment != '—'
LEFT JOIN moment_photos p ON p.assigned_moment = c.assigned_moment AND c.assigned_moment != '—'
LEFT JOIN moment_videos v ON v.assigned_moment = c.assigned_moment AND c.assigned_moment != '—'
WHERE c.camera_model = 'iPhone 16 Pro'  -- 👈 Filter by device
  AND c.score_quartile IN (1, 2)         -- 👈 Bottom 50% lowest quality assets (Q1 and Q2)
  -- Optional Safety Guards (uncomment if desired):
  -- AND COALESCE(c.google_favorite, 0) = 0
  -- AND COALESCE(c.apple_featured, 0) = 0
ORDER BY c.file_size_bytes DESC;


-- ============================================================================
-- 📊 Quartile Summary by Device (Count, Total GB & Score Range per Quartile)
-- ============================================================================
WITH scored_assets AS (
    SELECT 
        COALESCE(zea.ZCAMERAMODEL, 'Unknown') AS camera_model,
        COALESCE(do.owner_name, 'Shared/Other') AS primary_owner,
        COALESCE(v.score_normalized, 0.0) AS score_normalized,
        COALESCE(aaa.ZORIGINALFILESIZE, 0) AS file_size_bytes,
        NTILE(4) OVER (
            PARTITION BY COALESCE(zea.ZCAMERAMODEL, 'Unknown')
            ORDER BY COALESCE(v.score_normalized, 0.0) ASC
        ) AS score_quartile
    FROM assets a
    LEFT JOIN ZASSET za ON za.ZUUID = a.asset_id
    LEFT JOIN ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = za.Z_PK
    LEFT JOIN ZEXTENDEDATTRIBUTES zea ON zea.ZASSET = za.Z_PK
    LEFT JOIN device_owners do ON do.camera_model = zea.ZCAMERAMODEL
        AND (do.start_date IS NULL OR do.start_date <= date(za.ZDATECREATED + 978307200, 'unixepoch'))
        AND (do.end_date IS NULL OR do.end_date >= date(za.ZDATECREATED + 978307200, 'unixepoch'))
    LEFT JOIN ranked_assets_view v ON v.asset_id = a.asset_id
    WHERE zea.ZCAMERAMODEL IS NOT NULL 
      AND zea.ZCAMERAMODEL != ''
      AND COALESCE(a.removed_from_source, 0) = 0
)
SELECT 
    primary_owner,
    camera_model,
    score_quartile,
    COUNT(*) AS asset_count,
    ROUND(SUM(file_size_bytes) / 1073741824.0, 2) AS total_size_gb,
    ROUND(MIN(score_normalized), 4) AS min_score,
    ROUND(MAX(score_normalized), 4) AS max_score
FROM scored_assets
GROUP BY primary_owner, camera_model, score_quartile
ORDER BY primary_owner ASC, camera_model ASC, score_quartile ASC;


-- 1. Attach Apple Photos database
ATTACH DATABASE '/Volumes/Extreme Pro/Photos Library/All-Media.photoslibrary/database/Photos.sqlite' AS photos_db;
-- (Or local copy:)
-- ATTACH DATABASE '/Users/nickolaycohen/Photos Library DB/All-Media-Extreme/database/Photos.sqlite' AS photos_db;

-- 2. Query Skipped Videos with Removal Status, Source & Owner
SELECT 
    a.original_filename,
    a.month,
    COALESCE(v.score_normalized, 0.0) AS score,
    a.date_created_utc,
    COALESCE(zea.ZCAMERAMODEL, zea.ZCAMERAMAKE, 'Unknown') AS source_name,
    COALESCE(
        do.owner_name,
        CASE 
            WHEN zea.ZCAMERAMODEL IN ('iPhone 16 Pro', 'iPhone 13 Pro Max', 'iPhone 11 Pro', 'iPhone 8 Plus', 'iPhone 6s', 'iPhone 5s', 'iPhone 4') THEN 'Nickolay'
            WHEN zea.ZCAMERAMODEL IN ('iPhone 17 Pro Max', 'iPhone 12 Pro Max', 'iPhone 14 Pro Max', 'iPhone 11 Pro Max', 'iPhone XS Max') THEN 'Wife'
            WHEN zea.ZCAMERAMODEL IN ('iPhone 12', 'iPhone 13 Pro') THEN 'Kid'
            WHEN zea.ZCAMERAMODEL IN ('Canon EOS Rebel T7', 'Canon EOS 5D Mark IV', 'Canon EOS 5D', 'HERO12 Black', 'HERO7 Silver') THEN 'Shared'
            ELSE 'Shared/Other'
        END
    ) AS owner,
    CASE 
        WHEN EXISTS (
            SELECT 1 
            FROM photos_db.Z_30ASSETS aa_rem 
            JOIN photos_db.ZGENERICALBUM ga_rem ON ga_rem.Z_PK = aa_rem.Z_30ALBUMS
            WHERE aa_rem.Z_3ASSETS = za.Z_PK 
              AND ga_rem.ZTITLE = 'Removed from Source'
              AND ga_rem.ZTRASHEDSTATE = 0
        ) THEN '✅ Yes' 
        ELSE '❌ No' 
    END AS is_removed_from_source,
    CASE WHEN a.uploaded_to_google = 1 THEN '✅ Yes' ELSE '❌ No' END AS uploaded_to_google,
    za.Z_PK
FROM assets a
JOIN photos_db.ZASSET za ON za.ZUUID = a.asset_id
JOIN photos_db.Z_30ASSETS aa ON aa.Z_3ASSETS = za.Z_PK
JOIN photos_db.ZGENERICALBUM ga ON ga.Z_PK = aa.Z_30ALBUMS
LEFT JOIN photos_db.ZEXTENDEDATTRIBUTES zea ON zea.ZASSET = za.Z_PK
LEFT JOIN device_owners do ON do.camera_model = zea.ZCAMERAMODEL
    AND (do.start_date IS NULL OR do.start_date <= date(za.ZDATECREATED + 978307200, 'unixepoch'))
    AND (do.end_date IS NULL OR do.end_date >= date(za.ZDATECREATED + 978307200, 'unixepoch'))
LEFT JOIN ranked_assets_view v ON v.asset_id = a.asset_id
WHERE ga.ZTITLE = 'Google Upload Skipped Videos'
  AND ga.ZTRASHEDSTATE = 0
  AND za.ZTRASHEDSTATE = 0
ORDER BY v.score_normalized DESC NULLS LAST
-- LIMIT 15;


-- ============================================================================
-- Device Cleanup Summary with Featured Photos & Published Moments
-- ============================================================================
SELECT 
    a.month AS asset_month,
    COALESCE(do.owner_name, 'Shared/Other') AS primary_owner,
    COALESCE(zea.ZCAMERAMODEL, 'Unknown') AS device_camera_model,
    COUNT(a.asset_id) AS total_assets_on_device,
    SUM(CASE WHEN a.mobile_apple_photos_featured_photos = 1 THEN 1 ELSE 0 END) AS featured_count,
    GROUP_CONCAT(CASE WHEN a.mobile_apple_photos_featured_photos = 1 THEN a.original_filename END, ', ') AS featured_filenames,
    SUM(CASE WHEN a.google_favorite = 1 THEN 1 ELSE 0 END) AS google_fav_count,
    COUNT(DISTINCT p.asset_id) AS published_count,
    GROUP_CONCAT(DISTINCT p.moment_name) AS published_moments_or_albums  -- 👈 Placed right after published_count
FROM assets a
LEFT JOIN ZASSET za ON za.ZUUID = a.asset_id
LEFT JOIN ZEXTENDEDATTRIBUTES zea ON zea.ZASSET = za.Z_PK
LEFT JOIN device_owners do ON do.camera_model = zea.ZCAMERAMODEL
    AND (do.start_date IS NULL OR do.start_date <= date(za.ZDATECREATED + 978307200, 'unixepoch'))
    AND (do.end_date IS NULL OR do.end_date >= date(za.ZDATECREATED + 978307200, 'unixepoch'))
LEFT JOIN publications p ON p.asset_id = a.asset_id
-- WHERE a.month = '2026-07'  -- 👈 Change month here (or remove to view all months)
GROUP BY a.month, primary_owner, device_camera_model
HAVING featured_count > 0 OR published_count > 0
ORDER BY primary_owner ASC, asset_month desc;

-- ============================================================================
-- Summary of Featured Assets Grouped by Device & Month
-- ============================================================================
SELECT 
    a.month AS asset_month,
    COALESCE(do.owner_name, 'Shared/Other') AS primary_owner,
    COALESCE(zea.ZCAMERAMODEL, 'Unknown') AS device_camera_model,
    COUNT(a.asset_id) AS total_assets_on_device,
    SUM(CASE WHEN a.mobile_apple_photos_featured_photos = 1 THEN 1 ELSE 0 END) AS featured_count,
    GROUP_CONCAT(CASE WHEN a.mobile_apple_photos_featured_photos = 1 THEN a.original_filename END, ', ') AS featured_filenames,
    SUM(CASE WHEN a.google_favorite = 1 THEN 1 ELSE 0 END) AS google_fav_count,
    COUNT(DISTINCT p.asset_id) AS published_count
FROM assets a
LEFT JOIN ZASSET za ON za.ZUUID = a.asset_id
LEFT JOIN ZEXTENDEDATTRIBUTES zea ON zea.ZASSET = za.Z_PK
LEFT JOIN device_owners do ON do.camera_model = zea.ZCAMERAMODEL
    AND (do.start_date IS NULL OR do.start_date <= date(za.ZDATECREATED + 978307200, 'unixepoch'))
    AND (do.end_date IS NULL OR do.end_date >= date(za.ZDATECREATED + 978307200, 'unixepoch'))
LEFT JOIN publications p ON p.asset_id = a.asset_id
-- WHERE a.month = '2026-07'  -- 👈 Change month here
GROUP BY a.month, primary_owner, device_camera_model
HAVING featured_count > 0
ORDER BY primary_owner ASC, asset_month desc, device_camera_model ASC;


-- save assets to clean from source
-- ============================================================================
-- Media Cleanup Recommendation Overview for DBeaver
-- Shows which months and devices are imported, Google-rated, and published.
-- ============================================================================
WITH device_month_stats AS (
    SELECT 
        COALESCE(zea.ZCAMERAMODEL, 'Unknown') AS camera_model,
        zea.ZCAMERAMAKE AS camera_make,
        a.month,
        COUNT(DISTINCT a.asset_id) AS total_imported_assets,
        SUM(CASE WHEN a.google_favorite = 1 THEN 1 ELSE 0 END) AS google_fav_count,
        SUM(CASE WHEN a.aesthetic_score IS NOT NULL AND a.aesthetic_score > 0 THEN 1 ELSE 0 END) AS aesthetic_scored_count,
        MIN(a.original_filename) AS earliest_imported_file,
        MAX(a.original_filename) AS latest_imported_file,
        MIN(a.date_created_utc) AS min_date_created,
        MAX(a.date_created_utc) AS max_date_created
    FROM assets a
    LEFT JOIN ZASSET za ON za.ZUUID = a.asset_id
    LEFT JOIN ZEXTENDEDATTRIBUTES zea ON zea.ZASSET = za.Z_PK
    GROUP BY COALESCE(zea.ZCAMERAMODEL, 'Unknown'), zea.ZCAMERAMAKE, a.month
),
published_stats AS (
    SELECT 
        COALESCE(zea.ZCAMERAMODEL, 'Unknown') AS camera_model,
        a.month,
        COUNT(DISTINCT p.asset_id) AS published_count,
        GROUP_CONCAT(DISTINCT p.moment_name) AS published_moments,
        GROUP_CONCAT(DISTINCT p.platform) AS platforms
    FROM publications p
    JOIN assets a ON p.asset_id = a.asset_id
    LEFT JOIN ZASSET za ON za.ZUUID = a.asset_id
    LEFT JOIN ZEXTENDEDATTRIBUTES zea ON zea.ZASSET = za.Z_PK
    GROUP BY COALESCE(zea.ZCAMERAMODEL, 'Unknown'), a.month
)
SELECT 
    COALESCE(do.owner_name, 'Shared/Other') AS primary_owner,
    dms.camera_model AS device_camera_model,
    dms.month AS asset_month,
    mb.status_code AS batch_status_code,
    bs.short_label AS batch_stage,
    dms.total_imported_assets,
    dms.google_fav_count AS google_favorites,
    COALESCE(ps.published_count, 0) AS published_assets_count,
    ps.published_moments,
    dms.earliest_imported_file || ' -> ' || dms.latest_imported_file AS source_filename_range,
    dms.min_date_created || ' to ' || dms.max_date_created AS capture_date_range,
    CASE 
        WHEN mb.status_code >= '650' AND COALESCE(ps.published_count, 0) > 0 
            THEN '✅ SAFE TO REMOVE: Curated, Ranked & Published'
        WHEN mb.status_code >= '600' AND COALESCE(ps.published_count, 0) > 0 
            THEN '✅ SAFE TO REMOVE: Ranked & Published'
        WHEN mb.status_code >= '550' AND COALESCE(ps.published_count, 0) > 0 
            THEN '⚠️ Google rated & Published (Pipeline in progress)'
        WHEN mb.status_code >= '550' 
            THEN '⏳ Rated with Google (Not yet published)'
        WHEN mb.status_code >= '400' 
            THEN '⏳ In Google Pipeline (Awaiting curation)'
        ELSE '❌ NOT SAFE: Pipeline pending'
    END AS recommendation_status
FROM device_month_stats dms
LEFT JOIN published_stats ps ON ps.camera_model = dms.camera_model AND ps.month = dms.month
LEFT JOIN device_owners do ON do.camera_model = dms.camera_model
    AND (do.start_date IS NULL OR do.start_date <= dms.month || '-31')
    AND (do.end_date IS NULL OR do.end_date >= dms.month || '-01')
LEFT JOIN month_batches mb ON mb.month = dms.month
LEFT JOIN batch_status bs ON mb.status_code = bs.code
WHERE COALESCE(ps.published_count, 0) > 0 OR mb.status_code >= '600'
ORDER BY primary_owner ASC, dms.month DESC;

-- get count of favorites for month batches
select mb.*, 
	(select count() from assets a WHERE a.google_favorite = 1 and a."month" = mb."month")
from month_batches mb 
order by mb.month desc

select 	
/*	case 
		when a.aesthetic_score between 0 and .25 then '0-.25'
		when a.aesthetic_score between 0.25 and 0.3125 then '0.25-0.3125'
		when a.aesthetic_score between 0.3125 and .375 then '.3175-.375'
		when a.aesthetic_score between 0.375 and .4375 then '.375-.4375'
		when a.aesthetic_score between 0.4375 and .5 then '.4375-.5'
		when a.aesthetic_score between 0.5 and 0.625 then '.5-0.625'
		when a.aesthetic_score between 0.625 and .75 then '.625-.75'
		when a.aesthetic_score between 0.75 and 1 then 1		
	else .75 */
	case 
		when aesthetic_score >  0.5341 then 1
		else 0
	end as score_range
	, count()
from assets a
group by score_range;
--0	180551
--1	28378 - 15.7%

-- 
select * from ranked_assets_view rav 
order by rav.score_normalized desc
-- 
select * from moments m 

-- all google favs by month
select month, count(*)
from assets a 
where a.google_favorite 
group by "month" 
order by "month" DESC 

-- check status of favs
select a."month", count(*)
from assets a 
where a.google_favorite =  1 or a.uploaded_to_google =1
group by "month" 

-- rest uploaded flag for months in 000 status
/*
update assets 
set uploaded_to_google = 0
where assets."month" in ('2024-12', '2025-01', '2025-02', '2025-04')
and uploaded_to_google = 1
*/

-- get batch statuses
SELECT month, status_code, latest_import_id 
FROM month_batches
order by month desc;


-- rate Identified Moments
select me.album_name, avg(a.), count(*)
from moment_exports me 
join assets a on a.asset_id = me.asset_id 
group by album_name 
order by 2 desc

select *
from moment_exports me 

-- find next asset to be added to Moments
SELECT v.asset_id, v.original_filename, v."month", a.date_created_utc , v.MomentsAlbumName, v.score_normalized
FROM ranked_assets_view v
JOIN month_batches mb ON v.month = mb.month
join assets a on a.asset_id = v.asset_id 
WHERE mb.status_code >= '600'
ORDER BY v.score_normalized DESC;


SELECT v.asset_id, v.MomentsAlbumName
FROM ranked_assets_view v
JOIN month_batches mb ON v.month = mb.month
WHERE mb.status_code >= '600' 
  AND v.MomentsAlbumName IS NOT NULL
  AND v.MomentsAlbumName != ''
limit 50;

        
select * 
from schema_migrations 
        

select *
from ranked_assets_view
where -- MomentsAlbumName is not null and
	original_filename = 'IMG_7959.HEIC'

select * 
from assets
where 

select *
from month_batches mb 
order by 2 desc;

-- FAILING: Pick assets to add moments - locate first item with null Moment and added it to a new album in Apple Photos Library
SELECT v.original_filename, v.month, v.MomentsAlbumName, v.score_normalized, v.aesthetic_score, v.google_favorite , v.apple_photos_monthly_selection 
FROM ranked_assets_view v
JOIN month_batches mb ON v.month = mb.month
WHERE mb.status_code >= '600'
-- and not exists (select 1 from )
ORDER BY v.score_normalized DESC;


SELECT original_filename, uploaded_to_google, * FROM assets WHERE original_filename = 'IMG_2580.HEIC';


select mb.status_code, a.google_favorite, a.aesthetic_score, '---', a.*
from assets a
join month_batches mb on mb."month" = a."month" 
where a.month = '2026-04'
order by a.google_favorite desc, a.aesthetic_score desc;

delete 
from assets 
where month = '2025-07' and date_created_utc is null
-- select combined score
-- change selection approach
-- google weight = a.google_favorite * 0.125
-- :months - ('2026-04', '2026-03','2026-02','2026-01','2025-12','2025-11','2025-10', '2025-09')

SELECT month, original_filename, date_created_utc , aesthetic_score, score_normalized,
	google_favorite, MomentsAlbumName  
FROM ranked_assets_view
WHERE month in (select "month" from month_batches mb where mb.status_code >= 600)
ORDER BY score_normalized DESC;

select a.date_created_utc , a.uploaded_to_google, * 
from assets a 
where 
a."month" = '2025-07' or 
(a.date_created_utc like '2025-07%')
order by 1


update assets  
set uploaded_to_google = 0
where "month" = '2025-07'

-- get all moments
select *
from moments

-- get all albums
select *
from albums sa 
-- where parent_folder_name = 'Google Photos Moments' 
order by album_pk desc


-- PRAGMA quick_check

-- How is Google tagging holding time determined
SELECT MAX(updated_at_utc) FROM assets WHERE uploaded_to_google = 1 AND month = '2026-04'

SELECT updated_at_utc,     z.original_filename , z.aesthetic_score, z."month" , z.google_favorite 
FROM assets z
WHERE uploaded_to_google = 1 AND month = '2026-01' 
order by updated_at_utc desc



update 
assets 
set uploaded_to_google =0
where month = '2025-11'


select * 
from assets a 
where a.ignore_continuity_check 

UPDATE month_batches 
SET status_code = '400', updated_at_utc = datetime('now') 
WHERE month = '2026-01';


select * 
from planned_execution pe 

UPDATE month_batches
SET status_code = '400'
WHERE month = '2026-03';


-- sync photos derived query 
SELECT
        strftime('%Y-%m', datetime(z.ZDATECREATED + 978307200, 'unixepoch', 'localtime')) || '_' || COALESCE(ea.ZCAMERAMODEL, 'Unknown'),
    strftime('%Y-%m', datetime(z.ZDATECREATED + 978307200, 'unixepoch', 'localtime')) || ' - ' || COALESCE(ea.ZCAMERAMODEL, 'Unknown'),
    MIN(datetime(z.ZDATECREATED + 978307200, 'unixepoch', 'localtime')),
    NULL,
    COUNT(z.Z_ENT),
    COALESCE(ea.ZCAMERAMAKE, 'Unknown'),
    COALESCE(ea.ZCAMERAMODEL, 'Unknown'),
    MIN(aaa.ZORIGINALFILENAME),
    MAX(aaa.ZORIGINALFILENAME),
    MIN(datetime(z.ZDATECREATED + 978307200, 'unixepoch', 'localtime')),
    MAX(datetime(z.ZDATECREATED + 978307200, 'unixepoch', 'localtime')),
    strftime('%Y-%m', datetime(z.ZDATECREATED + 978307200, 'unixepoch', 'localtime'))
FROM ZASSET z
LEFT JOIN ZEXTENDEDATTRIBUTES ea ON ea.ZASSET = z.Z_PK
LEFT JOIN ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = z.Z_PK
WHERE z.ZIMPORTSESSION IS NOT NULL
GROUP BY 1, ea.ZCAMERAMAKE, ea.ZCAMERAMODEL
ORDER BY 1 DESC

delete FROM imports 
where import_uuid in (72997, 72996, 72995)

update imports
set min_filename = NULL, max_filename = NULL, sequencing_confirmed = 0, min_date = NULL, max_date = NULL
where import_uuid in (72997, 72996, 72995)

-- check sequencing logic
select * 
from imports i 
order by min_date desc

-- find assets by camera model

SELECT name, sql
FROM sqlite_master
WHERE type='table'
AND sql LIKE '%CAMERA%';

select * 
from ZASSET z 
where z.Z_PK   in (70470, 89119, 224855, 238925, 302805, 339176)

select * 
from ZADDITIONALASSETATTRIBUTES z 
where z.ZORIGINALFILENAME = 'IMG_6782.HEIC'

select * 
from 
where Z_PK in (70470
224852
238926
302809
339174)


--update imports 
--set execution_id = null 
--where import_uuid = '72720';
--update imports 
--set status_code = null 
--where import_uuid = '72720';
--update month_batches 
--set status_code = '000'
--where "month" = '2025-08';
-- this query executes against the DB when mocking flag is not passed
SELECT pipeline_stage, full_description, code, script_name, transition_type
        FROM batch_status
        WHERE code GLOB '[0-9][0-9][0-9]'
          AND script_name NOT LIKE '%generate_month_batches.py%'
        ORDER BY code


-- mocking executor data
SELECT pipeline_stage || ' '|| full_description, code, script_name, transition_type
FROM batch_status
WHERE code GLOB '[0-9][0-9][0-9]'
ORDER BY code


-- batches status
select * 
from month_batches mb 			
order by mb."month" desc

-- get google favorites by month - passed stage 550
select count(), a."month" 
from assets a 
where a.google_favorite = 1
group by a."month" 
order by a."month" desc


-- reset flag so assets can be reuploaded - first month to be reuploaded - March 2025
update assets 
set uploaded_to_google = 0
where "month" = '2025-03'


select *
from assets a
where a.uploaded_to_google =1 and "month" = '2025-03'
order by a.updated_at_utc desc

select *
from assets a 

select * 
from planned_execution pe 

select max(a.updated_at_utc)
from assets a 
where a.uploaded_to_google = 1
and a."month" = '2025-08'


select *
from pipeline_executions pe 
order by executed_at_utc desc

select * 
from assets a 
where a.google_favorite = 1
order by a.updated_at_utc desc

select * 
from batch_status bs 
order by bs.code 

select *
from imports i 
--where i.execution_id  = 'f4ccf14a-c7dd-4d50-b0e1-b1d9fd8748f8'
order by i.import_uuid desc



select distinct a."month", i2.import_uuid, i2.execution_id, i2.status_code 
from assets a
join imports i2 on i2.import_uuid = a.import_id 
order by a."month" desc

-- inverse query
-- find for each month how many imports with not null status code
select distinct a."month"
from assets a
join imports i2 on i2.import_uuid = a.import_id 
where i2.status_code is null
order by a."month" desc
--2025-08	1
--2025-07	1
--2025-06	4
--2025-05	4
--2025-04	3
--2025-03	1
--2025-02	2
--2025-01	7
--2024-12	28


-- inverse query
-- find for each month how many imports with not null status code
select distinct a."month", count(distinct i2.import_uuid)
from assets a
join imports i2 on i2.import_uuid = a.import_id 
group by a."month" 
order by a."month" desc

-- which assets creation months are included in the import
select distinct i.import_uuid, a."month", i.status_code  
from imports i
join assets a on a.import_id = i.import_uuid 
order by cast(i.import_uuid as integer) desc, a."month" 


-- batch manager refactor
-- find latest import
select import_uuid, * 
from imports
order by import_uuid desc
-- latest import in 72720

-- which months are included in this import?
select distinct month 
from assets a 
where a.import_id = 72720
-- 2025-08




SELECT DISTINCT import_datetime_utc FROM photos_assets_view
WHERE import_datetime_utc IS NOT NULL


-- which import id are included in the month
-- these are months that executor need to update execution_id field 
SELECT DISTINCT pav.import_id, 
	(select count(*) from assets a where a.import_id = pav.import_id )
FROM photos_assets_view pav
order by import_id desc



SELECT DISTINCT strftime('%Y-%m', pav.creation_datetime_utc , 'localtime') as month, pav.import_id, 
	(select count(*) from assets a where a.import_id = pav.import_id ) as count_assets
--	,(select mb.assets_count from month_batches mb where mb."month" = strftime('%Y-%m', pav.creation_datetime_utc , 'localtime'))
FROM photos_assets_view pav
--group by strftime('%Y-%m', pav.creation_datetime_utc , 'localtime')
order by strftime('%Y-%m', pav.creation_datetime_utc , 'localtime') desc, pav.import_id 

-- which import id are included in the month
-- these are months that executor need to update execution_id field 
SELECT DISTINCT pav.import_id, 
	(select count(*) from assets a where a."month" = '2025-05' and a.import_id = pav.import_id )
FROM photos_assets_view pav
WHERE strftime('%Y-%m', import_datetime_utc, 'localtime') = '2025-05'

select mb."month", mb.latest_import_id , *
from pipeline_executions pe 
join month_batches mb on mb.id = pe.batch_month_id 
order by executed_at_utc desc

elect z.ZALLOWEDFORANALYSIS, z.ZIMPORTEDBY, z.ZFACEANALYSISVERSION, z.ZFACEREGIONS
from ZADDITIONALASSETATTRIBUTES z 

select z.ZMEDIAANALYSISATTRIBUTES, z.ZMOMENT, z.ZPHOTOANALYSISATTRIBUTES 
from ZASSET z 
order by 2 desc

select distinct a.month, 
	(select max(a2.import_id) from assets a2 where a2.month = a.month)
from assets a 
order by a."month" desc

-- photo batches script
select distinct a.month, a.import_id, count(*) as asset_count, 
	(select i.assets_count from month_batches mb where mb.month = a.month) as mb_asset_count
from assets a
join imports i on i.import_uuid = a.import_id  
group by "month" , a.import_id 
order by "month" desc, cast(a.import_id as integer) desc

select *
from imports i 
order by cast(import_uuid as integer) desc


select * 
from month_batches mb 

SELECT 
        a.ZUUID AS uuid,
        a.ZFILENAME AS filename,
        aaa.ZORIGINALFILENAME AS original_filename,
        a.ZIMPORTSESSION AS import_id,
        datetime(a.ZDATECREATED + 978307200, 'unixepoch') AS creation_datetime_utc,
        datetime(a.ZADDEDDATE + 978307200, 'unixepoch') AS import_datetime_utc
    FROM main.ZASSET a
    LEFT JOIN main.ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = a.Z_PK;


SELECT 
    a.ZUUID, 
    a.ZOVERALLAESTHETICSCORE, assets.aesthetic_score,
    aaa.ZORIGINALFILENAME, 
    datetime(a.ZDATECREATED + 978307200, 'unixepoch'),
    datetime(a.ZADDEDDATE + 978307200, 'unixepoch'),
    a.ZIMPORTSESSION,
    strftime('%Y-%m', datetime(a.ZDATECREATED + 978307200, 'unixepoch', 'localtime')) as month
FROM ZASSET a
LEFT JOIN ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = a.Z_PK
LEFT JOIN assets ON assets.asset_id = a.ZUUID
WHERE a.ZOVERALLAESTHETICSCORE IS NOT NULL
AND a.ZOVERALLAESTHETICSCORE != 0.5
AND (
    a.ZIMPORTSESSION > ?
    OR a.ZOVERALLAESTHETICSCORE != assets.aesthetic_score
)
order by datetime(a.ZDATECREATED + 978307200, 'unixepoch') desc


select * 
from assets a 
where "month" = '2025-07'
order by a.imported_date_utc desc

-- get the assets from 72720 import
select *
from assets a
where a.import_id = '72720'

-- 
delete FROM assets 
where import_id = '72720'

--  remove assets from the latest import with score 0.5
select distinct cast(a.import_id as integer)
from assets a 
where a.aesthetic_score = 0.5
order by a.import_id desc, a.imported_date_utc desc
-- 72720
-- 72678
-- 72672

-- find assets with default score
select a.original_filename, a.import_id, a.imported_date_utc, a.uploaded_to_google, a.google_favorite
from assets a 
where a.aesthetic_score = 0.5
order by a.import_id desc, a.imported_date_utc desc

-- find out what is in latest import
select *
from imports 
order by 2 desc
-- 145751	2025-09-18 02:55:16	72720	2025-09-18 02:55:16 UTC - Apple-iPhone 13 Pro Max		328

select * 
from assets
order by imported_date_utc desc

select datetime(ZADDEDDATE + 978307200, 'unixepoch'), z.ZIMPORTSESSION, z.ZCURATIONSCORE, z.ZOVERALLAESTHETICSCORE, z2.ZORIGINALFILENAME,  * 
from ZASSET z 
join ZADDITIONALASSETATTRIBUTES z2 on z2.ZASSET = z.Z_PK 
where z.ZIMPORTSESSION = 72720
order by z.ZOVERALLAESTHETICSCORE desc

select datetime(ZADDEDDATE + 978307200, 'unixepoch'), *
from ZASSET
order by 1 desc

select count(*)
from ZADDITIONALASSETATTRIBUTES

select count(*)
from ZIMPORTSESSION

select *
from imports 
order by 2 desc

update db_updates
set raw_synced = 0
where id = 14

select *
from schema_migrations sm 

select * 
from db_updates

-- check if google_favorites are added to the assets table
select a.google_favorite, *
from assets a
where "month" = "2025-07" and a.google_favorite 
order by a.aesthetic_score desc


SELECT month, status_code, *
FROM month_batches
order by status_code desc, month desc

SELECT (
    SELECT i.import_uuid
    FROM assets a
    JOIN imports i ON a.import_id = i.import_uuid
    WHERE a.month = mb2.month
    ORDER BY i.import_uuid DESC
    LIMIT 1
) AS latest_import,
mb2.month
FROM month_batches mb2
WHERE mb2.month < strftime('%Y-%m', 'now')
  AND mb2.status_code IS NOT NULL
  AND EXISTS (
    SELECT 1 FROM batch_status bs
    WHERE bs.preceding_code = mb2.status_code
      AND bs.transition_type = "pipeline"
      AND bs.code NOT LIKE '%E'
  )
ORDER BY mb2.month DESC
LIMIT 1;

select *
from batch_status

SELECT code
FROM batch_status
WHERE preceding_code IS NOT NULL
  AND LENGTH(code) = 3
  AND transition_type = 'pipeline'
ORDER BY code DESC
LIMIT 1


SELECT DISTINCT i.import_uuid, a.month, a.original_filename 
FROM imports i
LEFT JOIN assets a ON a.import_id = i.import_uuid
LEFT JOIN month_batches m ON m.month = a.month
WHERE (latest_import_id < i.import_uuid OR latest_import_id IS NULL)
AND (m.status_code < (
        SELECT code
        FROM batch_status
        WHERE preceding_code IS NOT NULL
          AND LENGTH(code) = 3
          AND transition_type = 'pipeline'
        ORDER BY code DESC
        LIMIT 1
    ) OR m.status_code IS NULL)
  -- exclude current month to avoid incomplete batch
  AND a.month < strftime('%Y-%m', 'now')
ORDER BY i.import_uuid DESC, a.month DESC
LIMIT 1;

SELECT month FROM month_batches WHERE status_code = 399 ORDER BY month DESC

SELECT album_name FROM smart_albums

        
SELECT month, status_code FROM month_batches ORDER BY month;

--  get latest import and month
SELECT DISTINCT i.import_uuid, a.month
FROM imports i
LEFT JOIN assets a ON a.import_id = i.import_uuid
LEFT JOIN month_batches m ON m.month = a.month
WHERE (latest_import_id < i.import_uuid OR latest_import_id IS NULL)

AND (m.status_code < ( -- we want the code to be smaller than the largest code - this means incomplete batch
        SELECT code
        FROM batch_status
        WHERE preceding_code IS NOT NULL
        AND LENGTH(code) = 3
        ORDER BY code DESC
        LIMIT 1
    ) OR m.status_code IS NULL)
-- exclude current month to avoid incomplete batch
AND a.month < strftime('%Y-%m', 'now')
ORDER BY i.import_uuid DESC, a.month DESC
LIMIT 1;

SELECT *
FROM assets
WHERE strftime('%Y-%m', datetime(strftime('%s', date_created_utc), 'unixepoch', 'localtime')) != month
order by assets.date_created_utc desc

        DELETE FROM assets
        WHERE strftime('%Y-%m', datetime(strftime('%s', date_created_utc), 'unixepoch', 'localtime')) != month



SELECT 
            a.ZUUID, 
            a.ZOVERALLAESTHETICSCORE, 
            aaa.ZORIGINALFILENAME, 
            datetime(a.ZDATECREATED + 978307200, 'unixepoch'),
            datetime(a.ZADDEDDATE + 978307200, 'unixepoch'),
            a.ZIMPORTSESSION,
            strftime('%Y-%m', datetime(a.ZDATECREATED + 978307200, 'unixepoch', 'localtime')) as month
        FROM ZASSET a
        LEFT JOIN ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = a.Z_PK
        WHERE a.ZOVERALLAESTHETICSCORE IS NOT NULL
        AND a.ZIMPORTSESSION >= 72678

-- assets in a month
SELECT COUNT(*) FROM assets WHERE month = '2025-08'
SELECT date_created_utc, *  FROM assets WHERE month = '2025-08' 
AND (date_captured IS NOT NULL AND date_captured != '')


select *
from assets a 
where month = '2025-05'
and a.uploaded_to_google = 1
order by a.updated_at_utc 

--
select * 
from schema_migrations sm 

-- reversing processing order - newer scripts at the top
select * from assets a
order by a.aesthetic_score desc

select  distinct i.import_uuid, a.month
from imports i 
left join assets a on a.import_id = i.import_uuid
left join month_batches m on m.month = a.month
where latest_import_id < i.import_uuid or latest_import_id is null
and m.status_code < (SELECT code
                        FROM batch_status
                        WHERE preceding_code IS NOT NULL
                            and length(code) = 3
                            order by code desc
                        limit 1) or m.status_code is null
order by i.import_uuid desc, a.month desc
limit 1;


SELECT * FROM planned_execution 
WHERE active = 1;

select *
from imports i

select * 
from batch_status b 


        select  distinct i.import_uuid, a.month
        from imports i 
        left join assets a on a.import_id = i.import_uuid
        left join month_batches m on m.month = a.month
        where (latest_import_id < i.import_uuid or latest_import_id is null)
        and m.status_code < (SELECT code
                                FROM batch_status
                                WHERE preceding_code IS NOT NULL
                                    and length(code) = 3
                                    order by code desc
                                limit 1) or m.status_code is null
        order by i.import_uuid desc, a.month desc        
        limit 1;





SELECT code, preceding_code, full_description
FROM batch_status
WHERE preceding_code IS NOT NULL
  AND code NOT LIKE '%E'

SELECT mb.month, mb.status_code, bs.short_label
FROM month_batches mb
LEFT JOIN batch_status bs ON mb.status_code = bs.code
ORDER BY mb.month

select *
from batch_status b 

select * 
from planned_execution p 

ALTER TABLE planned_execution ADD COLUMN active INTEGER NOT NULL DEFAULT 0;
-- find next batch to process
-- need to find an import that has a month which has latest import smaller than the current import id or none
select distinct i.import_uuid, a.month, m.latest_import_id, m.status_code
from imports i 
left join assets a on a.import_id = i.import_uuid
left join month_batches m on m.month = a.month
where latest_import_id < i.import_uuid or latest_import_id is null
order by i.import_uuid desc, a.month desc

-- code query
select  distinct i.import_uuid, a.month
from imports i 
left join assets a on a.import_id = i.import_uuid
left join month_batches m on m.month = a.month
where latest_import_id < i.import_uuid or latest_import_id is null
and m.status_code < (SELECT code
						FROM batch_status
						WHERE preceding_code IS NOT NULL
							and length(code) = 3
							order by code desc
						limit 1)
order by i.import_uuid desc, a.month desc
limit 1;

CREATE TABLE IF NOT EXISTS planned_execution (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    planned_month TEXT,
    set_at_utc TEXT DEFAULT (datetime('now'))
);


select distinct import_id, month
from assets
order by import_id desc, month desc

-- get non-error stage transitions
SELECT code, preceding_code, full_description
FROM batch_status
WHERE preceding_code IS NOT NULL
	and length(code) = 3
order by code;

-- latest developed stage
SELECT code
FROM batch_status
WHERE preceding_code IS NOT NULL
	and length(code) = 3
order by code desc
limit 1;


-- get latest import and month
select * 
from imports
order by import_uuid desc
-- import id = 72672

-- find import devices
select import_uuid, cast(substr(import_name, 26) as text)
from imports 

--update month_batches
--set latest_import_id = NULL


-- group assets by import and by date created
select import_id, count(*), SUBSTR(datetime(imported_date_utc, 'localtime'),0,8) as dateImported
--substr(datetime(imported_date_utc + 978307200, 'unixepoch'),0,8) as dateImported
from assets
group by import_id, dateImported
order by import_id desc, dateImported desc



-- upload_to_google_photos set score_imported_at_utc timestamp
select * 
from assets
where score_imported_at_utc is null
order by import_id desc




select b.code
from batch_status b 
where b.preceding_code = 100 and length(code) = 3

SELECT mb.month, mb.status_code , bs.code, bs.preceding_code
FROM month_batches mb
LEFT JOIN batch_status bs ON mb.status_code = bs.code
--        WHERE mb.status_code = ?
ORDER BY mb.month DESC


SELECT mb.month, bs.code, bs.preceding_code
FROM month_batches mb
JOIN batch_status bs ON mb.status_code = bs.preceding_code
--WHERE bs.preceding_code = '100'
ORDER BY mb.month DESC

LIMIT 1;


SELECT mb.month, mb.status_code, bs.code, bs.preceding_code
FROM month_batches mb
JOIN batch_status bs ON mb.status_code = bs.preceding_code
WHERE bs.code = '200';

select * 
from schema_migrations sm 

select * 
from assets a 

SELECT * FROM photos_assets_view

SELECT strftime('%Y- %m', creation_datetime)
FROM photos_assets_view

WHERE strftime('%Y-%m', creation_datetime) = '2025-05'

select * 
from pipeline_executions pe 
order by id desc

SELECT month FROM month_batches
        WHERE status_code = '000'
        ORDER BY month DESC
        LIMIT 1;

select *
from batch_status bs
order by code

select *
from month_batches mb 

-- need to modify the view - need to consider import and asset creation date
-- summary
SELECT 
    count(a.ZUUID) AS uuid_count,
    count(distinct a.ZIMPORTSESSION) AS import_id,
    -- substr(datetime((a.ZADDEDDATE + 978303599.796), 'unixepoch', 'localtime'),0,8) as import_datetime,
    substr(datetime(a.ZDATECREATED + 978307200, 'unixepoch'),0,8) as dtCreated
FROM main.ZASSET a
LEFT JOIN main.ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = a.Z_PK
group by 
	-- import_datetime, 
	--	import_id,
	dtCreated
order by 
	-- import_datetime desc, 
	dtCreated desc, import_id desc;

-- details
SELECT 
                a.ZUUID AS uuid,
                a.ZFILENAME AS filename,
                aaa.ZORIGINALFILENAME AS original_filename,
                a.ZIMPORTSESSION AS import_id,
                datetime((a.ZADDEDDATE + 978303599.796), 'unixepoch', 'localtime') as import_datetime,
                datetime(a.ZDATECREATED + 978307200, 'unixepoch') dtCreated
            FROM main.ZASSET a
            LEFT JOIN main.ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = a.Z_PK
order by a.zimportsession DESC;

-- tracking uploaded assets
select *
from assets


-- Step 1: Rename the original table
ALTER TABLE schema_migrations RENAME TO schema_migrations_old;

-- Step 2: Create a new table with filename allowing NULL
CREATE TABLE schema_migrations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    filename TEXT, -- formerly NOT NULL
    applied_at_utc TEXT,
    status TEXT DEFAULT 'applied',
    description TEXT DEFAULT NULL,
    migration TEXT
);

-- Step 3: Copy data over
INSERT INTO schema_migrations (id, filename, applied_at_utc, status, description, migration)
SELECT id, filename, applied_at_utc, status, description, filename
FROM schema_migrations_old;

-- Step 4: Drop the old table
DROP TABLE schema_migrations_old;



ALTER TABLE schema_migrations ADD COLUMN migration TEXT;

SELECT migration, applied_at_utc FROM schema_migrations ORDER BY applied_at_utc DESC LIMIT 1

SELECT migration, applied_at_utc FROM schema_migrations ORDER BY applied_at_utc DESC LIMIT 1

SELECT migration, applied_at_utc FROM schema_migrations ORDER BY applied_at_utc DESC LIMIT 1

SELECT migration FROM schema_migrations;

-- view definition
SELECT 
                a.ZUUID AS uuid,
                a.ZFILENAME AS filename,
                aaa.ZORIGINALFILENAME AS original_filename,
                a.ZIMPORTSESSION AS import_id,
                datetime((a.ZADDEDDATE + 978303599.796), 'unixepoch', 'localtime') as import_datetime
            FROM main.ZASSET a
            LEFT JOIN main.ZADDITIONALASSETATTRIBUTES aaa ON aaa.ZASSET = a.Z_PK
order by a.zimportsession DESC;

select DISTINCT strftime('%Y-%m', import_datetime) as month, count(uuid)
from photos_assets_view pav 
group by month

select *
from batch_status bs 

select * 
from month_batches mb 


select * 
from schema_migrations sm 

update schema_migrations
set applied_at_utc = NULL

-- adding columns to schema_migration
ALTER TABLE schema_migrations ADD COLUMN status TEXT DEFAULT 'applied';
ALTER TABLE schema_migrations ADD COLUMN description TEXT DEFAULT NULL;
ALTER TABLE schema_migrations ADD COLUMN applied_at_utc TEXT DEFAULT NULL;


-- generate batches module scripts
SELECT DISTINCT month FROM month_batches;

SELECT DISTINCT strftime('%Y-%m', import_datetime) as month
FROM photos_assets_view
ORDER BY month ASC;

delete from month_batches;

-- get_next_batch in verify_smart_album
SELECT month, created_at, status 
FROM month_batches
--WHERE status = 'pending'
ORDER BY created_at ASC

LIMIT 1;



SELECT month FROM month_batches
WHERE status = 'pending'
ORDER BY month DESC
LIMIT 1;

select * 
from month_batches mb 

select * 
from smart_albums sa 

select * 
from assets a 
where a.uploaded_to_google = 1 or "month" = '2025-03'
order by a.date_created_utc desc

select * 
from imports i 

select * 
from logs l 

select * 
from smart_albums sa 

DROP TABLE IF EXISTS imports;
DROP TABLE IF EXISTS month_batches;
DROP TABLE IF EXISTS assets;
DROP TABLE IF EXISTS logs;
DROP TABLE IF EXISTS smart_albums;

select * 
from assets a 
where a.original_filename  = '428EC94C-1DC2-4959-B914-EFCC56CEA15D.JPG'
order by a.date_created_utc desc

SELECT original_filename, month, import_id, aesthetic_score, date_created_utc, imported_date_utc
FROM assets

 SELECT original_filename, month, import_id, aesthetic_score, date_created_utc, imported_date_utc
        FROM assets
        WHERE month = '2025-03'

select date_created_utc, * 
from assets a 
where a.original_filename = 'IMG_4112.HEIC' and a.date_created_utc = '2025-03-01 13:52:17'

update assets
set uploaded_to_google = 1
where "month" = '2025-03'

-- 51 favorites in March 2025
select aesthetic_score, google_favorite, * 
from assets a 
where "month" = '2025-03' and a.google_favorite 
order by a.date_created_utc desc

-- 
SELECT aesthetic_score, google_favorite,
    (a.aesthetic_score / 100.0) + (CASE WHEN a.google_favorite = 1 THEN 0.5 ELSE 0 END) AS score_normalized,
    *
FROM assets a
where a."month" = '2025-03'
ORDER BY (a.aesthetic_score / 100.0) + (CASE WHEN a.google_favorite = 1 THEN 0.5 ELSE 0 END) desc

--where a.month = '2025-03' and a.google_favorite 
--ORDER BY where "month" = '2025-03' and a.google_favorite 

SELECT 
    a.original_filename,
    a.aesthetic_score,
    a.google_favorite,
    a.aesthetic_score * .875  + a.google_favorite * 0.125 AS score_normalized,
    month
FROM assets a
WHERE a."month" in ('2025-03', '2025-04')
order by month, score_normalized desc

select * from ranked_assets_view rav 
where month = '2025-03'

select *
from month_batches mb 

update month_batches 
set status = 'pending'
where "month" = '2025-04'

select * 
from assets a 
where a.google_favorite = 1

-- pull_google_favorites
select month
from month_batches mb 
where status = 'uploaded'
order by month 
limit 1

select * 
from assets

select * 
from schema_migrations sm 

select * 
from imports
order by import_uuid desc

SELECT 
    v.asset_id,
    v.MomentsAlbumName,
    v.score_normalized,
    v.original_filename,
    v.aesthetic_score,
    v.google_favorite,
    v.mobile_apple_photos_featured_photos,
    v.apple_photos_monthly_selection,
    (SELECT 1 FROM moment_exports me 
     WHERE me.asset_id = v.asset_id AND me.curation_stage = 'to_be_curated') AS is_proposed,
    (SELECT 1 FROM moment_exports me 
     WHERE me.asset_id = v.asset_id AND me.curation_stage = 'curated') AS is_curated,
    ast.curated_album,
    (SELECT 1 FROM publications p 
     WHERE p.asset_id = v.asset_id LIMIT 1) AS is_published
FROM ranked_assets_view v
JOIN assets ast ON v.asset_id = ast.asset_id
JOIN month_batches mb ON v.month = mb.month
WHERE mb.status_code >= '600' 
  AND v.MomentsAlbumName IS NOT NULL 
  AND v.MomentsAlbumName != ''
  AND LOWER(v.MomentsAlbumName) NOT IN ('skippublishing', 'ignore')
  AND v.score_normalized > :effective_threshold
ORDER BY v.score_normalized DESC;
