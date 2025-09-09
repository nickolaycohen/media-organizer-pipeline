SELECT DISTINCT i.import_uuid, a.month
FROM imports i
LEFT JOIN assets a ON a.import_id = i.import_uuid
LEFT JOIN month_batches m ON m.month = a.month
WHERE (latest_import_id < i.import_uuid OR latest_import_id IS NULL)
AND (m.status_code < (
        SELECT code
        FROM batch_status
        WHERE preceding_code IS NOT NULL
          AND LENGTH(code) = 3
          AND transition_type = ?
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


SELECT month, status_code, *
FROM month_batches


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

-- get batch statuses
SELECT month, status_code, latest_import_id 
FROM month_batches
order by month desc;

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