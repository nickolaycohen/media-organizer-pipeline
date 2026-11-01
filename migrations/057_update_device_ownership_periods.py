def run(conn):
    cursor = conn.cursor()
    print("🔄 Running migration 057: Updating ownership periods from asset ranges for single-owner devices...")

    updates = [
        ('Ani Ivanova', 'E5700', '2003-08-16', '2023-06-19'),
        ('Benny', 'SCH-I800', '2011-07-02', '2013-03-01'),
        ('Benny', 'Samsung SCH-I800', '2011-07-02', '2013-03-01'),
        ('Benny', 'SGH-A817', '2011-07-09', '2013-09-27'),
        ('Benny', 'Samsung SGH-A817', '2011-07-09', '2013-09-27'),
        ('Benny', 'iPod touch', '2012-04-10', '2012-04-22'),
        ('Benny', 'iPad', '2013-05-12', '2017-09-30'),
        ('Benny', 'SCH-I535', '2013-04-13', '2016-11-28'),
        ('Benny', 'iPad mini', '2014-06-30', '2014-09-24'),
        ('Benny', 'SCH-I435', '2015-11-15', '2016-06-15'),
        ('Benny', 'iPad mini 2', '2015-02-15', '2020-05-18'),
        ('Benny', 'iPhone 6s', '2016-07-05', '2020-06-08'),
        ('Benny', 'iPhone 7', '2017-07-04', '2021-05-02'),
        ('Benny', 'iPad (5th generation)', '2017-10-10', '2025-01-17'),
        ('Benny', 'iPhone SE (2nd generation)', '2020-07-03', '2021-05-08'),
        ('Benny', 'iPhone 12', '2021-05-09', '2024-12-31'),
        ('Cohens', 'KODAK DX6490 ZOOM DIGITAL CAMERA', '2004-06-18', '2005-06-21'),
        ('Cohens', 'DSC-T1', '2004-06-04', '2008-12-28'),
        ('Cohens', 'KODAK EASYSHARE M753 ZOOM DIGITAL CAMERA', '2007-02-14', '2012-01-06'),
        ('Cohens', 'KODAK EASYSHARE M753', '2007-02-14', '2012-01-06'),
        ('Cohens', 'DSC-T300', '2008-04-17', '2012-06-10'),
        ('Cohens', 'NIKON 1 J1', '2012-07-11', '2018-08-20'),
        ('Cohens Bulgaria', '<VLUU L730  / Samsung L730>', '2007-01-01', '2016-05-23'),
        ('Cohens Bulgaria', 'Samsung L730', '2007-01-01', '2016-05-23'),
        ('Cohens Bulgaria', 'DSC-H2', '2007-04-06', '2015-10-17'),
        ('Doris', 'iPhone 8', '2020-04-13', '2023-03-12'),
        ('Eva', 'MotoE2(4G-LTE)', '2016-02-17', '2017-08-25'),
        ('Goro', 'iPhone 8 Plus', '2018-04-11', '2022-07-18'),
        ('Ilian Stojanov', 'EX-S600', '2005-01-02', '2006-06-18'),
        ('Lee', 'KODAK EASYSHARE M341 Digital Camera', '2010-07-04', '2010-07-04'),
        ('Lee', 'KODAK EASYSHARE M341', '2010-07-04', '2010-07-04'),
        ('Michelle', 'Canon EOS DIGITAL REBEL', '2006-02-22', '2006-02-22'),
        ('Niki', 'W580i', '2009-01-29', '2009-07-12'),
        ('Niki', 'Sony Ericsson W580i', '2009-01-29', '2009-07-12'),
        ('Niki', 'DROIDX 225a0001ffd800000a3a7de10201a02c', '2010-12-13', '2011-06-30'),
        ('Niki', 'DROIDX', '2011-04-30', '2017-07-16'),
        ('Niki', 'iPhone 4', '2012-07-04', '2017-05-30'),
        ('Niki', 'HDR-AS30V', '2013-01-01', '2018-12-29'),
        ('Niki', 'Sony Action Cam HDR-AS30V', '2013-01-01', '2018-12-29'),
        ('Niki', 'SCH-I545', '2013-10-10', '2014-08-24'),
        ('Niki', 'iPad Air', '2014-10-15', '2017-06-30'),
        ('Niki', 'SM-G920V', '2015-04-11', '2016-05-17'),
        ('Roberto', 'COOLPIX S3700', '1980-01-01', '2022-07-20'),
        ('Sid Father', 'Canon EOS 5D Mark IV', '2019-11-16', '2019-11-16'),
        ('Bianca', 'iPhone 5s', '2016-07-02', '2017-12-25'),
        ('Tedi i Vessi', 'FE5020,X935', '2010-01-30', '2012-11-02'),
        ('Tedi i Vessi', 'FE5020,X935            ', '2010-01-30', '2012-11-02'),
        ('Tedi i Vessi', 'Olympus FE-5020', '2010-01-30', '2012-11-02'),
        ('Tedi i Vessi', 'FE-5020', '2010-01-30', '2012-11-02'),
        ('Nickolay Vassilev', 'DMC-FZ40', '2011-05-30', '2012-01-01'),
        ('Nickolay Vassilev', 'Panasonic DMC-FZ40', '2011-05-30', '2012-01-01'),
    ]

    updated_count = 0
    for owner, model, s_date, e_date in updates:
        cursor.execute("""
            UPDATE device_owners
            SET start_date = ?, end_date = ?
            WHERE owner_name = ? AND camera_model = ?
        """, (s_date, e_date, owner, model))
        if cursor.rowcount > 0:
            updated_count += cursor.rowcount

    conn.commit()
    print(f"✅ Migration 057 complete: {updated_count} device ownership periods updated.")

