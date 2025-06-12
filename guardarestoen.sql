------------------------------------------------------------------------------
------- ALL FOR CSV -----


WITH t1 AS (
    SELECT
        CAST(
            FROM_UNIXTIME(
                ROUND(CAST(r."_dlt_load_id" AS DOUBLE), 3)
            ) AS TIMESTAMP(3)
        ) AS dlt_ingestion_timestamp,
        r.*, 
        rdr.*, 
        rp.data__store_info__ratings__num_of_reviews
    FROM raw_portal_public_glovo.restaurant r
    LEFT JOIN raw_portal_public_glovo.restaurant__data__restaurants rdr
        ON r."_dlt_id" = rdr."_dlt_parent_id"
    LEFT JOIN raw_portal_public_glovo.restaurant__data__restaurants__cards rdrc
        ON rdr."_dlt_id" = rdrc."_dlt_parent_id"
    LEFT JOIN raw_portal_public_glovo.restaurant__data__restaurants__filters rdrf
        ON rdr."_dlt_id" = rdrf."_dlt_parent_id"
    LEFT JOIN raw_portal_public_glovo.restaurant_product rp
        ON rdr.id = CAST(rp.data__store_id AS BIGINT)
    WHERE DATE(CAST(FROM_UNIXTIME(ROUND(CAST(r."_dlt_load_id" AS DOUBLE), 3)) AS TIMESTAMP(3))) = date

)

SELECT DISTINCT
    t1.source_city, 
    t1.store_name, 
    t1.data__store_info__ratings__num_of_reviews, 
    t1.ratings__score, 
    t1.filters, 
    t1.is_new, 
    t1.phone_number 
FROM t1
WHERE t1.data__store_info__ratings__num_of_reviews IS NULL;

----------------------------------------------------------------------
-----------------------------------------------------------------------



--Joan Composition

select  COUNT(*) AS total
FROM raw_portal_public_glovo.restaurant r
LEFT JOIN raw_portal_public_glovo.restaurant__data__restaurants rdr
    ON r."_dlt_id" = rdr."_dlt_parent_id"
LEFT JOIN raw_portal_public_glovo.restaurant__data__restaurants__cards rdrc
    ON rdr."_dlt_id" = rdrc."_dlt_parent_id"
LEFT JOIN raw_portal_public_glovo.restaurant__data__restaurants__filters rdrf
    ON rdr."_dlt_id" = rdrf."_dlt_parent_id"
LEFT JOIN raw_portal_public_glovo.restaurant_product rp
    ON rdr.id = CAST(rp.data__store_id AS BIGINT)
WHERE DATE(CAST(FROM_UNIXTIME(ROUND(CAST(r."_dlt_load_id" AS DOUBLE), 3)) AS TIMESTAMP(3))) = CURRENT_DATE;

-------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------


WITH t1 AS (
    SELECT
        CAST(
            FROM_UNIXTIME(
                ROUND(CAST(r."_dlt_load_id" AS DOUBLE), 3)
            ) AS TIMESTAMP(3)
        ) AS dlt_ingestion_timestamp,
        r.*, 
        rdr.*, 
        rp.data__store_info__ratings__num_of_reviews
    FROM raw_portal_public_glovo.restaurant r
    LEFT JOIN raw_portal_public_glovo.restaurant__data__restaurants rdr
        ON r."_dlt_id" = rdr."_dlt_parent_id"
    LEFT JOIN raw_portal_public_glovo.restaurant__data__restaurants__cards rdrc
        ON rdr."_dlt_id" = rdrc."_dlt_parent_id"
    LEFT JOIN raw_portal_public_glovo.restaurant__data__restaurants__filters rdrf
        ON rdr."_dlt_id" = rdrf."_dlt_parent_id"
    LEFT JOIN raw_portal_public_glovo.restaurant_product rp
        ON rdr.id = CAST(rp.data__store_id AS BIGINT)
    WHERE DATE(CAST(FROM_UNIXTIME(ROUND(CAST(r."_dlt_load_id" AS DOUBLE), 3)) AS TIMESTAMP(3))) = CURRENT_DATE
)
---------------------------------------
---------------------------------------
--Debbuging
SELECT 
    COUNT(DISTINCT t1.store_name) AS total_distinct_restaurants
FROM t1;



SELECT 
    COUNT(DISTINCT p.data__store_id) AS distinct_store_ids, 
    COUNT(*) as todo,
    COUNT(DISTINCT p.data__store_address_id) AS distinct_store_address_ids
FROM raw_portal_public_glovo.restaurant_product p
WHERE DATE(CAST(FROM_UNIXTIME(ROUND(CAST(p."_dlt_load_id" AS DOUBLE), 3)) AS TIMESTAMP(3))) = CURRENT_DATE;



-----------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------


WITH t1 AS (
    SELECT
        CAST(
            FROM_UNIXTIME(
                ROUND(CAST(r."_dlt_load_id" AS DOUBLE), 3)
            ) AS TIMESTAMP(3)
        ) AS dlt_ingestion_timestamp,
        r.*, 
        rdr.*, 
        rp.data__store_info__ratings__num_of_reviews,
        CONCAT('https://glovoapp.com', rp.restaurant_url_path) AS full_restaurant_url
    FROM raw_portal_public_glovo.restaurant r
    LEFT JOIN raw_portal_public_glovo.restaurant__data__restaurants rdr
        ON r."_dlt_id" = rdr."_dlt_parent_id"
    LEFT JOIN raw_portal_public_glovo.restaurant__data__restaurants__cards rdrc
        ON rdr."_dlt_id" = rdrc."_dlt_parent_id"
    LEFT JOIN raw_portal_public_glovo.restaurant__data__restaurants__filters rdrf
        ON rdr."_dlt_id" = rdrf."_dlt_parent_id"
    LEFT JOIN raw_portal_public_glovo.restaurant_product rp
        ON rdr.id = CAST(rp.data__store_id AS BIGINT)
    WHERE DATE(CAST(FROM_UNIXTIME(ROUND(CAST(r."_dlt_load_id" AS DOUBLE), 3)) AS TIMESTAMP(3))) = CURRENT_DATE
)
SELECT DISTINCT
    t1.source_city, 
    t1.store_name, 
    t1.data__store_info__ratings__num_of_reviews, 
    t1.ratings__score, 
    t1.filters, 
    t1.is_new, 
    t1.phone_number,
    t1.full_restaurant_url
FROM t1
WHERE t1.data__store_info__ratings__num_of_reviews IS NULL;

-------------------------------------------------------------------------------------------------------------------------



WITH t1 AS (
    SELECT
        CAST(
            FROM_UNIXTIME(
                ROUND(CAST(r."_dlt_load_id" AS DOUBLE), 3)
            ) AS TIMESTAMP(3)
        ) AS dlt_ingestion_timestamp,
        r.*, 
        rdr.*, 
        rp.data__store_info__ratings__num_of_reviews,
        CONCAT('https://glovoapp.com', rp.restaurant_url_path) AS full_restaurant_url,
        rp.restaurant_url_path
    FROM raw_portal_public_glovo.restaurant r
    LEFT JOIN raw_portal_public_glovo.restaurant__data__restaurants rdr
        ON r."_dlt_id" = rdr."_dlt_parent_id"
    LEFT JOIN raw_portal_public_glovo.restaurant__data__restaurants__cards rdrc
        ON rdr."_dlt_id" = rdrc."_dlt_parent_id"
    LEFT JOIN raw_portal_public_glovo.restaurant__data__restaurants__filters rdrf
        ON rdr."_dlt_id" = rdrf."_dlt_parent_id"
    LEFT JOIN raw_portal_public_glovo.restaurant_product rp
        ON rdr.id = CAST(rp.data__store_id AS BIGINT)
    WHERE DATE(CAST(FROM_UNIXTIME(ROUND(CAST(r."_dlt_load_id" AS DOUBLE), 3)) AS TIMESTAMP(3))) = CURRENT_DATE
)
SELECT DISTINCT
    t1.source_city, 
    t1.store_name, 
    t1.data__store_info__ratings__num_of_reviews, 
    t1.ratings__score, 
    t1.filters, 
    t1.is_new, 
    t1.phone_number,
    t1.full_restaurant_url
FROM t1
WHERE t1.restaurant_url_path IS NULL;



--------------------------------------------------------------------------------------------------------------------------






------------------------Debugueando desde restaurants-----------------

select *
from raw_portal_public_glovo.restaurant r
where  DATE(CAST(FROM_UNIXTIME(ROUND(CAST(r."_dlt_load_id" AS DOUBLE), 3)) AS TIMESTAMP(3))) = CURRENT_DATE

-----
SELECT *
FROM raw_portal_public_glovo.restaurant__data__restaurants r
JOIN raw_portal_public_glovo.restaurant o
  ON r._dlt_parent_id = o._dlt_id
WHERE DATE(CAST(FROM_UNIXTIME(ROUND(CAST(o."_dlt_load_id" AS DOUBLE), 3)) AS TIMESTAMP)) = CURRENT_DATE;




---------------------------------------------------------------------------------------------------------------------------
select *
from raw_portal_public_glovo.restaurant_product r
where r.restaurant_url_path is null
and DATE(CAST(FROM_UNIXTIME(ROUND(CAST(r."_dlt_load_id" AS DOUBLE), 3)) AS TIMESTAMP(3))) = CURRENT_DATE



------------------------------
SELECT *
FROM raw_portal_public_glovo.restaurant__data__restaurants r
JOIN raw_portal_public_glovo.restaurant o
  ON r._dlt_parent_id = o._dlt_id
WHERE DATE(CAST(FROM_UNIXTIME(ROUND(CAST(o."_dlt_load_id" AS DOUBLE), 3)) AS TIMESTAMP)) = CURRENT_DATE;   Pero aqui si WITH t1 AS (
    SELECT
        CAST(
            FROM_UNIXTIME(
                ROUND(CAST(r."_dlt_load_id" AS DOUBLE), 3)
            ) AS TIMESTAMP(3)
        ) AS dlt_ingestion_timestamp,
        r.*, 
        rdr.*, 
        rp.data__store_info__ratings__num_of_reviews,
        CONCAT('https://glovoapp.com', rp.restaurant_url_path) AS full_restaurant_url
    FROM raw_portal_public_glovo.restaurant r
    LEFT JOIN raw_portal_public_glovo.restaurant__data__restaurants rdr
        ON r."_dlt_id" = rdr."_dlt_parent_id"
    LEFT JOIN raw_portal_public_glovo.restaurant__data__restaurants__cards rdrc
        ON rdr."_dlt_id" = rdrc."_dlt_parent_id"
    LEFT JOIN raw_portal_public_glovo.restaurant__data__restaurants__filters rdrf
        ON rdr."_dlt_id" = rdrf."_dlt_parent_id"
    LEFT JOIN raw_portal_public_glovo.restaurant_product rp
        ON rdr.id = CAST(rp.data__store_id AS BIGINT)
    WHERE DATE(CAST(FROM_UNIXTIME(ROUND(CAST(r."_dlt_load_id" AS DOUBLE), 3)) AS TIMESTAMP(3))) = CURRENT_DATE
)
SELECT DISTINCT
    t1.source_city, 
    t1.store_name, 
    t1.data__store_info__ratings__num_of_reviews, 
    t1.ratings__score, 
    t1.filters, 
    t1.is_new, 
    t1.phone_number,
    t1.full_restaurant_url
FROM t1
WHERE t1.data__store_info__ratings__num_of_reviews IS NULL;

--------------------------------
SELECT
    rdr.id AS rdr_id,
    CAST(rp.data__store_id AS BIGINT) AS store_id,
    rp.data__store_info__ratings__num_of_reviews,
    rp.restaurant_url_path
FROM raw_portal_public_glovo.restaurant__data__restaurants rdr
LEFT JOIN raw_portal_public_glovo.restaurant_product rp
    ON rdr.id = CAST(rp.data__store_id AS BIGINT)
WHERE DATE(CAST(FROM_UNIXTIME(ROUND(CAST(rdr."_dlt_load_id" AS DOUBLE), 3)) AS TIMESTAMP(3))) = CURRENT_DATE
  AND rp.data__store_info__ratings__num_of_reviews IS NULL;





--------------------------
 de rd 

WITH t1 AS (
  SELECT 
    rr.*, 
    rd."_dlt_load_id" AS dlt_load_id
  FROM raw_portal_public_glovo.restaurant__data__restaurants rr
  LEFT JOIN raw_portal_public_glovo.restaurant rd
    ON rr."_dlt_parent_id" = rd."_dlt_id"
), 
t2 AS (
  SELECT * 
  FROM t1
  WHERE DATE(CAST(FROM_UNIXTIME(ROUND(CAST(t1.dlt_load_id AS DOUBLE), 3)) AS TIMESTAMP(3))) = DATE('2025-05-29')
)
SELECT DISTINCT
  t2.*,  
  r2.data__store_info__ratings__num_of_reviews
FROM t2
left JOIN raw_portal_public_glovo.restaurant_product r2
  ON CAST(t2.id AS VARCHAR) = r2.data__store_id 
  and DATE(CAST(FROM_UNIXTIME(ROUND(CAST(r2._dlt_load_id AS DOUBLE), 3)) AS TIMESTAMP(3))) = DATE('2025-05-29')
where r2.data__store_info__ratings__num_of_reviews is null;



--------------------
select * from raw_portal_public_glovo.restaurant_product rp 
where rp.data__store_info__ratings__num_of_reviews is null 
and DATE(CAST(FROM_UNIXTIME(ROUND(CAST(rp._dlt_load_id AS DOUBLE), 3)) AS TIMESTAMP(3))) = DATE('2025-05-29');




WITH t1 AS (
  SELECT 
    rr.*, 
    rd."_dlt_load_id" AS dlt_load_id
  FROM raw_portal_public_glovo.restaurant__data__restaurants rr
  LEFT JOIN raw_portal_public_glovo.restaurant rd
    ON rr."_dlt_parent_id" = rd."_dlt_id"
), 
t2_ranked AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY id ORDER BY dlt_load_id DESC) AS rn
  FROM t1
  WHERE DATE(CAST(FROM_UNIXTIME(ROUND(CAST(dlt_load_id AS DOUBLE), 3)) AS TIMESTAMP(3))) = DATE('2025-05-29')
)
SELECT 
  t2.*,  
  r2.data__store_info__ratings__num_of_reviews
FROM t2_ranked t2
 JOIN raw_portal_public_glovo.restaurant_product r2
  ON CAST(t2.id AS VARCHAR) = r2.data__store_id
  AND DATE(CAST(FROM_UNIXTIME(ROUND(CAST(r2._dlt_load_id AS DOUBLE), 3)) AS TIMESTAMP(3))) = DATE('2025-05-29')
WHERE t2.rn = 1
  AND r2.data__store_info__ratings__num_of_reviews IS NULL;



----------------------------------------------------------------------------------------------------------------------------
------------------------------------------Nueva Movida, esa de Ads----------------------------------------------------------

select SUM(b.ads_amt_ad_spent_sum)
from biz_portal_unif.obt_c_monthly_business_review b
where b.des_company= 'Beleavers'
and month(b.pk_td_entity)= 5 
and b.des_portal = 'Uber Eats'

---


select SUM(b.ads_amt_ad_spent_sum)
from biz_portal.obt_business_review b
where b.des_company= 'Beleavers'
and month(b.pk_td_entity)= 5 
and b.des_portal = 'Uber Eats'




-----------Report Ventas--------
select distinct 
r.des_store_name as Nombre,
r.des_categoria as Categoria,
r.des_phone_number_address as Telefono_company,
r.des_phone_number_address as Telefono_tienda,
r.des_address as Direccion, 
r.des_email as email, 
r.flg_new as is_new,
r.des_city as city,
r.val_reviews as reviews,
r.pct_rating as pct_rating, 
r.flg_top as is_top,
r.flg_prime as is_prime
from playground.crp_portal_public__dt_restaurant_mp r
where r.pct_rating is not null and r.val_reviews is not null 




select distinct 
r.restaurant__name,
r.restaurant__uuid
from raw_portal_uber_orders.orders_history r
where restaurant__name like '%orcel%'





select SUM(t.)
from playground.crp_portal__ft_advertising_hp t
where MONTH(t.td_updated_at) = 6
and pfk_id_store= '25681D02' 


select distinct
t.pfk_id_portal,
t.pfk_id_company, 
t.pfk_id_store
from playground.crp_portal__ft_advertising_hp t
where pfk_id_company='149356913891'


