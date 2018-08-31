--=============================================
-- serialize province related fields (6 fields)
--=============================================
DROP TABLE IF EXISTS Province_Dict;
CREATE TABLE Province_Dict AS
SELECT province,  ROW_NUMBER() OVER(ORDER BY province ASC) AS province_code
FROM
	(
        SELECT DISTINCT province AS province
        FROM
            (
                SELECT ip_prov AS province
                FROM atec_1000w_ins_data
                UNION
                SELECT cert_prov AS province
                FROM atec_1000w_ins_data
                UNION
                SELECT card_bin_prov AS province
                FROM atec_1000w_ins_data
                UNION
                SELECT card_mobile_prov AS province
                FROM atec_1000w_ins_data
                UNION
                SELECT card_cert_prov AS province
                FROM atec_1000w_ins_data
                UNION
                SELECT province AS province
                FROM atec_1000w_ins_data
                UNION
                SELECT ip_prov AS province
                FROM atec_1000w_oota_data
                UNION
                SELECT cert_prov AS province
                FROM atec_1000w_oota_data
                UNION
                SELECT card_bin_prov AS province
                FROM atec_1000w_oota_data
                UNION
                SELECT card_mobile_prov AS province
                FROM atec_1000w_oota_data
                UNION
                SELECT card_cert_prov AS province
                FROM atec_1000w_oota_data
                UNION
                SELECT province AS province
                FROM atec_1000w_oota_data
                UNION
                SELECT ip_prov AS province
                FROM atec_1000w_ootb_data
                UNION
                SELECT cert_prov AS province
                FROM atec_1000w_ootb_data
                UNION
                SELECT card_bin_prov AS province
                FROM atec_1000w_ootb_data
                UNION
                SELECT card_mobile_prov AS province
                FROM atec_1000w_ootb_data
                UNION
                SELECT card_cert_prov AS province
                FROM atec_1000w_ootb_data
                UNION
                SELECT province AS province
                FROM atec_1000w_ootb_data
            ) a
	) b;

--=============================================
-- serialize city related fields (6 fields)
--=============================================
DROP TABLE IF EXISTS City_Dict;
CREATE TABLE City_Dict AS
SELECT city,  ROW_NUMBER() OVER(ORDER BY city ASC) AS city_code
FROM
	(
        SELECT DISTINCT city AS city
        FROM
            (
                SELECT ip_city AS city
                FROM atec_1000w_ins_data
                UNION
                SELECT cert_city AS city
                FROM atec_1000w_ins_data
                UNION
                SELECT card_bin_city AS city
                FROM atec_1000w_ins_data
                UNION
                SELECT card_mobile_city AS city
                FROM atec_1000w_ins_data
                UNION
                SELECT card_cert_city AS city
                FROM atec_1000w_ins_data
                UNION
                SELECT city AS city
                FROM atec_1000w_ins_data
                UNION
                SELECT ip_city AS city
                FROM atec_1000w_oota_data
                UNION
                SELECT cert_city AS city
                FROM atec_1000w_oota_data
                UNION
                SELECT card_bin_city AS city
                FROM atec_1000w_oota_data
                UNION
                SELECT card_mobile_city AS city
                FROM atec_1000w_oota_data
                UNION
                SELECT card_cert_city AS city
                FROM atec_1000w_oota_data
                UNION
                SELECT city AS city
                FROM atec_1000w_oota_data
                UNION
                SELECT ip_city AS city
                FROM atec_1000w_ootb_data
                UNION
                SELECT cert_city AS city
                FROM atec_1000w_ootb_data
                UNION
                SELECT card_bin_city AS city
                FROM atec_1000w_ootb_data
                UNION
                SELECT card_mobile_city AS city
                FROM atec_1000w_ootb_data
                UNION
                SELECT card_cert_city AS city
                FROM atec_1000w_ootb_data
                UNION
                SELECT city AS city
                FROM atec_1000w_ootb_data
            ) a
	) b;
    
--=============================================
-- serialize id related fields (2 fields)
--=============================================
DROP TABLE IF EXISTS ID_Dict;
CREATE TABLE ID_Dict AS
SELECT id,  ROW_NUMBER() OVER(ORDER BY id ASC) AS id_code
FROM
	(
        SELECT DISTINCT id AS id
        FROM
            (
                SELECT user_id AS id
                FROM atec_1000w_ins_data
                UNION
                SELECT opposing_id AS id
                FROM atec_1000w_ins_data
                UNION
                SELECT user_id AS id
                FROM atec_1000w_oota_data
                UNION
                SELECT opposing_id AS id
                FROM atec_1000w_oota_data
                UNION
                SELECT user_id AS id
                FROM atec_1000w_ootb_data
                UNION
                SELECT opposing_id AS id
                FROM atec_1000w_ootb_data
            ) a
	) b;

--=============================================
-- serialize every other field (16 fields)
--=============================================
DROP TABLE IF EXISTS Other_Dict;
CREATE TABLE Other_Dict AS
SELECT event_id
    , dense_rank() OVER(ORDER BY client_ip ASC) AS client_ip
    , dense_rank() OVER(ORDER BY network ASC) AS network
    , dense_rank() OVER(ORDER BY device_sign ASC) AS device_sign
    , dense_rank() OVER(ORDER BY info_1 ASC) AS info_1
    , dense_rank() OVER(ORDER BY info_2 ASC) AS info_2
    , dense_rank() OVER(ORDER BY is_one_people ASC) AS is_one_people
    , dense_rank() OVER(ORDER BY mobile_oper_platform ASC) AS mobile_oper_platform
    , dense_rank() OVER(ORDER BY operation_channel ASC) AS operation_channel
    , dense_rank() OVER(ORDER BY pay_scene ASC) AS pay_scene
    , dense_rank() OVER(ORDER BY card_cert_no ASC) AS card_cert_no
    , dense_rank() OVER(ORDER BY income_card_no ASC) AS income_card_no
    , dense_rank() OVER(ORDER BY income_card_cert_no ASC) AS income_card_cert_no
    , dense_rank() OVER(ORDER BY income_card_mobile ASC) AS income_card_mobile
    , dense_rank() OVER(ORDER BY income_card_bank_code ASC) AS income_card_bank_code
    , dense_rank() OVER(ORDER BY is_peer_pay ASC) AS is_peer_pay
    , dense_rank() OVER(ORDER BY version ASC) AS version
FROM
    (
        SELECT event_id
            , client_ip
            , network
            , device_sign
            , info_1
            , info_2
            , is_one_people
            , mobile_oper_platform
            , operation_channel
            , pay_scene
            , card_cert_no
            , income_card_no
            , income_card_cert_no
            , income_card_mobile
            , income_card_bank_code
            , is_peer_pay
            , version
        FROM atec_1000w_ins_data
        UNION
        SELECT event_id
            , client_ip
            , network
            , device_sign
            , info_1
            , info_2
            , is_one_people
            , mobile_oper_platform
            , operation_channel
            , pay_scene
            , card_cert_no
            , income_card_no
            , income_card_cert_no
            , income_card_mobile
            , income_card_bank_code
            , is_peer_pay
            , version
        FROM atec_1000w_oota_data
        UNION
        SELECT event_id
            , client_ip
            , network
            , device_sign
            , info_1
            , info_2
            , is_one_people
            , mobile_oper_platform
            , operation_channel
            , pay_scene
            , card_cert_no
            , income_card_no
            , income_card_cert_no
            , income_card_mobile
            , income_card_bank_code
            , is_peer_pay
            , version
        FROM atec_1000w_ootb_data
    ) a;

--=============================================
-- create a serialized training data table
--=============================================
DROP TABLE IF EXISTS serialized_training;
CREATE TABLE serialized_training AS
SELECT AID.event_id event_id
	, ID_user.id_code user_id
    , AID.gmt_occur gmt_occur
    , OD.client_ip client_ip
    , OD.network network
    , OD.device_sign device_sign
    , OD.info_1 info_1
    , OD.info_2 info_2
    , PD_ip.province_code ip_prov
	, CD_ip.city_code ip_city
	, PD_cert.province_code cert_prov
    , CD_cert.city_code cert_city
    , PD_card_bin.province_code card_bin_prov
    , CD_card_bin.city_code card_bin_city
    , PD_card_mobile.province_code card_mobile_prov
    , CD_card_mobile.city_code card_mobile_city
    , PD_card_cert.province_code card_cert_prov
    , CD_card_cert.city_code card_cert_city
    , OD.is_one_people is_one_people
    , OD.mobile_oper_platform mobile_oper_platform
    , OD.operation_channel operation_channel
	, OD.pay_scene pay_scene
    , AID.amt amt
    , OD.card_cert_no card_cert_no
    , ID_opposing.id_code opposing_id
    , OD.income_card_no income_card_no
    , OD.income_card_cert_no income_card_cert_no
    , OD.income_card_mobile income_card_mobile
    , OD.income_card_bank_code income_card_bank_code
    , PD.province_code province    
    , CD.city_code city
    , OD.is_peer_pay is_peer_pay
    , OD.version version
    , AID.is_fraud is_fraud
FROM atec_1000w_ins_data AID
	INNER JOIN Other_Dict OD
    ON coalesce(AID.event_id, '') = coalesce(OD.event_id, '')
	INNER JOIN ID_Dict ID_user
    ON coalesce(AID.user_id, '') = coalesce(ID_user.id, '')
    INNER JOIN ID_Dict ID_opposing
    ON coalesce(AID.opposing_id, '') = coalesce(ID_opposing.id, '')
	INNER JOIN Province_Dict PD_ip
    ON coalesce(AID.ip_prov, '') = coalesce(PD_ip.province, '')
    INNER JOIN Province_Dict PD_cert
    ON coalesce(AID.cert_prov, '') = coalesce(PD_cert.province, '')
    INNER JOIN Province_Dict PD_card_bin
    ON coalesce(AID.card_bin_prov, '') = coalesce(PD_card_bin.province, '')
    INNER JOIN Province_Dict PD_card_mobile
    ON coalesce(AID.card_mobile_prov, '') = coalesce(PD_card_mobile.province, '')
    INNER JOIN Province_Dict PD_card_cert
    ON coalesce(AID.card_cert_prov, '') = coalesce(PD_card_cert.province, '')
    INNER JOIN Province_Dict PD
    ON coalesce(AID.province, '') = coalesce(PD.province, '')
    INNER JOIN City_Dict CD_ip
    ON coalesce(AID.ip_city, '') = coalesce(CD_ip.city, '')
    INNER JOIN City_Dict CD_cert
    ON coalesce(AID.cert_city, '') = coalesce(CD_cert.city, '')
    INNER JOIN City_Dict CD_card_bin
    ON coalesce(AID.card_bin_city, '') = coalesce(CD_card_bin.city, '')
    INNER JOIN City_Dict CD_card_mobile
    ON coalesce(AID.card_mobile_city, '') = coalesce(CD_card_mobile.city, '')
    INNER JOIN City_Dict CD_card_cert
    ON coalesce(AID.card_cert_city, '') = coalesce(CD_card_cert.city, '')
    INNER JOIN City_Dict CD
    ON coalesce(AID.city, '') = coalesce(CD.city, '');

--=============================================
-- create a serialized test data table
--=============================================
DROP TABLE IF EXISTS serialized_test_b;
CREATE TABLE serialized_test_b AS
SELECT AOD.event_id event_id
	, ID_user.id_code user_id
    , AOD.gmt_occur gmt_occur
    , OD.client_ip client_ip
    , OD.network network
    , OD.device_sign device_sign
    , OD.info_1 info_1
    , OD.info_2 info_2
    , PD_ip.province_code ip_prov
	, CD_ip.city_code ip_city
	, PD_cert.province_code cert_prov
    , CD_cert.city_code cert_city
    , PD_card_bin.province_code card_bin_prov
    , CD_card_bin.city_code card_bin_city
    , PD_card_mobile.province_code card_mobile_prov
    , CD_card_mobile.city_code card_mobile_city
    , PD_card_cert.province_code card_cert_prov
    , CD_card_cert.city_code card_cert_city
    , OD.is_one_people is_one_people
    , OD.mobile_oper_platform mobile_oper_platform
    , OD.operation_channel operation_channel
	, OD.pay_scene pay_scene
    , AOD.amt amt
    , OD.card_cert_no card_cert_no
    , ID_opposing.id_code opposing_id
    , OD.income_card_no income_card_no
    , OD.income_card_cert_no income_card_cert_no
    , OD.income_card_mobile income_card_mobile
    , OD.income_card_bank_code income_card_bank_code
    , PD.province_code province    
    , CD.city_code city
    , OD.is_peer_pay is_peer_pay
    , OD.version version
FROM atec_1000w_ootb_data AOD
	INNER JOIN Other_Dict OD
    ON coalesce(AOD.event_id, '') = coalesce(OD.event_id, '')
	INNER JOIN ID_Dict ID_user
    ON coalesce(AOD.user_id, '') = coalesce(ID_user.id, '')
    INNER JOIN ID_Dict ID_opposing
    ON coalesce(AOD.opposing_id, '') = coalesce(ID_opposing.id, '')
	INNER JOIN Province_Dict PD_ip
    ON coalesce(AOD.ip_prov, '') = coalesce(PD_ip.province, '')
    INNER JOIN Province_Dict PD_cert
    ON coalesce(AOD.cert_prov, '') = coalesce(PD_cert.province, '')
    INNER JOIN Province_Dict PD_card_bin
    ON coalesce(AOD.card_bin_prov, '') = coalesce(PD_card_bin.province, '')
    INNER JOIN Province_Dict PD_card_mobile
    ON coalesce(AOD.card_mobile_prov, '') = coalesce(PD_card_mobile.province, '')
    INNER JOIN Province_Dict PD_card_cert
    ON coalesce(AOD.card_cert_prov, '') = coalesce(PD_card_cert.province, '')
    INNER JOIN Province_Dict PD
    ON coalesce(AOD.province, '') = coalesce(PD.province, '')
    INNER JOIN City_Dict CD_ip
    ON coalesce(AOD.ip_city, '') = coalesce(CD_ip.city, '')
    INNER JOIN City_Dict CD_cert
    ON coalesce(AOD.cert_city, '') = coalesce(CD_cert.city, '')
    INNER JOIN City_Dict CD_card_bin
    ON coalesce(AOD.card_bin_city, '') = coalesce(CD_card_bin.city, '')
    INNER JOIN City_Dict CD_card_mobile
    ON coalesce(AOD.card_mobile_city, '') = coalesce(CD_card_mobile.city, '')
    INNER JOIN City_Dict CD_card_cert
    ON coalesce(AOD.card_cert_city, '') = coalesce(CD_card_cert.city, '')
    INNER JOIN City_Dict CD
    ON coalesce(AOD.city, '') = coalesce(CD.city, '');
    
--=========================================================
-- create a serialized combined train_and_test data table
--=========================================================
CREATE TABLE serialized_train_and_test_b AS
SELECT *
FROM
	(
        SELECT 1 AS is_train
        	, event_id
            , user_id
            , gmt_occur
            , client_ip
            , network
            , device_sign
            , info_1
            , info_2
            , ip_prov
            , ip_city
            , cert_prov
            , cert_city
            , card_bin_prov
            , card_bin_city
            , card_mobile_prov
            , card_mobile_city
            , card_cert_prov
            , card_cert_city
            , is_one_people
            , mobile_oper_platform
            , operation_channel
            , pay_scene
            , amt
            , card_cert_no
            , opposing_id
            , income_card_no
            , income_card_cert_no
            , income_card_mobile
            , income_card_bank_code
            , province
            , city
            , is_peer_pay
            , version
        FROM serialized_training
        UNION
        SELECT 0 AS is_train
        	, event_id
            , user_id
            , gmt_occur
            , client_ip
            , network
            , device_sign
            , info_1
            , info_2
            , ip_prov
            , ip_city
            , cert_prov
            , cert_city
            , card_bin_prov
            , card_bin_city
            , card_mobile_prov
            , card_mobile_city
            , card_cert_prov
            , card_cert_city
            , is_one_people
            , mobile_oper_platform
            , operation_channel
            , pay_scene
            , amt
            , card_cert_no
            , opposing_id
            , income_card_no
            , income_card_cert_no
            , income_card_mobile
            , income_card_bank_code
            , province
            , city
            , is_peer_pay
            , version
        FROM serialized_test_b
    ) tmp;
    
--===============================================================================
-- record the number of previous transactions
--===============================================================================
DROP TABLE IF EXISTS prev_transaction_count_b;
CREATE TABLE prev_transaction_count_b AS
SELECT event_id, COUNT(1) AS num_prev_transaction
FROM 
    (
        SELECT t2.amt, t1.event_id
        FROM serialized_train_and_test_b t1  --驱动表
        	INNER JOIN serialized_train_and_test_b t2  --历史行为明细表
        	ON t1.user_id = t2.user_id 
        WHERE t1.gmt_occur >= t2.gmt_occur
    ) tmp  -- 汇总历史行为信息需要早于本笔时间
GROUP BY event_id

--===============================================================================
-- 增加feature记录这个用户交易的历史中出现的distinct feature的数目
--===============================================================================
DROP TABLE IF EXISTS prev_distinct_count;
CREATE TABLE prev_distinct_count AS
SELECT ip_prov_count.event_id
	, num_prev_ip_prov
    , num_prev_cert_prov
    , num_prev_card_bin_prov
    , num_prev_card_mobile_prov
    , num_prev_card_cert_prov
    , num_prev_province
    , num_prev_ip_city
    , num_prev_cert_city
    , num_prev_card_bin_city
    , num_prev_card_mobile_city
    , num_prev_card_cert_city
    , num_prev_city
    , num_prev_client_ip
    , num_prev_network
    , num_prev_device_sign
    , num_prev_info_1
    , num_prev_info_2
    , num_prev_is_one_people
    , num_prev_mobile_oper_platform
    , num_prev_operation_channel
    , num_prev_pay_scene
    , num_prev_card_cert_no
    , num_prev_opposing_id
    , num_prev_income_card_no
    , num_prev_income_card_cert_no
    , num_prev_income_card_mobile
    , num_prev_income_card_bank_code
    , num_prev_is_peer_pay
    , num_prev_version
FROM
    (SELECT event_id, COUNT(DISTINCT ip_prov) AS num_prev_ip_prov
    FROM 
        (
            SELECT t1.event_id, t2.ip_prov
            FROM serialized_train_and_test t1  --驱动表
                INNER JOIN serialized_train_and_test t2  --历史行为明细表
                ON t1.user_id = t2.user_id
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_ip_prov  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) ip_prov_count
    INNER JOIN
    (SELECT event_id, COUNT(DISTINCT cert_prov) AS num_prev_cert_prov
    FROM 
        (
            SELECT t1.event_id, t2.cert_prov
            FROM serialized_train_and_test t1  --驱动表
                INNER JOIN serialized_train_and_test t2  --历史行为明细表
                ON t1.user_id = t2.user_id
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_cert_prov  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) cert_prov_count
    ON ip_prov_count.event_id = cert_prov_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(DISTINCT card_bin_prov) AS num_prev_card_bin_prov
    FROM 
        (
            SELECT t1.event_id, t2.card_bin_prov
            FROM serialized_train_and_test t1  --驱动表
                INNER JOIN serialized_train_and_test t2  --历史行为明细表
                ON t1.user_id = t2.user_id
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_card_bin_prov  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) card_bin_prov_count
    ON ip_prov_count.event_id = card_bin_prov_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(DISTINCT card_mobile_prov) AS num_prev_card_mobile_prov
    FROM 
        (
            SELECT t1.event_id, t2.card_mobile_prov
            FROM serialized_train_and_test t1  --驱动表
                INNER JOIN serialized_train_and_test t2  --历史行为明细表
                ON t1.user_id = t2.user_id
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_card_mobile_prov  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) card_mobile_prov_count
    ON ip_prov_count.event_id = card_mobile_prov_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(DISTINCT card_cert_prov) AS num_prev_card_cert_prov
    FROM 
        (
            SELECT t1.event_id, t2.card_cert_prov
            FROM serialized_train_and_test t1  --驱动表
                INNER JOIN serialized_train_and_test t2  --历史行为明细表
                ON t1.user_id = t2.user_id
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_card_cert_prov  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) card_cert_prov_count
    ON ip_prov_count.event_id = card_cert_prov_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(DISTINCT province) AS num_prev_province
    FROM 
        (
            SELECT t1.event_id, t2.province
            FROM serialized_train_and_test t1  --驱动表
                INNER JOIN serialized_train_and_test t2  --历史行为明细表
                ON t1.user_id = t2.user_id
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_province  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) province_count
    ON ip_prov_count.event_id = province_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(DISTINCT ip_city) AS num_prev_ip_city
    FROM 
        (
            SELECT t1.event_id, t2.ip_city
            FROM serialized_train_and_test t1  --驱动表
                INNER JOIN serialized_train_and_test t2  --历史行为明细表
                ON t1.user_id = t2.user_id
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_ip_city  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) ip_city_count
    ON ip_prov_count.event_id = ip_city_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(DISTINCT cert_city) AS num_prev_cert_city
    FROM 
        (
            SELECT t1.event_id, t2.cert_city
            FROM serialized_train_and_test t1  --驱动表
                INNER JOIN serialized_train_and_test t2  --历史行为明细表
                ON t1.user_id = t2.user_id
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_cert_city  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) cert_city_count
    ON ip_prov_count.event_id = cert_city_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(DISTINCT card_bin_city) AS num_prev_card_bin_city
    FROM 
        (
            SELECT t1.event_id, t2.card_bin_city
            FROM serialized_train_and_test t1  --驱动表
                INNER JOIN serialized_train_and_test t2  --历史行为明细表
                ON t1.user_id = t2.user_id
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_card_bin_city  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) card_bin_city_count
    ON ip_prov_count.event_id = card_bin_city_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(DISTINCT card_mobile_city) AS num_prev_card_mobile_city
    FROM 
        (
            SELECT t1.event_id, t2.card_mobile_city
            FROM serialized_train_and_test t1  --驱动表
                INNER JOIN serialized_train_and_test t2  --历史行为明细表
                ON t1.user_id = t2.user_id
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_card_mobile_city  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) card_mobile_city_count
    ON ip_prov_count.event_id = card_mobile_city_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(DISTINCT card_cert_city) AS num_prev_card_cert_city
    FROM 
        (
            SELECT t1.event_id, t2.card_cert_city
            FROM serialized_train_and_test t1  --驱动表
                INNER JOIN serialized_train_and_test t2  --历史行为明细表
                ON t1.user_id = t2.user_id
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_card_cert_city  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) card_cert_city_count
    ON ip_prov_count.event_id = card_cert_city_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(DISTINCT city) AS num_prev_city
    FROM 
        (
            SELECT t1.event_id, t2.city
            FROM serialized_train_and_test t1  --驱动表
                INNER JOIN serialized_train_and_test t2  --历史行为明细表
                ON t1.user_id = t2.user_id
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_city  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) city_count
    ON ip_prov_count.event_id = city_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(DISTINCT client_ip) AS num_prev_client_ip
    FROM 
        (
            SELECT t1.event_id, t2.client_ip
            FROM serialized_train_and_test t1  --驱动表
                INNER JOIN serialized_train_and_test t2  --历史行为明细表
                ON t1.user_id = t2.user_id
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_client_ip  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) client_ip_count
    ON ip_prov_count.event_id = client_ip_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(DISTINCT network) AS num_prev_network
    FROM 
        (
            SELECT t1.event_id, t2.network
            FROM serialized_train_and_test t1  --驱动表
                INNER JOIN serialized_train_and_test t2  --历史行为明细表
                ON t1.user_id = t2.user_id
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_network  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) network_count
    ON ip_prov_count.event_id = network_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(DISTINCT device_sign) AS num_prev_device_sign
    FROM 
        (
            SELECT t1.event_id, t2.device_sign
            FROM serialized_train_and_test t1  --驱动表
                INNER JOIN serialized_train_and_test t2  --历史行为明细表
                ON t1.user_id = t2.user_id
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_device_sign  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) device_sign_count
    ON ip_prov_count.event_id = device_sign_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(DISTINCT info_1) AS num_prev_info_1
    FROM 
        (
            SELECT t1.event_id, t2.info_1
            FROM serialized_train_and_test t1  --驱动表
                INNER JOIN serialized_train_and_test t2  --历史行为明细表
                ON t1.user_id = t2.user_id
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_info_1  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) info_1_count
    ON ip_prov_count.event_id = info_1_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(DISTINCT info_2) AS num_prev_info_2
    FROM 
        (
            SELECT t1.event_id, t2.info_2
            FROM serialized_train_and_test t1  --驱动表
                INNER JOIN serialized_train_and_test t2  --历史行为明细表
                ON t1.user_id = t2.user_id
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_info_2  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) info_2_count
    ON ip_prov_count.event_id = info_2_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(DISTINCT is_one_people) AS num_prev_is_one_people
    FROM 
        (
            SELECT t1.event_id, t2.is_one_people
            FROM serialized_train_and_test t1  --驱动表
                INNER JOIN serialized_train_and_test t2  --历史行为明细表
                ON t1.user_id = t2.user_id
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_is_one_people  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) is_one_people_count
    ON ip_prov_count.event_id = is_one_people_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(DISTINCT mobile_oper_platform) AS num_prev_mobile_oper_platform
    FROM 
        (
            SELECT t1.event_id, t2.mobile_oper_platform
            FROM serialized_train_and_test t1  --驱动表
                INNER JOIN serialized_train_and_test t2  --历史行为明细表
                ON t1.user_id = t2.user_id
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_mobile_oper_platform  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) mobile_oper_platform_count
    ON ip_prov_count.event_id = mobile_oper_platform_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(DISTINCT operation_channel) AS num_prev_operation_channel
    FROM 
        (
            SELECT t1.event_id, t2.operation_channel
            FROM serialized_train_and_test t1  --驱动表
                INNER JOIN serialized_train_and_test t2  --历史行为明细表
                ON t1.user_id = t2.user_id
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_operation_channel  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) operation_channel_count
    ON ip_prov_count.event_id = operation_channel_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(DISTINCT pay_scene) AS num_prev_pay_scene
    FROM 
        (
            SELECT t1.event_id, t2.pay_scene
            FROM serialized_train_and_test t1  --驱动表
                INNER JOIN serialized_train_and_test t2  --历史行为明细表
                ON t1.user_id = t2.user_id
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_pay_scene  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) pay_scene_count
    ON ip_prov_count.event_id = pay_scene_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(DISTINCT card_cert_no) AS num_prev_card_cert_no
    FROM 
        (
            SELECT t1.event_id, t2.card_cert_no
            FROM serialized_train_and_test t1  --驱动表
                INNER JOIN serialized_train_and_test t2  --历史行为明细表
                ON t1.user_id = t2.user_id
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_card_cert_no  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) card_cert_no_count
    ON ip_prov_count.event_id = card_cert_no_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(DISTINCT opposing_id) AS num_prev_opposing_id
    FROM 
        (
            SELECT t1.event_id, t2.opposing_id
            FROM serialized_train_and_test t1  --驱动表
                INNER JOIN serialized_train_and_test t2  --历史行为明细表
                ON t1.user_id = t2.user_id
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_opposing_id  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) opposing_id_count
    ON ip_prov_count.event_id = opposing_id_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(DISTINCT income_card_no) AS num_prev_income_card_no
    FROM 
        (
            SELECT t1.event_id, t2.income_card_no
            FROM serialized_train_and_test t1  --驱动表
                INNER JOIN serialized_train_and_test t2  --历史行为明细表
                ON t1.user_id = t2.user_id
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_income_card_no  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) income_card_no_count
    ON ip_prov_count.event_id = income_card_no_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(DISTINCT income_card_cert_no) AS num_prev_income_card_cert_no
    FROM 
        (
            SELECT t1.event_id, t2.income_card_cert_no
            FROM serialized_train_and_test t1  --驱动表
                INNER JOIN serialized_train_and_test t2  --历史行为明细表
                ON t1.user_id = t2.user_id
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_income_card_cert_no  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) income_card_cert_no_count
    ON ip_prov_count.event_id = income_card_cert_no_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(DISTINCT income_card_mobile) AS num_prev_income_card_mobile
    FROM 
        (
            SELECT t1.event_id, t2.income_card_mobile
            FROM serialized_train_and_test t1  --驱动表
                INNER JOIN serialized_train_and_test t2  --历史行为明细表
                ON t1.user_id = t2.user_id
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_income_card_mobile  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) income_card_mobile_count
    ON ip_prov_count.event_id = income_card_mobile_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(DISTINCT income_card_bank_code) AS num_prev_income_card_bank_code
    FROM 
        (
            SELECT t1.event_id, t2.income_card_bank_code
            FROM serialized_train_and_test t1  --驱动表
                INNER JOIN serialized_train_and_test t2  --历史行为明细表
                ON t1.user_id = t2.user_id
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_income_card_bank_code  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) income_card_bank_code_count
    ON ip_prov_count.event_id = income_card_bank_code_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(DISTINCT is_peer_pay) AS num_prev_is_peer_pay
    FROM 
        (
            SELECT t1.event_id, t2.is_peer_pay
            FROM serialized_train_and_test t1  --驱动表
                INNER JOIN serialized_train_and_test t2  --历史行为明细表
                ON t1.user_id = t2.user_id
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_is_peer_pay  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) is_peer_pay_count
    ON ip_prov_count.event_id = is_peer_pay_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(DISTINCT version) AS num_prev_version
    FROM 
        (
            SELECT t1.event_id, t2.version
            FROM serialized_train_and_test t1  --驱动表
                INNER JOIN serialized_train_and_test t2  --历史行为明细表
                ON t1.user_id = t2.user_id
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_version  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) version_count
    ON ip_prov_count.event_id = version_count.event_id
--===============================================================================
-- 增加feature记录这个ip_prov在这个用户交易的历史中出现的次数
--===============================================================================
DROP TABLE IF EXISTS prev_feature_count_b;
CREATE TABLE prev_feature_count_b AS
SELECT ip_prov_count.event_id
	, ip_prov_count.ip_prov_occurences
    , cert_prov_count.cert_prov_occurences
    , card_bin_prov_count.card_bin_prov_occurences
    , card_mobile_prov_count.card_mobile_prov_occurences
    , card_cert_prov_count.card_cert_prov_occurences
    , province_count.province_occurences
    , ip_city_count.ip_city_occurences
    , cert_city_count.cert_city_occurences
    , card_bin_city_count.card_bin_city_occurences
    , card_mobile_city_count.card_mobile_city_occurences
    , card_cert_city_count.card_cert_city_occurences
    , city_count.city_occurences
    , client_ip_count.client_ip_occurences
    , network_count.network_occurences
    , device_sign_count.device_sign_occurences
    , info_1_count.info_1_occurences
    , info_2_count.info_2_occurences
    , is_one_people_count.is_one_people_occurences
    , mobile_oper_platform_count.mobile_oper_platform_occurences
    , operation_channel_count.operation_channel_occurences
    , pay_scene_count.pay_scene_occurences
    , card_cert_no_count.card_cert_no_occurences
    , opposing_id_count.opposing_id_occurences
    , income_card_no_count.income_card_no_occurences
    , income_card_cert_no_count.income_card_cert_no_occurences
    , income_card_mobile_count.income_card_mobile_occurences
    , income_card_bank_code_count.income_card_bank_code_occurences
    , is_peer_pay_count.is_peer_pay_occurences
    , version_count.version_occurences
FROM
    (SELECT event_id, COUNT(1) AS ip_prov_occurences
    FROM 
        (
            SELECT t1.event_id
            FROM serialized_train_and_test_b t1  --驱动表
                INNER JOIN serialized_train_and_test_b t2  --历史行为明细表
                ON t1.user_id = t2.user_id
                AND t1.ip_prov = t2.ip_prov
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_ip_prov  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) ip_prov_count
    INNER JOIN
    (SELECT event_id, COUNT(1) AS cert_prov_occurences
    FROM 
        (
            SELECT t1.event_id
            FROM serialized_train_and_test_b t1  --驱动表
                INNER JOIN serialized_train_and_test_b t2  --历史行为明细表
                ON t1.user_id = t2.user_id
                AND t1.cert_prov = t2.cert_prov
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_cert_prov  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) cert_prov_count
    ON ip_prov_count.event_id = cert_prov_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(1) AS card_bin_prov_occurences
    FROM 
        (
            SELECT t1.event_id
            FROM serialized_train_and_test_b t1  --驱动表
                INNER JOIN serialized_train_and_test_b t2  --历史行为明细表
                ON t1.user_id = t2.user_id
                AND t1.card_bin_prov = t2.card_bin_prov
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_card_bin_prov  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) card_bin_prov_count
    ON ip_prov_count.event_id = card_bin_prov_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(1) AS card_mobile_prov_occurences
    FROM 
        (
            SELECT t1.event_id
            FROM serialized_train_and_test_b t1  --驱动表
                INNER JOIN serialized_train_and_test_b t2  --历史行为明细表
                ON t1.user_id = t2.user_id
                AND t1.card_mobile_prov = t2.card_mobile_prov
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_card_mobile_prov  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) card_mobile_prov_count
    ON ip_prov_count.event_id = card_mobile_prov_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(1) AS card_cert_prov_occurences
    FROM 
        (
            SELECT t1.event_id
            FROM serialized_train_and_test_b t1  --驱动表
                INNER JOIN serialized_train_and_test_b t2  --历史行为明细表
                ON t1.user_id = t2.user_id
                AND t1.card_cert_prov = t2.card_cert_prov
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_card_cert_prov  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) card_cert_prov_count
    ON ip_prov_count.event_id = card_cert_prov_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(1) AS province_occurences
    FROM 
        (
            SELECT t1.event_id
            FROM serialized_train_and_test_b t1  --驱动表
                INNER JOIN serialized_train_and_test_b t2  --历史行为明细表
                ON t1.user_id = t2.user_id
                AND t1.province = t2.province
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_province  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) province_count
    ON ip_prov_count.event_id = province_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(1) AS ip_city_occurences
    FROM 
        (
            SELECT t1.event_id
            FROM serialized_train_and_test_b t1  --驱动表
                INNER JOIN serialized_train_and_test_b t2  --历史行为明细表
                ON t1.user_id = t2.user_id
                AND t1.ip_city = t2.ip_city
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_ip_city  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) ip_city_count
    ON ip_prov_count.event_id = ip_city_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(1) AS cert_city_occurences
    FROM 
        (
            SELECT t1.event_id
            FROM serialized_train_and_test_b t1  --驱动表
                INNER JOIN serialized_train_and_test_b t2  --历史行为明细表
                ON t1.user_id = t2.user_id
                AND t1.cert_city = t2.cert_city
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_cert_city  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) cert_city_count
    ON ip_prov_count.event_id = cert_city_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(1) AS card_bin_city_occurences
    FROM 
        (
            SELECT t1.event_id
            FROM serialized_train_and_test_b t1  --驱动表
                INNER JOIN serialized_train_and_test_b t2  --历史行为明细表
                ON t1.user_id = t2.user_id
                AND t1.card_bin_city = t2.card_bin_city
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_card_bin_city  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) card_bin_city_count
    ON ip_prov_count.event_id = card_bin_city_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(1) AS card_mobile_city_occurences
    FROM 
        (
            SELECT t1.event_id
            FROM serialized_train_and_test_b t1  --驱动表
                INNER JOIN serialized_train_and_test_b t2  --历史行为明细表
                ON t1.user_id = t2.user_id
                AND t1.card_mobile_city = t2.card_mobile_city
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_card_mobile_city  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) card_mobile_city_count
    ON ip_prov_count.event_id = card_mobile_city_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(1) AS card_cert_city_occurences
    FROM 
        (
            SELECT t1.event_id
            FROM serialized_train_and_test_b t1  --驱动表
                INNER JOIN serialized_train_and_test_b t2  --历史行为明细表
                ON t1.user_id = t2.user_id
                AND t1.card_cert_city = t2.card_cert_city
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_card_cert_city  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) card_cert_city_count
    ON ip_prov_count.event_id = card_cert_city_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(1) AS city_occurences
    FROM 
        (
            SELECT t1.event_id
            FROM serialized_train_and_test_b t1  --驱动表
                INNER JOIN serialized_train_and_test_b t2  --历史行为明细表
                ON t1.user_id = t2.user_id
                AND t1.city = t2.city
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_city  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) city_count
    ON ip_prov_count.event_id = city_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(1) AS client_ip_occurences
    FROM 
        (
            SELECT t1.event_id
            FROM serialized_train_and_test_b t1  --驱动表
                INNER JOIN serialized_train_and_test_b t2  --历史行为明细表
                ON t1.user_id = t2.user_id
                AND t1.client_ip = t2.client_ip
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_client_ip  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) client_ip_count
    ON ip_prov_count.event_id = client_ip_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(1) AS network_occurences
    FROM 
        (
            SELECT t1.event_id
            FROM serialized_train_and_test_b t1  --驱动表
                INNER JOIN serialized_train_and_test_b t2  --历史行为明细表
                ON t1.user_id = t2.user_id
                AND t1.network = t2.network
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_network  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) network_count
    ON ip_prov_count.event_id = network_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(1) AS device_sign_occurences
    FROM 
        (
            SELECT t1.event_id
            FROM serialized_train_and_test_b t1  --驱动表
                INNER JOIN serialized_train_and_test_b t2  --历史行为明细表
                ON t1.user_id = t2.user_id
                AND t1.device_sign = t2.device_sign
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_device_sign  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) device_sign_count
    ON ip_prov_count.event_id = device_sign_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(1) AS info_1_occurences
    FROM 
        (
            SELECT t1.event_id
            FROM serialized_train_and_test_b t1  --驱动表
                INNER JOIN serialized_train_and_test_b t2  --历史行为明细表
                ON t1.user_id = t2.user_id
                AND t1.info_1 = t2.info_1
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_info_1  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) info_1_count
    ON ip_prov_count.event_id = info_1_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(1) AS info_2_occurences
    FROM 
        (
            SELECT t1.event_id
            FROM serialized_train_and_test_b t1  --驱动表
                INNER JOIN serialized_train_and_test_b t2  --历史行为明细表
                ON t1.user_id = t2.user_id
                AND t1.info_2 = t2.info_2
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_info_2  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) info_2_count
    ON ip_prov_count.event_id = info_2_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(1) AS is_one_people_occurences
    FROM 
        (
            SELECT t1.event_id
            FROM serialized_train_and_test_b t1  --驱动表
                INNER JOIN serialized_train_and_test_b t2  --历史行为明细表
                ON t1.user_id = t2.user_id
                AND t1.is_one_people = t2.is_one_people
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_is_one_people  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) is_one_people_count
    ON ip_prov_count.event_id = is_one_people_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(1) AS mobile_oper_platform_occurences
    FROM 
        (
            SELECT t1.event_id
            FROM serialized_train_and_test_b t1  --驱动表
                INNER JOIN serialized_train_and_test_b t2  --历史行为明细表
                ON t1.user_id = t2.user_id
                AND t1.mobile_oper_platform = t2.mobile_oper_platform
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_mobile_oper_platform  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) mobile_oper_platform_count
    ON ip_prov_count.event_id = mobile_oper_platform_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(1) AS operation_channel_occurences
    FROM 
        (
            SELECT t1.event_id
            FROM serialized_train_and_test_b t1  --驱动表
                INNER JOIN serialized_train_and_test_b t2  --历史行为明细表
                ON t1.user_id = t2.user_id
                AND t1.operation_channel = t2.operation_channel
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_operation_channel  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) operation_channel_count
    ON ip_prov_count.event_id = operation_channel_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(1) AS pay_scene_occurences
    FROM 
        (
            SELECT t1.event_id
            FROM serialized_train_and_test_b t1  --驱动表
                INNER JOIN serialized_train_and_test_b t2  --历史行为明细表
                ON t1.user_id = t2.user_id
                AND t1.pay_scene = t2.pay_scene
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_pay_scene  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) pay_scene_count
    ON ip_prov_count.event_id = pay_scene_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(1) AS card_cert_no_occurences
    FROM 
        (
            SELECT t1.event_id
            FROM serialized_train_and_test_b t1  --驱动表
                INNER JOIN serialized_train_and_test_b t2  --历史行为明细表
                ON t1.user_id = t2.user_id
                AND t1.card_cert_no = t2.card_cert_no
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_card_cert_no  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) card_cert_no_count
    ON ip_prov_count.event_id = card_cert_no_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(1) AS opposing_id_occurences
    FROM 
        (
            SELECT t1.event_id
            FROM serialized_train_and_test_b t1  --驱动表
                INNER JOIN serialized_train_and_test_b t2  --历史行为明细表
                ON t1.user_id = t2.user_id
                AND t1.opposing_id = t2.opposing_id
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_opposing_id  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) opposing_id_count
    ON ip_prov_count.event_id = opposing_id_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(1) AS income_card_no_occurences
    FROM 
        (
            SELECT t1.event_id
            FROM serialized_train_and_test_b t1  --驱动表
                INNER JOIN serialized_train_and_test_b t2  --历史行为明细表
                ON t1.user_id = t2.user_id
                AND t1.income_card_no = t2.income_card_no
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_income_card_no  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) income_card_no_count
    ON ip_prov_count.event_id = income_card_no_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(1) AS income_card_cert_no_occurences
    FROM 
        (
            SELECT t1.event_id
            FROM serialized_train_and_test_b t1  --驱动表
                INNER JOIN serialized_train_and_test_b t2  --历史行为明细表
                ON t1.user_id = t2.user_id
                AND t1.income_card_cert_no = t2.income_card_cert_no
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_income_card_cert_no  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) income_card_cert_no_count
    ON ip_prov_count.event_id = income_card_cert_no_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(1) AS income_card_mobile_occurences
    FROM 
        (
            SELECT t1.event_id
            FROM serialized_train_and_test_b t1  --驱动表
                INNER JOIN serialized_train_and_test_b t2  --历史行为明细表
                ON t1.user_id = t2.user_id
                AND t1.income_card_mobile = t2.income_card_mobile
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_income_card_mobile  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) income_card_mobile_count
    ON ip_prov_count.event_id = income_card_mobile_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(1) AS income_card_bank_code_occurences
    FROM 
        (
            SELECT t1.event_id
            FROM serialized_train_and_test_b t1  --驱动表
                INNER JOIN serialized_train_and_test_b t2  --历史行为明细表
                ON t1.user_id = t2.user_id
                AND t1.income_card_bank_code = t2.income_card_bank_code
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_income_card_bank_code  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) income_card_bank_code_count
    ON ip_prov_count.event_id = income_card_bank_code_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(1) AS is_peer_pay_occurences
    FROM 
        (
            SELECT t1.event_id
            FROM serialized_train_and_test_b t1  --驱动表
                INNER JOIN serialized_train_and_test_b t2  --历史行为明细表
                ON t1.user_id = t2.user_id
                AND t1.is_peer_pay = t2.is_peer_pay
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_is_peer_pay  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) is_peer_pay_count
    ON ip_prov_count.event_id = is_peer_pay_count.event_id
    INNER JOIN
    (SELECT event_id, COUNT(1) AS version_occurences
    FROM 
        (
            SELECT t1.event_id
            FROM serialized_train_and_test_b t1  --驱动表
                INNER JOIN serialized_train_and_test_b t2  --历史行为明细表
                ON t1.user_id = t2.user_id
                AND t1.version = t2.version
            WHERE t1.gmt_occur >= t2.gmt_occur
        )tmp_version  -- 汇总历史行为信息需要早于本笔时间
    GROUP BY event_id) version_count
    ON ip_prov_count.event_id = version_count.event_id
    
--=========================================================
-- add hand crafted features
--=========================================================
DROP TABLE IF EXISTS training_1_b;
CREATE TABLE training_1_b AS
SELECT t_main.*
	, t_1.num_prev_transaction
    , t_2.ip_prov_occurences/t_1.num_prev_transaction AS ip_prov_ratio_among_prev
    , t_2.cert_prov_occurences/t_1.num_prev_transaction AS cert_prov_ratio_among_prev
    , t_2.card_bin_prov_occurences/t_1.num_prev_transaction AS card_bin_prov_ratio_among_prev
    , t_2.card_mobile_prov_occurences/t_1.num_prev_transaction AS card_mobile_prov_ratio_among_prev
    , t_2.card_cert_prov_occurences/t_1.num_prev_transaction AS card_cert_prov_ratio_among_prev
    , t_2.province_occurences/t_1.num_prev_transaction AS province_ratio_among_prev
    , t_2.ip_city_occurences/t_1.num_prev_transaction AS ip_city_ratio_among_prev
    , t_2.cert_city_occurences/t_1.num_prev_transaction AS cert_city_ratio_among_prev
    , t_2.card_bin_city_occurences/t_1.num_prev_transaction AS card_bin_city_ratio_among_prev
    , t_2.card_mobile_city_occurences/t_1.num_prev_transaction AS card_mobile_city_ratio_among_prev
    , t_2.card_cert_city_occurences/t_1.num_prev_transaction AS card_cert_city_ratio_among_prev
    , t_2.city_occurences/t_1.num_prev_transaction AS city_ratio_among_prev
    , t_2.client_ip_occurences/t_1.num_prev_transaction AS client_ip_ratio_among_prev
    , t_2.network_occurences/t_1.num_prev_transaction AS network_ratio_among_prev
    , t_2.device_sign_occurences/t_1.num_prev_transaction AS device_sign_ratio_among_prev
    , t_2.info_1_occurences/t_1.num_prev_transaction AS info_1_ratio_among_prev
    , t_2.info_2_occurences/t_1.num_prev_transaction AS info_2_ratio_among_prev
    , t_2.is_one_people_occurences/t_1.num_prev_transaction AS is_one_people_ratio_among_prev
    , t_2.mobile_oper_platform_occurences/t_1.num_prev_transaction AS mobile_oper_platform_ratio_among_prev
    , t_2.operation_channel_occurences/t_1.num_prev_transaction AS operation_channel_ratio_among_prev
    , t_2.pay_scene_occurences/t_1.num_prev_transaction AS pay_scene_ratio_among_prev
    , t_2.card_cert_no_occurences/t_1.num_prev_transaction AS card_cert_no_ratio_among_prev
    , t_2.opposing_id_occurences/t_1.num_prev_transaction AS opposing_id_ratio_among_prev
    , t_2.income_card_no_occurences/t_1.num_prev_transaction AS income_card_no_ratio_among_prev
    , t_2.income_card_cert_no_occurences/t_1.num_prev_transaction AS income_card_cert_no_ratio_among_prev
    , t_2.income_card_mobile_occurences/t_1.num_prev_transaction AS income_card_mobile_ratio_among_prev
    , t_2.income_card_bank_code_occurences/t_1.num_prev_transaction AS income_card_bank_code_ratio_among_prev
    , t_2.is_peer_pay_occurences/t_1.num_prev_transaction AS is_peer_pay_ratio_among_prev
    , t_2.version_occurences/t_1.num_prev_transaction AS version_ratio_among_prev
FROM
    (
        SELECT *
            , CASE WHEN ip_prov=1 OR cert_prov=1 THEN -1 WHEN ip_prov!=cert_prov THEN 0 ELSE 1 END AS ip_prov_vs_cert_prov
            , CASE WHEN ip_prov=1 OR card_bin_prov=1 THEN -1 WHEN ip_prov!=card_bin_prov THEN 0 ELSE 1 END AS ip_prov_vs_card_bin_prov
            , CASE WHEN ip_prov=1 OR card_mobile_prov=1 THEN -1 WHEN ip_prov!=card_mobile_prov THEN 0 ELSE 1 END AS ip_prov_vs_card_mobile_prov
            , CASE WHEN ip_prov=1 OR card_cert_prov=1 THEN -1 WHEN ip_prov!=card_cert_prov THEN 0 ELSE 1 END AS ip_prov_vs_card_cert_prov
            , CASE WHEN ip_prov=1 OR province=1 THEN -1 WHEN ip_prov!=province THEN 0 ELSE 1 END AS ip_prov_vs_province
            , CASE WHEN cert_prov=1 OR card_bin_prov=1 THEN -1 WHEN cert_prov!=card_bin_prov THEN 0 ELSE 1 END AS cert_prov_vs_card_bin_prov
            , CASE WHEN cert_prov=1 OR card_mobile_prov=1 THEN -1 WHEN cert_prov!=card_mobile_prov THEN 0 ELSE 1 END AS cert_prov_vs_card_mobile_prov
            , CASE WHEN cert_prov=1 OR card_cert_prov=1 THEN -1 WHEN cert_prov!=card_cert_prov THEN 0 ELSE 1 END AS cert_prov_vs_card_cert_prov
            , CASE WHEN cert_prov=1 OR province=1 THEN -1 WHEN cert_prov!=province THEN 0 ELSE 1 END AS cert_prov_vs_province
            , CASE WHEN card_bin_prov=1 OR card_mobile_prov=1 THEN -1 WHEN card_bin_prov!=card_mobile_prov THEN 0 ELSE 1 END AS card_bin_prov_vs_card_mobile_prov
            , CASE WHEN card_bin_prov=1 OR card_cert_prov=1 THEN -1 WHEN card_bin_prov!=card_cert_prov THEN 0 ELSE 1 END AS card_bin_prov_vs_card_cert_prov
            , CASE WHEN card_bin_prov=1 OR province=1 THEN -1 WHEN card_bin_prov!=province THEN 0 ELSE 1 END AS card_bin_prov_vs_province
            , CASE WHEN card_mobile_prov=1 OR card_cert_prov=1 THEN -1 WHEN card_mobile_prov!=card_cert_prov THEN 0 ELSE 1 END AS card_mobile_prov_vs_card_cert_prov
            , CASE WHEN card_mobile_prov=1 OR province=1 THEN -1 WHEN card_mobile_prov!=province THEN 0 ELSE 1 END AS card_mobile_prov_vs_province
            , CASE WHEN card_cert_prov=1 OR province=1 THEN -1 WHEN card_cert_prov!=province THEN 0 ELSE 1 END AS card_cert_prov_vs_province
            , CASE WHEN ip_city=1 OR cert_city=1 THEN -1 WHEN ip_city!=cert_city THEN 0 ELSE 1 END AS ip_city_vs_cert_city
            , CASE WHEN ip_city=1 OR card_bin_city=1 THEN -1 WHEN ip_city!=card_bin_city THEN 0 ELSE 1 END AS ip_city_vs_card_bin_city
            , CASE WHEN ip_city=1 OR card_mobile_city=1 THEN -1 WHEN ip_city!=card_mobile_city THEN 0 ELSE 1 END AS ip_city_vs_card_mobile_city
            , CASE WHEN ip_city=1 OR card_cert_city=1 THEN -1 WHEN ip_city!=card_cert_city THEN 0 ELSE 1 END AS ip_city_vs_card_cert_city
            , CASE WHEN ip_city=1 OR city=1 THEN -1 WHEN ip_city!=city THEN 0 ELSE 1 END AS ip_city_vs_city
            , CASE WHEN cert_city=1 OR card_bin_city=1 THEN -1 WHEN cert_city!=card_bin_city THEN 0 ELSE 1 END AS cert_city_vs_card_bin_city
            , CASE WHEN cert_city=1 OR card_mobile_city=1 THEN -1 WHEN cert_city!=card_mobile_city THEN 0 ELSE 1 END AS cert_city_vs_card_mobile_city
            , CASE WHEN cert_city=1 OR card_cert_city=1 THEN -1 WHEN cert_city!=card_cert_city THEN 0 ELSE 1 END AS cert_city_vs_card_cert_city
            , CASE WHEN cert_city=1 OR city=1 THEN -1 WHEN cert_city!=city THEN 0 ELSE 1 END AS cert_city_vs_city
            , CASE WHEN card_bin_city=1 OR card_mobile_city=1 THEN -1 WHEN card_bin_city!=card_mobile_city THEN 0 ELSE 1 END AS card_bin_city_vs_card_mobile_city
            , CASE WHEN card_bin_city=1 OR card_cert_city=1 THEN -1 WHEN card_bin_city!=card_cert_city THEN 0 ELSE 1 END AS card_bin_city_vs_card_cert_city
            , CASE WHEN card_bin_city=1 OR city=1 THEN -1 WHEN card_bin_city!=city THEN 0 ELSE 1 END AS card_bin_city_vs_city
            , CASE WHEN card_mobile_city=1 OR card_cert_city=1 THEN -1 WHEN card_mobile_city!=card_cert_city THEN 0 ELSE 1 END AS card_mobile_city_vs_card_cert_city
            , CASE WHEN card_mobile_city=1 OR city=1 THEN -1 WHEN card_mobile_city!=city THEN 0 ELSE 1 END AS card_mobile_city_vs_city
            , CASE WHEN card_cert_city=1 OR city=1 THEN -1 WHEN card_cert_city!=city THEN 0 ELSE 1 END AS card_cert_city_vs_city
            , DATEPART(TO_DATE(gmt_occur, 'yyyy-mm-dd hh'), 'hh') AS transaction_hour
            , WEEKDAY(TO_DATE(gmt_occur, 'yyyy-mm-dd hh')) AS transaction_dow
            , DATEPART(TO_DATE(gmt_occur, 'yyyy-mm-dd hh'), 'dd') AS transaction_dom
            , DATEDIFF(TO_DATE(gmt_occur, 'yyyy-mm-dd hh'), TO_DATE((LAG(gmt_occur, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC)), 'yyyy-mm-dd hh'), 'hh') AS diff_hour
            , DATEDIFF(TO_DATE(gmt_occur, 'yyyy-mm-dd hh'), TO_DATE((LAG(gmt_occur, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC)), 'yyyy-mm-dd hh'), 'dd') AS diff_day
        	, CASE WHEN (LAG(ip_prov, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR ip_prov=1 THEN -1 WHEN ((LAG(ip_prov, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-ip_prov) != 0 THEN 0 ELSE 1 END AS ip_prov_vs_prev
			, CASE WHEN (LAG(cert_prov, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR cert_prov=1 THEN -1 WHEN ((LAG(cert_prov, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-cert_prov) != 0 THEN 0 ELSE 1 END AS cert_prov_vs_prev
			, CASE WHEN (LAG(card_bin_prov, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR card_bin_prov=1 THEN -1 WHEN ((LAG(card_bin_prov, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-card_bin_prov) != 0 THEN 0 ELSE 1 END AS card_bin_prov_vs_prev
			, CASE WHEN (LAG(card_mobile_prov, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR card_mobile_prov=1 THEN -1 WHEN ((LAG(card_mobile_prov, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-card_mobile_prov) != 0 THEN 0 ELSE 1 END AS card_mobile_prov_vs_prev
			, CASE WHEN (LAG(card_cert_prov, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR card_cert_prov=1 THEN -1 WHEN ((LAG(card_cert_prov, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-card_cert_prov) != 0 THEN 0 ELSE 1 END AS card_cert_prov_vs_prev
			, CASE WHEN (LAG(province, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR province=1 THEN -1 WHEN ((LAG(province, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-province) != 0 THEN 0 ELSE 1 END AS province_vs_prev
			, CASE WHEN (LAG(ip_city, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR ip_city=1 THEN -1 WHEN ((LAG(ip_city, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-ip_city) != 0 THEN 0 ELSE 1 END AS ip_city_vs_prev
			, CASE WHEN (LAG(cert_city, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR cert_city=1 THEN -1 WHEN ((LAG(cert_city, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-cert_city) != 0 THEN 0 ELSE 1 END AS cert_city_vs_prev
			, CASE WHEN (LAG(card_bin_city, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR card_bin_city=1 THEN -1 WHEN ((LAG(card_bin_city, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-card_bin_city) != 0 THEN 0 ELSE 1 END AS card_bin_city_vs_prev
			, CASE WHEN (LAG(card_mobile_city, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR card_mobile_city=1 THEN -1 WHEN ((LAG(card_mobile_city, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-card_mobile_city) != 0 THEN 0 ELSE 1 END AS card_mobile_city_vs_prev
			, CASE WHEN (LAG(card_cert_city, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR card_cert_city=1 THEN -1 WHEN ((LAG(card_cert_city, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-card_cert_city) != 0 THEN 0 ELSE 1 END AS card_cert_city_vs_prev
			, CASE WHEN (LAG(city, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR city=1 THEN -1 WHEN ((LAG(city, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-city) != 0 THEN 0 ELSE 1 END AS city_vs_prev
            , CASE WHEN (LAG(client_ip, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR client_ip=1 THEN -1 WHEN ((LAG(client_ip, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-client_ip) != 0 THEN 0 ELSE 1 END AS client_ip_vs_prev
            , CASE WHEN (LAG(network, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR network=1 THEN -1 WHEN ((LAG(network, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-network) != 0 THEN 0 ELSE 1 END AS network_vs_prev
            , CASE WHEN (LAG(device_sign, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR device_sign=1 THEN -1 WHEN ((LAG(device_sign, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-device_sign) != 0 THEN 0 ELSE 1 END AS device_sign_vs_prev
            , CASE WHEN (LAG(info_1, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR info_1=1 THEN -1 WHEN ((LAG(info_1, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-info_1) != 0 THEN 0 ELSE 1 END AS info_1_vs_prev
            , CASE WHEN (LAG(info_2, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR info_2=1 THEN -1 WHEN ((LAG(info_2, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-info_2) != 0 THEN 0 ELSE 1 END AS info_2_vs_prev
            , CASE WHEN ((LAG(is_one_people, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-is_one_people) != 0 THEN 0 ELSE 1 END AS is_one_people_vs_prev
            , CASE WHEN ((LAG(mobile_oper_platform, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-mobile_oper_platform) != 0 THEN 0 ELSE 1 END AS mobile_oper_platform_vs_prev
            , CASE WHEN ((LAG(operation_channel, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-operation_channel) != 0 THEN 0 ELSE 1 END AS operation_channel_vs_prev
            , CASE WHEN ((LAG(pay_scene, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-pay_scene) != 0 THEN 0 ELSE 1 END AS pay_scene_vs_prev
        	, CASE WHEN (LAG(card_cert_no, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR card_cert_no=1 THEN -1 WHEN ((LAG(card_cert_no, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-card_cert_no) != 0 THEN 0 ELSE 1 END AS card_cert_no_vs_prev
            , CASE WHEN (LAG(opposing_id, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR opposing_id=1 THEN -1 WHEN ((LAG(opposing_id, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-opposing_id) != 0 THEN 0 ELSE 1 END AS opposing_id_vs_prev
            , CASE WHEN (LAG(income_card_no, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR income_card_no=1 THEN -1 WHEN ((LAG(income_card_no, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-income_card_no) != 0 THEN 0 ELSE 1 END AS income_card_no_vs_prev
            , CASE WHEN (LAG(income_card_cert_no, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR income_card_cert_no=1 THEN -1 WHEN ((LAG(income_card_cert_no, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-income_card_cert_no) != 0 THEN 0 ELSE 1 END AS income_card_cert_no_vs_prev
            , CASE WHEN (LAG(income_card_mobile, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR income_card_mobile=1 THEN -1 WHEN ((LAG(income_card_mobile, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-income_card_mobile) != 0 THEN 0 ELSE 1 END AS income_card_mobile_vs_prev
            , CASE WHEN (LAG(income_card_bank_code, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR income_card_bank_code=1 THEN -1 WHEN ((LAG(income_card_bank_code, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-income_card_bank_code) != 0 THEN 0 ELSE 1 END AS income_card_bank_code_vs_prev
            , CASE WHEN (LAG(is_peer_pay, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR is_peer_pay=1 THEN -1 WHEN ((LAG(is_peer_pay, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-is_peer_pay) != 0 THEN 0 ELSE 1 END AS is_peer_pay_vs_prev
            , CASE WHEN (LAG(version, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR version=1 THEN -1 WHEN ((LAG(version, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-version) != 0 THEN 0 ELSE 1 END AS version_vs_prev
        FROM serialized_training
    ) t_main
    INNER JOIN prev_transaction_count_b t_1
    ON t_main.event_id = t_1.event_id
    INNER JOIN prev_feature_count_b t_2
    ON t_main.event_id = t_2.event_id


DROP TABLE IF EXISTS test_1_b;
CREATE TABLE test_1_b AS
SELECT t_main.*
	, t_1.num_prev_transaction
    , t_2.ip_prov_occurences/t_1.num_prev_transaction AS ip_prov_ratio_among_prev
    , t_2.cert_prov_occurences/t_1.num_prev_transaction AS cert_prov_ratio_among_prev
    , t_2.card_bin_prov_occurences/t_1.num_prev_transaction AS card_bin_prov_ratio_among_prev
    , t_2.card_mobile_prov_occurences/t_1.num_prev_transaction AS card_mobile_prov_ratio_among_prev
    , t_2.card_cert_prov_occurences/t_1.num_prev_transaction AS card_cert_prov_ratio_among_prev
    , t_2.province_occurences/t_1.num_prev_transaction AS province_ratio_among_prev
    , t_2.ip_city_occurences/t_1.num_prev_transaction AS ip_city_ratio_among_prev
    , t_2.cert_city_occurences/t_1.num_prev_transaction AS cert_city_ratio_among_prev
    , t_2.card_bin_city_occurences/t_1.num_prev_transaction AS card_bin_city_ratio_among_prev
    , t_2.card_mobile_city_occurences/t_1.num_prev_transaction AS card_mobile_city_ratio_among_prev
    , t_2.card_cert_city_occurences/t_1.num_prev_transaction AS card_cert_city_ratio_among_prev
    , t_2.city_occurences/t_1.num_prev_transaction AS city_ratio_among_prev
    , t_2.client_ip_occurences/t_1.num_prev_transaction AS client_ip_ratio_among_prev
    , t_2.network_occurences/t_1.num_prev_transaction AS network_ratio_among_prev
    , t_2.device_sign_occurences/t_1.num_prev_transaction AS device_sign_ratio_among_prev
    , t_2.info_1_occurences/t_1.num_prev_transaction AS info_1_ratio_among_prev
    , t_2.info_2_occurences/t_1.num_prev_transaction AS info_2_ratio_among_prev
    , t_2.is_one_people_occurences/t_1.num_prev_transaction AS is_one_people_ratio_among_prev
    , t_2.mobile_oper_platform_occurences/t_1.num_prev_transaction AS mobile_oper_platform_ratio_among_prev
    , t_2.operation_channel_occurences/t_1.num_prev_transaction AS operation_channel_ratio_among_prev
    , t_2.pay_scene_occurences/t_1.num_prev_transaction AS pay_scene_ratio_among_prev
    , t_2.card_cert_no_occurences/t_1.num_prev_transaction AS card_cert_no_ratio_among_prev
    , t_2.opposing_id_occurences/t_1.num_prev_transaction AS opposing_id_ratio_among_prev
    , t_2.income_card_no_occurences/t_1.num_prev_transaction AS income_card_no_ratio_among_prev
    , t_2.income_card_cert_no_occurences/t_1.num_prev_transaction AS income_card_cert_no_ratio_among_prev
    , t_2.income_card_mobile_occurences/t_1.num_prev_transaction AS income_card_mobile_ratio_among_prev
    , t_2.income_card_bank_code_occurences/t_1.num_prev_transaction AS income_card_bank_code_ratio_among_prev
    , t_2.is_peer_pay_occurences/t_1.num_prev_transaction AS is_peer_pay_ratio_among_prev
    , t_2.version_occurences/t_1.num_prev_transaction AS version_ratio_among_prev
FROM
    (
        SELECT *
            , CASE WHEN ip_prov=1 OR cert_prov=1 THEN -1 WHEN ip_prov!=cert_prov THEN 0 ELSE 1 END AS ip_prov_vs_cert_prov
            , CASE WHEN ip_prov=1 OR card_bin_prov=1 THEN -1 WHEN ip_prov!=card_bin_prov THEN 0 ELSE 1 END AS ip_prov_vs_card_bin_prov
            , CASE WHEN ip_prov=1 OR card_mobile_prov=1 THEN -1 WHEN ip_prov!=card_mobile_prov THEN 0 ELSE 1 END AS ip_prov_vs_card_mobile_prov
            , CASE WHEN ip_prov=1 OR card_cert_prov=1 THEN -1 WHEN ip_prov!=card_cert_prov THEN 0 ELSE 1 END AS ip_prov_vs_card_cert_prov
            , CASE WHEN ip_prov=1 OR province=1 THEN -1 WHEN ip_prov!=province THEN 0 ELSE 1 END AS ip_prov_vs_province
            , CASE WHEN cert_prov=1 OR card_bin_prov=1 THEN -1 WHEN cert_prov!=card_bin_prov THEN 0 ELSE 1 END AS cert_prov_vs_card_bin_prov
            , CASE WHEN cert_prov=1 OR card_mobile_prov=1 THEN -1 WHEN cert_prov!=card_mobile_prov THEN 0 ELSE 1 END AS cert_prov_vs_card_mobile_prov
            , CASE WHEN cert_prov=1 OR card_cert_prov=1 THEN -1 WHEN cert_prov!=card_cert_prov THEN 0 ELSE 1 END AS cert_prov_vs_card_cert_prov
            , CASE WHEN cert_prov=1 OR province=1 THEN -1 WHEN cert_prov!=province THEN 0 ELSE 1 END AS cert_prov_vs_province
            , CASE WHEN card_bin_prov=1 OR card_mobile_prov=1 THEN -1 WHEN card_bin_prov!=card_mobile_prov THEN 0 ELSE 1 END AS card_bin_prov_vs_card_mobile_prov
            , CASE WHEN card_bin_prov=1 OR card_cert_prov=1 THEN -1 WHEN card_bin_prov!=card_cert_prov THEN 0 ELSE 1 END AS card_bin_prov_vs_card_cert_prov
            , CASE WHEN card_bin_prov=1 OR province=1 THEN -1 WHEN card_bin_prov!=province THEN 0 ELSE 1 END AS card_bin_prov_vs_province
            , CASE WHEN card_mobile_prov=1 OR card_cert_prov=1 THEN -1 WHEN card_mobile_prov!=card_cert_prov THEN 0 ELSE 1 END AS card_mobile_prov_vs_card_cert_prov
            , CASE WHEN card_mobile_prov=1 OR province=1 THEN -1 WHEN card_mobile_prov!=province THEN 0 ELSE 1 END AS card_mobile_prov_vs_province
            , CASE WHEN card_cert_prov=1 OR province=1 THEN -1 WHEN card_cert_prov!=province THEN 0 ELSE 1 END AS card_cert_prov_vs_province
            , CASE WHEN ip_city=1 OR cert_city=1 THEN -1 WHEN ip_city!=cert_city THEN 0 ELSE 1 END AS ip_city_vs_cert_city
            , CASE WHEN ip_city=1 OR card_bin_city=1 THEN -1 WHEN ip_city!=card_bin_city THEN 0 ELSE 1 END AS ip_city_vs_card_bin_city
            , CASE WHEN ip_city=1 OR card_mobile_city=1 THEN -1 WHEN ip_city!=card_mobile_city THEN 0 ELSE 1 END AS ip_city_vs_card_mobile_city
            , CASE WHEN ip_city=1 OR card_cert_city=1 THEN -1 WHEN ip_city!=card_cert_city THEN 0 ELSE 1 END AS ip_city_vs_card_cert_city
            , CASE WHEN ip_city=1 OR city=1 THEN -1 WHEN ip_city!=city THEN 0 ELSE 1 END AS ip_city_vs_city
            , CASE WHEN cert_city=1 OR card_bin_city=1 THEN -1 WHEN cert_city!=card_bin_city THEN 0 ELSE 1 END AS cert_city_vs_card_bin_city
            , CASE WHEN cert_city=1 OR card_mobile_city=1 THEN -1 WHEN cert_city!=card_mobile_city THEN 0 ELSE 1 END AS cert_city_vs_card_mobile_city
            , CASE WHEN cert_city=1 OR card_cert_city=1 THEN -1 WHEN cert_city!=card_cert_city THEN 0 ELSE 1 END AS cert_city_vs_card_cert_city
            , CASE WHEN cert_city=1 OR city=1 THEN -1 WHEN cert_city!=city THEN 0 ELSE 1 END AS cert_city_vs_city
            , CASE WHEN card_bin_city=1 OR card_mobile_city=1 THEN -1 WHEN card_bin_city!=card_mobile_city THEN 0 ELSE 1 END AS card_bin_city_vs_card_mobile_city
            , CASE WHEN card_bin_city=1 OR card_cert_city=1 THEN -1 WHEN card_bin_city!=card_cert_city THEN 0 ELSE 1 END AS card_bin_city_vs_card_cert_city
            , CASE WHEN card_bin_city=1 OR city=1 THEN -1 WHEN card_bin_city!=city THEN 0 ELSE 1 END AS card_bin_city_vs_city
            , CASE WHEN card_mobile_city=1 OR card_cert_city=1 THEN -1 WHEN card_mobile_city!=card_cert_city THEN 0 ELSE 1 END AS card_mobile_city_vs_card_cert_city
            , CASE WHEN card_mobile_city=1 OR city=1 THEN -1 WHEN card_mobile_city!=city THEN 0 ELSE 1 END AS card_mobile_city_vs_city
            , CASE WHEN card_cert_city=1 OR city=1 THEN -1 WHEN card_cert_city!=city THEN 0 ELSE 1 END AS card_cert_city_vs_city
            , DATEPART(TO_DATE(gmt_occur, 'yyyy-mm-dd hh'), 'hh') AS transaction_hour
            , WEEKDAY(TO_DATE(gmt_occur, 'yyyy-mm-dd hh')) AS transaction_dow
            , DATEPART(TO_DATE(gmt_occur, 'yyyy-mm-dd hh'), 'dd') AS transaction_dom
            , DATEDIFF(TO_DATE(gmt_occur, 'yyyy-mm-dd hh'), TO_DATE((LAG(gmt_occur, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC)), 'yyyy-mm-dd hh'), 'hh') AS diff_hour
            , DATEDIFF(TO_DATE(gmt_occur, 'yyyy-mm-dd hh'), TO_DATE((LAG(gmt_occur, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC)), 'yyyy-mm-dd hh'), 'dd') AS diff_day
        	, CASE WHEN (LAG(ip_prov, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR ip_prov=1 THEN -1 WHEN ((LAG(ip_prov, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-ip_prov) != 0 THEN 0 ELSE 1 END AS ip_prov_vs_prev
			, CASE WHEN (LAG(cert_prov, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR cert_prov=1 THEN -1 WHEN ((LAG(cert_prov, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-cert_prov) != 0 THEN 0 ELSE 1 END AS cert_prov_vs_prev
			, CASE WHEN (LAG(card_bin_prov, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR card_bin_prov=1 THEN -1 WHEN ((LAG(card_bin_prov, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-card_bin_prov) != 0 THEN 0 ELSE 1 END AS card_bin_prov_vs_prev
			, CASE WHEN (LAG(card_mobile_prov, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR card_mobile_prov=1 THEN -1 WHEN ((LAG(card_mobile_prov, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-card_mobile_prov) != 0 THEN 0 ELSE 1 END AS card_mobile_prov_vs_prev
			, CASE WHEN (LAG(card_cert_prov, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR card_cert_prov=1 THEN -1 WHEN ((LAG(card_cert_prov, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-card_cert_prov) != 0 THEN 0 ELSE 1 END AS card_cert_prov_vs_prev
			, CASE WHEN (LAG(province, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR province=1 THEN -1 WHEN ((LAG(province, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-province) != 0 THEN 0 ELSE 1 END AS province_vs_prev
			, CASE WHEN (LAG(ip_city, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR ip_city=1 THEN -1 WHEN ((LAG(ip_city, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-ip_city) != 0 THEN 0 ELSE 1 END AS ip_city_vs_prev
			, CASE WHEN (LAG(cert_city, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR cert_city=1 THEN -1 WHEN ((LAG(cert_city, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-cert_city) != 0 THEN 0 ELSE 1 END AS cert_city_vs_prev
			, CASE WHEN (LAG(card_bin_city, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR card_bin_city=1 THEN -1 WHEN ((LAG(card_bin_city, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-card_bin_city) != 0 THEN 0 ELSE 1 END AS card_bin_city_vs_prev
			, CASE WHEN (LAG(card_mobile_city, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR card_mobile_city=1 THEN -1 WHEN ((LAG(card_mobile_city, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-card_mobile_city) != 0 THEN 0 ELSE 1 END AS card_mobile_city_vs_prev
			, CASE WHEN (LAG(card_cert_city, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR card_cert_city=1 THEN -1 WHEN ((LAG(card_cert_city, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-card_cert_city) != 0 THEN 0 ELSE 1 END AS card_cert_city_vs_prev
			, CASE WHEN (LAG(city, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR city=1 THEN -1 WHEN ((LAG(city, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-city) != 0 THEN 0 ELSE 1 END AS city_vs_prev
            , CASE WHEN (LAG(client_ip, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR client_ip=1 THEN -1 WHEN ((LAG(client_ip, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-client_ip) != 0 THEN 0 ELSE 1 END AS client_ip_vs_prev
            , CASE WHEN (LAG(network, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR network=1 THEN -1 WHEN ((LAG(network, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-network) != 0 THEN 0 ELSE 1 END AS network_vs_prev
            , CASE WHEN (LAG(device_sign, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR device_sign=1 THEN -1 WHEN ((LAG(device_sign, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-device_sign) != 0 THEN 0 ELSE 1 END AS device_sign_vs_prev
            , CASE WHEN (LAG(info_1, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR info_1=1 THEN -1 WHEN ((LAG(info_1, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-info_1) != 0 THEN 0 ELSE 1 END AS info_1_vs_prev
            , CASE WHEN (LAG(info_2, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR info_2=1 THEN -1 WHEN ((LAG(info_2, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-info_2) != 0 THEN 0 ELSE 1 END AS info_2_vs_prev
            , CASE WHEN ((LAG(is_one_people, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-is_one_people) != 0 THEN 0 ELSE 1 END AS is_one_people_vs_prev
            , CASE WHEN ((LAG(mobile_oper_platform, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-mobile_oper_platform) != 0 THEN 0 ELSE 1 END AS mobile_oper_platform_vs_prev
            , CASE WHEN ((LAG(operation_channel, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-operation_channel) != 0 THEN 0 ELSE 1 END AS operation_channel_vs_prev
            , CASE WHEN ((LAG(pay_scene, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-pay_scene) != 0 THEN 0 ELSE 1 END AS pay_scene_vs_prev
        	, CASE WHEN (LAG(card_cert_no, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR card_cert_no=1 THEN -1 WHEN ((LAG(card_cert_no, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-card_cert_no) != 0 THEN 0 ELSE 1 END AS card_cert_no_vs_prev
            , CASE WHEN (LAG(opposing_id, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR opposing_id=1 THEN -1 WHEN ((LAG(opposing_id, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-opposing_id) != 0 THEN 0 ELSE 1 END AS opposing_id_vs_prev
            , CASE WHEN (LAG(income_card_no, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR income_card_no=1 THEN -1 WHEN ((LAG(income_card_no, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-income_card_no) != 0 THEN 0 ELSE 1 END AS income_card_no_vs_prev
            , CASE WHEN (LAG(income_card_cert_no, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR income_card_cert_no=1 THEN -1 WHEN ((LAG(income_card_cert_no, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-income_card_cert_no) != 0 THEN 0 ELSE 1 END AS income_card_cert_no_vs_prev
            , CASE WHEN (LAG(income_card_mobile, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR income_card_mobile=1 THEN -1 WHEN ((LAG(income_card_mobile, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-income_card_mobile) != 0 THEN 0 ELSE 1 END AS income_card_mobile_vs_prev
            , CASE WHEN (LAG(income_card_bank_code, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR income_card_bank_code=1 THEN -1 WHEN ((LAG(income_card_bank_code, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-income_card_bank_code) != 0 THEN 0 ELSE 1 END AS income_card_bank_code_vs_prev
            , CASE WHEN (LAG(is_peer_pay, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR is_peer_pay=1 THEN -1 WHEN ((LAG(is_peer_pay, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-is_peer_pay) != 0 THEN 0 ELSE 1 END AS is_peer_pay_vs_prev
            , CASE WHEN (LAG(version, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR version=1 THEN -1 WHEN ((LAG(version, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-version) != 0 THEN 0 ELSE 1 END AS version_vs_prev
        FROM serialized_test_b
    ) t_main
    INNER JOIN prev_transaction_count_b t_1
    ON t_main.event_id = t_1.event_id
    INNER JOIN prev_feature_count_b t_2
    ON t_main.event_id = t_2.event_id
    
--=========================================================
-- change labels, -1 to 1
--=========================================================    
DROP TABLE IF EXISTS training_2_b;
CREATE TABLE training_2_b AS
SELECT *
	,CASE WHEN is_fraud=-1 THEN 1 WHEN is_fraud=0 THEN 0 WHEN is_fraud=1 THEN 1 END AS new_is_fraud
    ,row_number() over(order by gmt_occur ASC) AS RN 
FROM training_1_b

--=========================================================
-- put the predictions in the specified table
--=========================================================
SELECT *
FROM result
			 
-- 20180813
INSERT INTO TABLE result
SELECT event_id
	, CASE WHEN prediction_result=1 THEN prediction_score WHEN prediction_result=0 THEN 1-prediction_score ELSE 0.5 END AS score
FROM test_a_pred

-- 20180814
TRUNCATE TABLE result;
INSERT INTO TABLE result
SELECT event_id
	, CASE WHEN prediction_result=1 THEN prediction_score WHEN prediction_result=0 THEN 1-prediction_score ELSE 0.5 END AS score
FROM test_a_pred_20180814
