--=============================================
-- serialize province related fields (6 fields)
--=============================================
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
            ) a
	) b;

--=============================================
-- serialize city related fields (6 fields)
--=============================================
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
            ) a
	) b;
    
--=============================================
-- serialize id related fields (2 fields)
--=============================================
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
            ) a
	) b;

--=============================================
-- serialize every other field (16 fields)
--=============================================
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
DROP TABLE IF EXISTS serialized_test;
CREATE TABLE serialized_test AS
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
FROM atec_1000w_oota_data AOD
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
CREATE TABLE serialized_train_and_test AS
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
        FROM serialized_test
    ) tmp;
    
--=========================================================
-- add hand crafted features
--=========================================================
DROP TABLE IF EXISTS training_yabin;
CREATE TABLE training_yabin AS
SELECT t_main.*
	, avg_prev_amt
    , std_prev_amt
    , max_prev_amt
    , min_prev_amt
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
            , CASE WHEN (LAG(is_one_people, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR is_one_people=1 THEN -1 WHEN ((LAG(is_one_people, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-is_one_people) != 0 THEN 0 ELSE 1 END AS is_one_people_vs_prev
            , CASE WHEN (LAG(mobile_oper_platform, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR mobile_oper_platform=1 THEN -1 WHEN ((LAG(mobile_oper_platform, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-mobile_oper_platform) != 0 THEN 0 ELSE 1 END AS mobile_oper_platform_vs_prev
            , CASE WHEN (LAG(operation_channel, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR operation_channel=1 THEN -1 WHEN ((LAG(operation_channel, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-operation_channel) != 0 THEN 0 ELSE 1 END AS operation_channel_vs_prev
            , CASE WHEN (LAG(pay_scene, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR pay_scene=1 THEN -1 WHEN ((LAG(pay_scene, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-pay_scene) != 0 THEN 0 ELSE 1 END AS pay_scene_vs_prev
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
    INNER JOIN
    (
        SELECT
            event_id
            , AVG(amt) AS avg_prev_amt
            , STDDEV(amt) AS std_prev_amt
            , MAX(amt) AS max_prev_amt
            , MIN(amt) AS min_prev_amt
        	, COUNT(DISTINCT ip_prov) AS num_prev_ip_prov
            , COUNT(DISTINCT cert_prov) AS num_prev_cert_prov
            , COUNT(DISTINCT card_bin_prov) AS num_prev_card_bin_prov
            , COUNT(DISTINCT card_mobile_prov) AS num_prev_card_mobile_prov
            , COUNT(DISTINCT card_cert_prov) AS num_prev_card_cert_prov
            , COUNT(DISTINCT province) AS num_prev_province
            , COUNT(DISTINCT ip_city) AS num_prev_ip_city
            , COUNT(DISTINCT cert_city) AS num_prev_cert_city
            , COUNT(DISTINCT card_bin_city) AS num_prev_card_bin_city
            , COUNT(DISTINCT card_mobile_city) AS num_prev_card_mobile_city
            , COUNT(DISTINCT card_cert_city) AS num_prev_card_cert_city
            , COUNT(DISTINCT city) AS num_prev_city
            , COUNT(DISTINCT client_ip) AS num_prev_client_ip
            , COUNT(DISTINCT network) AS num_prev_network
            , COUNT(DISTINCT device_sign) AS num_prev_device_sign
            , COUNT(DISTINCT info_1) AS num_prev_info_1
            , COUNT(DISTINCT info_2) AS num_prev_info_2
            , COUNT(DISTINCT is_one_people) AS num_prev_is_one_people
            , COUNT(DISTINCT mobile_oper_platform) AS num_prev_mobile_oper_platform
            , COUNT(DISTINCT operation_channel) AS num_prev_operation_channel
            , COUNT(DISTINCT pay_scene) AS num_prev_pay_scene
            , COUNT(DISTINCT card_cert_no) AS num_prev_card_cert_no
            , COUNT(DISTINCT opposing_id) AS num_prev_opposing_id
            , COUNT(DISTINCT income_card_no) AS num_prev_income_card_no
            , COUNT(DISTINCT income_card_cert_no) AS num_prev_income_card_cert_no
            , COUNT(DISTINCT income_card_mobile) AS num_prev_income_card_mobile
            , COUNT(DISTINCT income_card_bank_code) AS num_prev_income_card_bank_code
            , COUNT(DISTINCT is_peer_pay) AS num_prev_is_peer_pay
            , COUNT(DISTINCT tmp.version) AS num_prev_version
        FROM 
            (
                SELECT t1.event_id
                	, t2.amt
                	, t2.ip_prov
                    , t2.cert_prov
                    , t2.card_bin_prov
                    , t2.card_mobile_prov
                    , t2.card_cert_prov
                    , t2.province
                    , t2.ip_city
                    , t2.cert_city
                    , t2.card_bin_city
                    , t2.card_mobile_city
                    , t2.card_cert_city
                    , t2.city
                    , t2.client_ip
                    , t2.network
                    , t2.device_sign
                    , t2.info_1
                    , t2.info_2
                    , t2.is_one_people
                    , t2.mobile_oper_platform
                    , t2.operation_channel
                    , t2.pay_scene
                    , t2.card_cert_no
                    , t2.opposing_id
                    , t2.income_card_no
                    , t2.income_card_cert_no
                    , t2.income_card_mobile
                    , t2.income_card_bank_code
                    , t2.is_peer_pay
                    , t2.version
                FROM atec_1000w_ins_data t1  --驱动表
                    INNER JOIN atec_1000w_ins_data t2  --history
                    ON t1.user_id = t2.user_id 
                WHERE t1.gmt_occur >= t2.gmt_occur
            )tmp  -- 汇总历史行为信息需要早于本笔时间
        GROUP BY event_id
    ) t_hist
    ON t_main.event_id = t_hist.event_id

DROP TABLE IF EXISTS test_yabin;
CREATE TABLE test_yabin AS
SELECT t_main.*
	, avg_prev_amt
    , std_prev_amt
    , max_prev_amt
    , min_prev_amt
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
            , CASE WHEN (LAG(is_one_people, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR is_one_people=1 THEN -1 WHEN ((LAG(is_one_people, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-is_one_people) != 0 THEN 0 ELSE 1 END AS is_one_people_vs_prev
            , CASE WHEN (LAG(mobile_oper_platform, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR mobile_oper_platform=1 THEN -1 WHEN ((LAG(mobile_oper_platform, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-mobile_oper_platform) != 0 THEN 0 ELSE 1 END AS mobile_oper_platform_vs_prev
            , CASE WHEN (LAG(operation_channel, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR operation_channel=1 THEN -1 WHEN ((LAG(operation_channel, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-operation_channel) != 0 THEN 0 ELSE 1 END AS operation_channel_vs_prev
            , CASE WHEN (LAG(pay_scene, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR pay_scene=1 THEN -1 WHEN ((LAG(pay_scene, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-pay_scene) != 0 THEN 0 ELSE 1 END AS pay_scene_vs_prev
            , CASE WHEN (LAG(card_cert_no, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR card_cert_no=1 THEN -1 WHEN ((LAG(card_cert_no, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-card_cert_no) != 0 THEN 0 ELSE 1 END AS card_cert_no_vs_prev
            , CASE WHEN (LAG(opposing_id, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR opposing_id=1 THEN -1 WHEN ((LAG(opposing_id, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-opposing_id) != 0 THEN 0 ELSE 1 END AS opposing_id_vs_prev
            , CASE WHEN (LAG(income_card_no, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR income_card_no=1 THEN -1 WHEN ((LAG(income_card_no, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-income_card_no) != 0 THEN 0 ELSE 1 END AS income_card_no_vs_prev
            , CASE WHEN (LAG(income_card_cert_no, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR income_card_cert_no=1 THEN -1 WHEN ((LAG(income_card_cert_no, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-income_card_cert_no) != 0 THEN 0 ELSE 1 END AS income_card_cert_no_vs_prev
            , CASE WHEN (LAG(income_card_mobile, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR income_card_mobile=1 THEN -1 WHEN ((LAG(income_card_mobile, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-income_card_mobile) != 0 THEN 0 ELSE 1 END AS income_card_mobile_vs_prev
            , CASE WHEN (LAG(income_card_bank_code, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR income_card_bank_code=1 THEN -1 WHEN ((LAG(income_card_bank_code, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-income_card_bank_code) != 0 THEN 0 ELSE 1 END AS income_card_bank_code_vs_prev
            , CASE WHEN (LAG(is_peer_pay, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR is_peer_pay=1 THEN -1 WHEN ((LAG(is_peer_pay, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-is_peer_pay) != 0 THEN 0 ELSE 1 END AS is_peer_pay_vs_prev
            , CASE WHEN (LAG(version, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))=1 OR version=1 THEN -1 WHEN ((LAG(version, 1) OVER (PARTITION BY user_id ORDER BY gmt_occur ASC))-version) != 0 THEN 0 ELSE 1 END AS version_vs_prev
        FROM test_yabin
    ) t_main
    INNER JOIN
    (
        SELECT is_train
            , event_id
            , AVG(amt) AS avg_prev_amt
            , STDDEV(amt) AS std_prev_amt
            , MAX(amt) AS max_prev_amt
            , MIN(amt) AS min_prev_amt
        	, COUNT(DISTINCT ip_prov) AS num_prev_ip_prov
            , COUNT(DISTINCT cert_prov) AS num_prev_cert_prov
            , COUNT(DISTINCT card_bin_prov) AS num_prev_card_bin_prov
            , COUNT(DISTINCT card_mobile_prov) AS num_prev_card_mobile_prov
            , COUNT(DISTINCT card_cert_prov) AS num_prev_card_cert_prov
            , COUNT(DISTINCT province) AS num_prev_province
            , COUNT(DISTINCT ip_city) AS num_prev_ip_city
            , COUNT(DISTINCT cert_city) AS num_prev_cert_city
            , COUNT(DISTINCT card_bin_city) AS num_prev_card_bin_city
            , COUNT(DISTINCT card_mobile_city) AS num_prev_card_mobile_city
            , COUNT(DISTINCT card_cert_city) AS num_prev_card_cert_city
            , COUNT(DISTINCT city) AS num_prev_city
            , COUNT(DISTINCT client_ip) AS num_prev_client_ip
            , COUNT(DISTINCT network) AS num_prev_network
            , COUNT(DISTINCT device_sign) AS num_prev_device_sign
            , COUNT(DISTINCT info_1) AS num_prev_info_1
            , COUNT(DISTINCT info_2) AS num_prev_info_2
            , COUNT(DISTINCT is_one_people) AS num_prev_is_one_people
            , COUNT(DISTINCT mobile_oper_platform) AS num_prev_mobile_oper_platform
            , COUNT(DISTINCT operation_channel) AS num_prev_operation_channel
            , COUNT(DISTINCT pay_scene) AS num_prev_pay_scene
            , COUNT(DISTINCT card_cert_no) AS num_prev_card_cert_no
            , COUNT(DISTINCT opposing_id) AS num_prev_opposing_id
            , COUNT(DISTINCT income_card_no) AS num_prev_income_card_no
            , COUNT(DISTINCT income_card_cert_no) AS num_prev_income_card_cert_no
            , COUNT(DISTINCT income_card_mobile) AS num_prev_income_card_mobile
            , COUNT(DISTINCT income_card_bank_code) AS num_prev_income_card_bank_code
            , COUNT(DISTINCT is_peer_pay) AS num_prev_is_peer_pay
            , COUNT(DISTINCT tmp.version) AS num_prev_version
        FROM 
            (
                SELECT t1.is_train
                	, t1.event_id
                	, t2.amt
                	, t2.ip_prov
                    , t2.cert_prov
                    , t2.card_bin_prov
                    , t2.card_mobile_prov
                    , t2.card_cert_prov
                    , t2.province
                    , t2.ip_city
                    , t2.cert_city
                    , t2.card_bin_city
                    , t2.card_mobile_city
                    , t2.card_cert_city
                    , t2.city
                    , t2.client_ip
                    , t2.network
                    , t2.device_sign
                    , t2.info_1
                    , t2.info_2
                    , t2.is_one_people
                    , t2.mobile_oper_platform
                    , t2.operation_channel
                    , t2.pay_scene
                    , t2.card_cert_no
                    , t2.opposing_id
                    , t2.income_card_no
                    , t2.income_card_cert_no
                    , t2.income_card_mobile
                    , t2.income_card_bank_code
                    , t2.is_peer_pay
                    , t2.version
                FROM serialized_train_and_test t1  --驱动表
                    INNER JOIN serialized_train_and_test t2  --history
                    ON t1.user_id = t2.user_id 
                WHERE t1.gmt_occur >= t2.gmt_occur
            )tmp  -- 汇总历史行为信息需要早于本笔时间
        WHERE is_train=0
        GROUP BY event_id
    ) t_hist
    ON t_main.event_id = t_hist.event_id
