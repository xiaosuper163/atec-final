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