SET hive.exec.dynamic.partition=true;  
SET hive.exec.dynamic.partition.mode=nonstrict; 

-- 1- ACTIF_DATA_ET_DATE_ACTIVATION

Drop table if exists oic.TT_ACTIF_DATA_ET_DATE_ACTIVATION_APRES;
Drop table if exists OIC.TT_ACTIF_DATA_ET_DATE_ACTIVATION_AVANT;
Drop table if exists OIC.TT_ACTIF_DATA_ET_DATE_ACTIVATION_AVANT2;

Create table oic.TT_ACTIF_DATA_ET_DATE_ACTIVATION_APRES As 
select * FROM OIC.TT_ACTIF_DATA_ET_DATE_ACTIVATION where date_activation not like upper('%AVANT%')
and upper(date_activation) not like upper('%date%');

Create table oic.TT_ACTIF_DATA_ET_DATE_ACTIVATION_AVANT As 
select * FROM OIC.TT_ACTIF_DATA_ET_DATE_ACTIVATION where date_activation like upper('%AVANT%');

Create table if not exists OIC.TT_ACTIF_DATA_ET_DATE_ACTIVATION_AVANT2
as
SELECT
  CAST(MSISDN AS BIGINT),
  case
    when date_activation = 'AVANT_MAI_2011' then '201001'
  end DATE_ACTIVATION
FROM OIC.TT_ACTIF_DATA_ET_DATE_ACTIVATION_AVANT;

INSERT OVERWRITE TABLE OIC.TF_ACTIF_DATA_ET_DATE_ACTIVATION PARTITION (PRT_DATE)
SELECT
  CAST(MSISDN AS BIGINT),
  FROM_UNIXTIME(UNIX_TIMESTAMP(DATE_ACTIVATION, 'yyyyMM')),
  DATE_ACTIVATION
from oic.TT_ACTIF_DATA_ET_DATE_ACTIVATION_APRES;

INSERT INTO TABLE OIC.TF_ACTIF_DATA_ET_DATE_ACTIVATION PARTITION (PRT_DATE)
SELECT
  CAST(MSISDN AS BIGINT),
  FROM_UNIXTIME(UNIX_TIMESTAMP(DATE_ACTIVATION, 'yyyyMM')),
  DATE_ACTIVATION
from oic.TT_ACTIF_DATA_ET_DATE_ACTIVATION_AVANT2;

Drop table if exists oic.TT_ACTIF_DATA_ET_DATE_ACTIVATION_APRES;
Drop table if exists OIC.TT_ACTIF_DATA_ET_DATE_ACTIVATION_AVANT;
Drop table if exists OIC.TT_ACTIF_DATA_ET_DATE_ACTIVATION_AVANT2;

-- 2- BUNDLE

INSERT OVERWRITE TABLE OIC.TF_BUNDLE PARTITION(PRT_DATE)
SELECT
  FROM_UNIXTIME(UNIX_TIMESTAMP(DATE_SOUSCRIPTION, 'yyyyMMdd')),
  CAST(MSISDN AS BIGINT),
  Cast(Trim(TYPE_SOUSCRIPTION) As String),
  CAST(cast(REGEXP_REPLACE(NOMBRE_SOUSCRIPTION, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(MONTANT_SOUSCRIPTION, ',', '.') as float) AS int),
  DATE_SOUSCRIPTION
FROM OIC.TT_BUNDLE WHERE TYPE_SOUSCRIPTION <> 'TYPE_SOUSCRIPTION';

INSERT INTO TABLE OIC.TF_BUNDLE PARTITION(PRT_DATE)
SELECT
  FROM_UNIXTIME(UNIX_TIMESTAMP(DATE_SOUSCRIPTION, 'yyyyMMdd')),
  CAST(MSISDN AS BIGINT),
  Cast(Trim(TYPE_SOUSCRIPTION) As String),
  CAST(cast(REGEXP_REPLACE(NOMBRE_SOUSCRIPTION, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(MONTANT_SOUSCRIPTION, ',', '.') as float) AS int),
  DATE_SOUSCRIPTION
FROM OIC.TT_BUNDLE_2 WHERE TYPE_SOUSCRIPTION <> 'TYPE_SOUSCRIPTION';

-- 3- CDR
-- 3-1- TF_CDR_TRAFFIC_CUSTOMER_CELL

INSERT OVERWRITE TABLE OIC.TF_CDR_TRAFFIC_CUSTOMER_CELL PARTITION (PRT_DATE)
SELECT
  FROM_UNIXTIME(UNIX_TIMESTAMP(day, 'yyyy-MM-dd')),
  CAST(idCust AS String),
  CAST(idGGSN AS int),
  CAST(idRAT AS int),
  CAST(trim(idTAC) AS String),
  CAST(idCell AS BIGINT),
  CAST(trim(roaming) AS String),
  CAST(trim(nBytesDn) AS BIGINT),
  CAST(trim(nBytesUp) AS BIGINT),
  Day
FROM OIC.TT_CDR_TRAFFIC_CUSTOMER_CELL where idCust not like upper('%idCust%');

-- 3-2- TF_CDR_TRAFFIC_CUSTOMER

INSERT OVERWRITE TABLE OIC.TF_CDR_TRAFFIC_CUSTOMER PARTITION (PRT_DATE)
SELECT
  FROM_UNIXTIME(UNIX_TIMESTAMP(day, 'yyyy-MM-dd')),
  CAST(idCust AS String),
  CAST(idAppli AS BIGINT),
  CAST(idAPN AS String),
  CAST(trim(idTAC) AS String),
  CAST(idRAT AS Int),
  CAST(idGGSN AS Int),
  CAST(trim(roaming) AS String),
  CAST(nBytesDn AS BIGINT),
  CAST(nBytesUp AS BIGINT),
  CAST(nBytesT AS BIGINT),
  Cast(Trim(Terminal_type) As String),
  Cast(Trim(Terminal_brand) As String),
  Cast(Trim(Terminal_model) As String),
  Cast(Trim(Subscriber_type) As String),
  Cast(Trim(Service_type) As String),
  CAST(idContract AS String),
  Day
FROM OIC.TT_CDR_TRAFFIC_CUSTOMER where idCust not like upper('%idCust%');

-- 3-3- Conversions_Id; Table = OIC.TF_CONVERSIONS_ID_*;
-- Table = OIC.TF_CONVERSIONS_ID_APN;

INSERT OVERWRITE TABLE OIC.TF_CONVERSIONS_ID_APN
SELECT
  CAST(Id_APN AS BIGINT),
  Cast(Trim(Description_APN) As String)
FROM OIC.TT_CONVERSIONS_ID_APN ;

-- Table = OIC.TF_CONVERSIONS_ID_APPLI;

INSERT OVERWRITE TABLE OIC.TF_CONVERSIONS_ID_APPLI
SELECT
  CAST(Id_appli AS BIGINT),
  Cast(Trim(Description_appli) As String)
FROM OIC.TT_CONVERSIONS_ID_APPLI;

-- Table = OIC.TF_CONVERSIONS_ID_CUST_CONTACT;

INSERT OVERWRITE TABLE OIC.TF_CONVERSIONS_ID_CUST_CONTACT
SELECT
  CAST(Id_cust_contract AS BIGINT),
  Cast(Trim(Description_Contract) As String)
FROM OIC.TT_CONVERSIONS_ID_CUST_CONTACT;

-- Table = OIC.TF_CONVERSIONS_ID_GGSN;

INSERT OVERWRITE TABLE OIC.TF_CONVERSIONS_ID_GGSN
SELECT
  CAST(Id_GGSN AS Int),
  Cast(Trim(Description_GGSN) As String),
  Cast(Trim(Address_GGSN) As String)
FROM OIC.TT_CONVERSIONS_ID_GGSN;

-- Table = OIC.TF_CONVERSIONS_ID_RAT;

INSERT OVERWRITE TABLE OIC.TF_CONVERSIONS_ID_RAT
SELECT
  CAST(Id_RAT AS Int),
  CAST(Trim(Description_RAT) As String)
FROM OIC.TT_CONVERSIONS_ID_RAT;

-- Table = OIC.TF_CONVERSIONS_ID_CUST_MSISDN;

INSERT OVERWRITE TABLE OIC.TF_CONVERSIONS_ID_CUST_MSISDN
SELECT
  Cast(Trim(Id_customer) As String),
  CAST(MSISDN AS BIGINT)
FROM OIC.TT_CONVERSIONS_ID_CUST_MSISDN;

-- 3-4- Localisation_cellules; Table = OIC.TF_LOCALISATION_CELLULES;

INSERT OVERWRITE TABLE OIC.TF_LOCALISATION_CELLULES
SELECT
  CAST(Id_Dashotarie AS BIGINT),
  CAST(cgi AS BIGINT),
  Cast(trim(bsc) As String),
  Cast(trim(msc) As String),
  Cast(trim(utrancell) As String),
  Cast(trim(nodob) As String),
  Cast(trim(rnc) As String),
  Cast(trim(cell) As String),
  Cast(trim(enodeb) As String),
  Cast(trim(lat) As String),
  Cast(trim(lon) As String),
  Cast(trim(GEOLOC) As String),
  Cast(trim(COMMUNE) As String),
  Cast(trim(DEPARTEMENT) As String),
  Cast(trim(REGION) As String),
  Cast(trim(DISTRICT)  As String)
FROM OIC.TT_LOCALISATION_CELLULES where upper(cgi) not like upper('%cgi%');

-- 4- DONNEES_HISTO_MENSUELLES

Drop table if exists oic.TT_DONNEES_HISTO_MENSUELLES_APRES;
Drop table if exists oic.TT_DONNEES_HISTO_MENSUELLES_AVANT;
Drop table if exists oic.TT_DONNEES_HISTO_MENSUELLES_AVANT2;

Create table oic.TT_DONNEES_HISTO_MENSUELLES_APRES As 
select * FROM OIC.TT_DONNEES_HISTO_MENSUELLES where upper(date_activation) not like upper('%AVANT%') and
upper(date_activation) not like upper('%date%');

Create table oic.TT_DONNEES_HISTO_MENSUELLES_AVANT As 
select * FROM OIC.TT_DONNEES_HISTO_MENSUELLES where upper(date_activation) like upper('%AVANT%');

Create table if not exists OIC.TT_DONNEES_HISTO_MENSUELLES_AVANT2
as
SELECT
  MONTH_ID,
  MSISDN,
  GEOLOCALITE,
  COMMUNE,
  DEPARTEMENT,
  REGION,
  REGION_CAT,
  DISTRIBUTEUR,
  case
    when date_activation = 'AVANT_MAI_2011' then '201001'
  end DATE_ACTIVATION,
  STATUT_OM,
  STATUT_MSIM,
  STATUT_MOOV,
  STATUT_MTN,
  SEGMENT_VALEUR,
  PLAN_TARIFAIRE,
  MONTANT_RECHARGE,
  REV_TOT_CONSOMME,
  REV_SORTANT,
  REV_ENTRANT,
  REV_VOIX,
  REV_SMS,
  REV_DATA,
  REV_PASS,
  REV_SVA,
  VOL_TOT_VOIX_SORTANT,
  NB_TOT_VOIX_SORTANT,
  NB_TOT_SMS_SORTANT,
  NB_VOIX_OFFNET_SORTANT,
  NB_VOIX_ONNET_SORTANT,
  NB_VOIX_INTER,
  NB_VOIX_ROAM_SORTANT,
  NB_VOIX_ROAM_ENTRANT,
  NB_SMS_OFFNET_SORTANT,
  NB_SMS_INTER_SORTANT,
  NB_SMS_ONET_SORTANT,
  NB_SMS_ROAM_SORTANT,
  VOL_TOT_VOIX_OFFNET_SORTANT,
  VOL_TOT_VOIX_ONNET_SORTANT,
  VOL_TOT_VOIX_INTER_SORTANT,
  VOL_VOIX_ROAM_SORTANT,
  VOL_VOIX_ROAM_ENTRANT,
  VOL_DATA_ROAM,
  VOL_CONSOMME
FROM OIC.TT_DONNEES_HISTO_MENSUELLES_AVANT;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE OIC.TF_DONNEES_HISTO_MENSUELLES PARTITION (PRT_DATE)
SELECT
  FROM_UNIXTIME(UNIX_TIMESTAMP(MONTH_ID, 'yyyyMM')),
  CAST(MSISDN AS BIGINT),
  Cast(Trim(GEOLOCALITE) As String),
  Cast(Trim(COMMUNE) As String),
  Cast(Trim(DEPARTEMENT) As String),
  Cast(Trim(REGION) As String),
  Cast(Trim(REGION_CAT) As String),
  Cast(Trim(DISTRIBUTEUR) As String),
  FROM_UNIXTIME(UNIX_TIMESTAMP(DATE_ACTIVATION, 'yyyyMM')),
  CAST(STATUT_OM AS String),
  Cast(Trim(STATUT_MSIM) As String),
  Cast(Trim(STATUT_MOOV) As String),
  Cast(Trim(STATUT_MTN) As String),
  Cast(Trim(SEGMENT_VALEUR) As String),
  Cast(Trim(PLAN_TARIFAIRE) As String), 
  CAST(REGEXP_REPLACE(MONTANT_RECHARGE, ',', '.') AS FLOAT),
  CAST(REGEXP_REPLACE(REV_TOT_CONSOMME, ',', '.') AS FLOAT),
  CAST(REGEXP_REPLACE(REV_SORTANT, ',', '.') AS FLOAT),
  CAST(REGEXP_REPLACE(REV_ENTRANT, ',', '.') AS FLOAT),
  CAST(REGEXP_REPLACE(REV_VOIX, ',', '.') AS FLOAT),
  CAST(REGEXP_REPLACE(REV_SMS, ',', '.') AS FLOAT),
  CAST(REGEXP_REPLACE(REV_DATA, ',', '.') AS FLOAT),
  CAST(REGEXP_REPLACE(REV_PASS, ',', '.') AS FLOAT),
  CAST(REGEXP_REPLACE(REV_SVA, ',', '.') AS FLOAT),
  CAST(cast(REGEXP_REPLACE(VOL_TOT_VOIX_SORTANT, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(NB_TOT_VOIX_SORTANT, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(NB_TOT_SMS_SORTANT, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(NB_VOIX_OFFNET_SORTANT, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(NB_VOIX_ONNET_SORTANT, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(NB_VOIX_INTER, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(NB_VOIX_ROAM_SORTANT, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(NB_VOIX_ROAM_ENTRANT, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(NB_SMS_OFFNET_SORTANT, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(NB_SMS_INTER_SORTANT, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(NB_SMS_ONET_SORTANT, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(NB_SMS_ROAM_SORTANT, ',', '.') as float) AS int),
  CAST(REGEXP_REPLACE(VOL_TOT_VOIX_OFFNET_SORTANT, ',', '.') AS FLOAT),
  CAST(REGEXP_REPLACE(VOL_TOT_VOIX_ONNET_SORTANT, ',', '.') AS FLOAT),
  CAST(REGEXP_REPLACE(VOL_TOT_VOIX_INTER_SORTANT, ',', '.') AS FLOAT),
  CAST(REGEXP_REPLACE(VOL_VOIX_ROAM_SORTANT, ',', '.') AS FLOAT),
  CAST(REGEXP_REPLACE(VOL_VOIX_ROAM_ENTRANT, ',', '.') AS FLOAT),
  CAST(REGEXP_REPLACE(VOL_DATA_ROAM, ',', '.') AS FLOAT),
  CAST(REGEXP_REPLACE(VOL_CONSOMME, ',', '.') AS FLOAT),
  MONTH_ID
FROM OIC.TT_DONNEES_HISTO_MENSUELLES_APRES;

INSERT INTO TABLE OIC.TF_DONNEES_HISTO_MENSUELLES PARTITION (PRT_DATE)
SELECT
  FROM_UNIXTIME(UNIX_TIMESTAMP(MONTH_ID, 'yyyyMM')),
  CAST(MSISDN AS BIGINT),
  Cast(Trim(GEOLOCALITE) As String),
  Cast(Trim(COMMUNE) As String),
  Cast(Trim(DEPARTEMENT) As String),
  Cast(Trim(REGION) As String),
  Cast(Trim(REGION_CAT) As String),
  Cast(Trim(DISTRIBUTEUR) As String),
  FROM_UNIXTIME(UNIX_TIMESTAMP(DATE_ACTIVATION, 'yyyyMM')),
  CAST(STATUT_OM AS String),
  Cast(Trim(STATUT_MSIM) As String),
  Cast(Trim(STATUT_MOOV) As String),
  Cast(Trim(STATUT_MTN) As String),
  Cast(Trim(SEGMENT_VALEUR) As String),
  Cast(Trim(PLAN_TARIFAIRE) As String), 
  CAST(REGEXP_REPLACE(MONTANT_RECHARGE, ',', '.') AS FLOAT),
  CAST(REGEXP_REPLACE(REV_TOT_CONSOMME, ',', '.') AS FLOAT),
  CAST(REGEXP_REPLACE(REV_SORTANT, ',', '.') AS FLOAT),
  CAST(REGEXP_REPLACE(REV_ENTRANT, ',', '.') AS FLOAT),
  CAST(REGEXP_REPLACE(REV_VOIX, ',', '.') AS FLOAT),
  CAST(REGEXP_REPLACE(REV_SMS, ',', '.') AS FLOAT),
  CAST(REGEXP_REPLACE(REV_DATA, ',', '.') AS FLOAT),
  CAST(REGEXP_REPLACE(REV_PASS, ',', '.') AS FLOAT),
  CAST(REGEXP_REPLACE(REV_SVA, ',', '.') AS FLOAT),
  CAST(cast(REGEXP_REPLACE(VOL_TOT_VOIX_SORTANT, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(NB_TOT_VOIX_SORTANT, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(NB_TOT_SMS_SORTANT, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(NB_VOIX_OFFNET_SORTANT, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(NB_VOIX_ONNET_SORTANT, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(NB_VOIX_INTER, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(NB_VOIX_ROAM_SORTANT, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(NB_VOIX_ROAM_ENTRANT, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(NB_SMS_OFFNET_SORTANT, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(NB_SMS_INTER_SORTANT, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(NB_SMS_ONET_SORTANT, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(NB_SMS_ROAM_SORTANT, ',', '.') as float) AS int),
  CAST(REGEXP_REPLACE(VOL_TOT_VOIX_OFFNET_SORTANT, ',', '.') AS FLOAT),
  CAST(REGEXP_REPLACE(VOL_TOT_VOIX_ONNET_SORTANT, ',', '.') AS FLOAT),
  CAST(REGEXP_REPLACE(VOL_TOT_VOIX_INTER_SORTANT, ',', '.') AS FLOAT),
  CAST(REGEXP_REPLACE(VOL_VOIX_ROAM_SORTANT, ',', '.') AS FLOAT),
  CAST(REGEXP_REPLACE(VOL_VOIX_ROAM_ENTRANT, ',', '.') AS FLOAT),
  CAST(REGEXP_REPLACE(VOL_DATA_ROAM, ',', '.') AS FLOAT),
  CAST(REGEXP_REPLACE(VOL_CONSOMME, ',', '.') AS FLOAT),
  MONTH_ID
FROM oic.TT_DONNEES_HISTO_MENSUELLES_AVANT2;

Drop table if exists oic.TT_DONNEES_HISTO_MENSUELLES_APRES;
Drop table if exists oic.TT_DONNEES_HISTO_MENSUELLES_AVANT;
Drop table if exists oic.TT_DONNEES_HISTO_MENSUELLES_AVANT2;

-- 5- ENTRANT

INSERT OVERWRITE TABLE OIC.TF_CDR_ENTRANT PARTITION (PRT_DATE)
SELECT
  FROM_UNIXTIME(UNIX_TIMESTAMP(DATE_ID, 'yyyyMMdd')),
  CAST(MSISDN AS String),
  CAST(Trim(MSISDN_APPELANT) AS String),
  CAST(Trim(MSISDN_APPELE) AS BIGINT),
  Cast(Trim(DESTINATION) As String),
  Cast(Trim(TYPE_EVENT) As String),
  Cast(Trim(SENS_EVENT) As String),
  Cast(Trim(OPERATEUR_APPELE) As String),
  Cast(Trim(OPERATEUR_APPELANT) As String),
  Cast(Trim(TYPE_CELLULE) As String),
  CAST(cast(REGEXP_REPLACE(CI, ',', '.') as float) AS BIGINT),
  CAST(cast(REGEXP_REPLACE(DUREE_EVENT, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(MONTANT_VALORISE, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(MONTANT_BUNDLE, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(MONTANT_BONUS, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(VOL_DATA_CONSOMME, ',', '.') as float) AS BIGINT),
  CAST(cast(REGEXP_REPLACE(VOL_DATA_DOWN, ',', '.') as float) AS BIGINT),
  CAST(cast(REGEXP_REPLACE(VOL_DATA_UP, ',', '.') as float) AS BIGINT),
  DATE_ID
FROM OIC.TT_CDR_ENTRANT where upper(msisdn) not like upper('%msisdn%');


-- 6- INFO_TERMINAUX

INSERT OVERWRITE TABLE OIC.TF_INFO_TERMINAUX PARTITION (PRT_DATE)
SELECT
  FROM_UNIXTIME(UNIX_TIMESTAMP(DATE_ID, 'yyyyMMdd')),
  CAST(Trim(MSISDN) AS BIGINT),
  substr(CAST(trim(CODE_TAC) AS String),0,8),
  CAST(Trim(MODELE) As String),
  CAST(Trim(MARQUE) As String),
  CAST(Trim(DEVICE_TYPE) As String),
  DATE_ID
FROM OIC.TT_INFO_TERMINAUX where upper(msisdn) not like upper('%msisdn%');

-- 7- MSIM

INSERT OVERWRITE TABLE OIC.TF_MSIM
SELECT
  CAST(MSISDN AS BIGINT),
  CAST(cast(REGEXP_REPLACE(STATUT_ORANGE, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(STATUT_MTN, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(STATUT_MOOV, ',', '.') as float) AS int)
FROM OIC.TT_MSIM where upper(msisdn) not like upper('%msisdn%');


-- 8- RECHARGE

INSERT OVERWRITE TABLE OIC.TF_RECHARGE PARTITION (PRT_DATE)
SELECT
  FROM_UNIXTIME(UNIX_TIMESTAMP(DATE_RECHARGEMENT, 'yyyyMMdd')),
  CAST(MSISDN AS BIGINT),
  Cast(trim(TYPE_RECHARGEMENT) As String),
  CAST(cast(REGEXP_REPLACE(MONTANT_RECHARGEMNT, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(NOMBRE_RECHARGEMNT, ',', '.') as float) AS int),
  DATE_RECHARGEMENT
FROM OIC.TT_RECHARGE where upper(msisdn) not like upper('%msisdn%');

INSERT INTO TABLE OIC.TF_RECHARGE PARTITION (PRT_DATE)
SELECT
  FROM_UNIXTIME(UNIX_TIMESTAMP(DATE_RECHARGEMENT, 'yyyyMMdd')),
  CAST(MSISDN AS BIGINT),
  Cast(trim(TYPE_RECHARGEMENT) As String),
  CAST(cast(REGEXP_REPLACE(MONTANT_RECHARGEMNT, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(NOMBRE_RECHARGEMNT, ',', '.') as float) AS int),
  DATE_RECHARGEMENT
FROM OIC.TT_RECHARGE_2 where upper(msisdn) not like upper('%msisdn%');

-- 9- REFERENTIEL_TERMINAUX

INSERT OVERWRITE TABLE OIC.TF_REFERENTIEL_TERMINAUX
SELECT
  CAST(trim(TAC) AS String),
  Cast(trim(PRODUCT_ID) As String),
  Cast(trim(SUPPLIER_NAME) As String),
  Cast(trim(MODEL_NAME) As String),
  Cast(trim(MODEL_NAME_OTHER) As String),
  Cast(trim(DEVICE_TYPE) As String),
  Cast(trim(SMARTPHONE_FLAG) As String),
  Cast(trim(PLATFORM) As String),
  Cast(trim(ADRESSBOOK_INTEGR) As String),
  Cast(trim(A_GPS) As String),
  Cast(trim(BLUETOOTH_FLAG) As String),
  Cast(trim(BLUETOOTH) As String),
  Cast(trim(BROWSER) As String),
  Cast(trim(BROWSING_LEVEL) As String),
  Cast(trim(CALCULATED_DP) As String),
  Cast(trim(CALL_HARD_KEY_FLAG) As String),
  Cast(trim(CAMERA_FLAG) As String),
  Cast(trim(CAMERA_FLASH_FLAG) As String),
  Cast(trim(CAMERA_RESOLUTION) As String),
  Cast(trim(CAMERA) As String),
  Cast(trim(CAMERA_ZOOM_FLAG) As String),
  Cast(trim(CATEGORY) As String),
  Cast(trim(CHARGE_USB_FLAG) As String),
  Cast(trim(COLOUR_SCREEN_FLAG) As String),
  Cast(trim(COMPASS_FLAG) As String),
  Cast(trim(CONNECT_DEVICE) As String),
  Cast(trim(CONTACT_CONTENT) As String),
  Cast(trim(DEST_POP_UP_FLAG) As String),
  Cast(trim(DIMENSIONS) As String),
  Cast(trim(DOWNLOADABLE) As String),
  Cast(trim(DP_MARKETING) As String),
  Cast(trim(DP_TEK) As String),
  Cast(trim(DRM_FOR_MUSIC_FLAG) As String),
  Cast(trim(DRM_FOR_VIDEO) As String),
  Cast(trim(DVBH_FLAG) As String),
  Cast(trim(DVP_CONFIRMATION) As String),
  Cast(trim(EDGE_FLAG) As String),
  Cast(trim(EDGE) As String),
  Cast(trim(EMAIL_WIZ) As String),
  Cast(trim(FM_RADIO_FLAG) As String),
  Cast(trim(FORM_FACTOR) As String),
  Cast(trim(GPRS_FLAG) As String),
  Cast(trim(GPRS_MULTISLOT_CLASS) As String),
  Cast(trim(GSM_BAND) As String),
  Cast(trim(GSM_BAND_FLAG) As String),
  Cast(trim(HANDSFREE_SPEAKER_FLAG) As String),
  Cast(trim(HANG_UP_HARD_KEY_FLAG) As String),
  Cast(trim(HDVOICE_FLAG) As String),
  Cast(trim(HEADSET) As String),
  Cast(trim(HOME_SCREEN_FLAG) As String),
  Cast(trim(HOMESCREEN_PAGE_NO) As String),
  Cast(trim(HOMESCREEN_VERSION) As String),
  Cast(trim(HSDPA_CATEGORY) As String),
  Cast(trim(HSDPA_FLAG) As String),
  Cast(trim(HSUPA_CATEGORY) As String),
  Cast(trim(HSUPA_FLAG) As String),
  Cast(trim(HTML_FLAG) As String),
  Cast(trim(HTML_VERSION) As String),
  Cast(trim(INSTANT_MSG_CLIENT) As String),
  Cast(trim(IS_BUNDLE) As String),
  Cast(trim(JAVA_VERSION) As String),
  Cast(trim(JOYN) As String),
  Cast(trim(JSR) As String),
  Cast(trim(KEYBOARD) As String),
  Cast(trim(LAUNCH_DATE_COME) As String),
  Cast(trim(LENGHT_MM) As String),
  Cast(trim(LOCAL_SYNCHRONISATION) As String),
  Cast(trim(LTE) As String),
  Cast(trim(MANUFACTURER_BUNDLE) As String),
  Cast(trim(MEMORY_TYPE_EXT) As String),
  Cast(trim(MMS_FLAG) As String),
  Cast(trim(MODEL_NACODE_BUNDLE) As String),
  Cast(trim(MODEM) As String),
  Cast(trim(MOUSE_FLAG) As String),
  Cast(trim(MULTITOUCH_FLAG) As String),
  Cast(trim(MUSIC_PLAYER_FLAG) As String),
  Cast(trim(NB_CPU_FOR_MM) As String),
  Cast(trim(NFC_FLAG) As String),
  Cast(trim(NFC_TAG_READING_WRITING) As String),
  Cast(trim(NUMBER_OF_SIGNATURE_APPS) As String),
  Cast(trim(OPEN_OS_FLAG) As String),
  Cast(trim(ORANGE_APP_SHOP) As String),
  Cast(trim(ORANGE_GAMES) As String),
  Cast(trim(ORANGE_HARD_KEY_FLAG) As String),
  Cast(trim(ORANGE_MAPS) As String),
  Cast(trim(ORANGE_MUSIC_STORE_FLAG) As String),
  Cast(trim(ORANGE_TVPLAYER) As String),
  Cast(trim(ORANGE_WIDGETS) As String),
  Cast(trim(OS) As String),
  Cast(trim(OS_VERSION) As String),
  Cast(trim(OTHER_GAMING_SOLUTION) As String),
  Cast(trim(OTHER_OS) As String),
  Cast(trim(PUSH_MAIL_FLAG) As String),
  Cast(trim(RCS) As String),
  Cast(trim(REPORTING_NAME) As String),
  Cast(trim(RING_TONES) As String),
  Cast(trim(SCREEN_SIZE_INCH) As String),
  Cast(trim(SCREEN_SIZE_PIXELS) As String),
  Cast(trim(SCREEN_TYPE) As String),
  Cast(trim(SEGMENTATION_FINANCE) As String),
  Cast(trim(SIGNATURE_FLAG) As String),
  Cast(trim(SIM_EMULATION) As String),
  Cast(trim(SOCIAL_NETWORK_CLIENT) As String),
  Cast(trim(STANDARD_MAIL_FLAG) As String),
  Cast(trim(STREAMING) As String),
  Cast(trim(SYNCHRO_PC_PLAYER_FLAG) As String),
  Cast(trim(TEK_RADIO) As String),
  Cast(trim(THREAD_MESSAGE_FLAG) As String),
  Cast(trim(TOUCH_SCREEN_FLAG) As String),
  Cast(trim(TV_HARD_KEY_FLAG) As String),
  Cast(trim(TV_OUT_LINK_SUPPORT_FLAG) As String),
  Cast(trim(UMA_FLAG) As String),
  Cast(trim(UMTS_FLAG) As String),
  Cast(trim(UMTS_BAND) As String),
  Cast(trim(USAGE_CATEGORY) As String),
  Cast(trim(USAGE_SUBCATEGORY) As String),
  Cast(trim(USAGE_CATEGORY_LEVEL) As String),
  Cast(trim(USB) As String),
  Cast(trim(USER_INT_MEMORY) As String),
  Cast(trim(VIBRATOR_FLAG) As String),
  Cast(trim(VIDEO_PLAYER) As String),
  Cast(trim(VIDEO_PLAYER_FLAG) As String),
  Cast(trim(VIDEOTELEPHONY_FLAG) As String),
  Cast(trim(VOICE_MAIL_DIRECT_ACCESS) As String),
  Cast(trim(VOLUME_CC) As String),
  Cast(trim(WAP_FLAG) As String),
  Cast(trim(WAP_PUSH) As String),
  Cast(trim(WAP_VERSION) As String),
  Cast(trim(WEIGHT_GR) As String),
  Cast(trim(WIFI_FLAG) As String),
  Cast(trim(XHTML) As String)
  FROM OIC.TT_REFERENTIEL_TERMINAUX where upper(tac) not like upper('%tac%');

  
-- 10- SORTANT

INSERT OVERWRITE TABLE OIC.TF_CDR_SORTANT PARTITION (PRT_DATE)
SELECT
  FROM_UNIXTIME(UNIX_TIMESTAMP(DATE_ID, 'yyyyMMdd')),
  CAST(MSISDN AS BIGINT),
  CAST(Trim(MSISDN_APPELANT) AS BIGINT),
  CAST(Trim(MSISDN_APPELE) AS BIGINT),
  Cast(Trim(DESTINATION) As String),
  Cast(Trim(TYPE_EVENT) As String),
  Cast(Trim(SENS_EVENT) As String),
  Cast(Trim(OPERATEUR_APPELANT) As String),
  Cast(Trim(OPERATEUR_APPELE) As String),
  Cast(Trim(TYPE_CELLULE) As String),
  CAST(cast(REGEXP_REPLACE(CI, ',', '.') as float) AS BIGINT),
  CAST(cast(REGEXP_REPLACE(DUREE_EVENT, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(MONTANT_VALORISE, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(MONTANT_BUNDLE, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(MONTANT_BONUS, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(VOL_DATA_CONSOMME, ',', '.') as float) AS BIGINT),
  CAST(cast(REGEXP_REPLACE(VOL_DATA_DOWN, ',', '.') as float) AS BIGINT),
  CAST(cast(REGEXP_REPLACE(VOL_DATA_UP, ',', '.') as float) AS BIGINT),
  DATE_ID
FROM OIC.TT_CDR_SORTANT where upper(msisdn) not like upper('%msisdn%');

-- 11- SVA

set mapreduce.map.memory.mb=4000;
set mapreduce.map.java.opts=-Xmx3000m;
set mapreduce.reduce.memory.mb=4000;
set mapreduce.reduce.java.opts=-Xmx3000m;

SET hive.exec.dynamic.partition=true;  
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE OIC.TF_SVA PARTITION (PRT_DATE)
SELECT
  FROM_UNIXTIME(UNIX_TIMESTAMP(MONTH_ID, 'yyyyMM')),
  CAST(MSISDN AS BIGINT),
  Cast(trim(TYPE_SOUSCRIPTION) As String),
  CAST(cast(REGEXP_REPLACE(NOMBRE_SOUSCRIPTION, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(MONTANT_SOUSCRIPTION, ',', '.') as float) AS int),
  MONTH_ID
FROM OIC.TT_SVA where upper(MSISDN) not like upper('%MSISDN%');

-- 12- TRANSACTION_OM

INSERT OVERWRITE TABLE OIC.TF_TRANSACTION_OM PARTITION (PRT_DATE)
SELECT
  FROM_UNIXTIME(UNIX_TIMESTAMP(MONTH_ID, 'yyyyMM')),
  CAST(MSISDN AS BIGINT),
  CAST(cast(REGEXP_REPLACE(NBRE_TRANSACTION, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(MONTANT_TRANSACTION, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(NBRE_TRANSACTION_IN, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(MONTANT_TRANSACTION_IN, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(NBRE_TRANSACTION_OUT, ',', '.') as float) AS int),
  CAST(cast(REGEXP_REPLACE(MONTANT_TRANSACTION_OUT, ',', '.') as float) AS int),
  MONTH_ID
FROM OIC.TT_TRANSACTION_OM where upper(MSISDN) not like upper('%MSISDN%');

-----------------------------------------------------------
-----------------------------------------------------------
-- 13- Service Provider per Day
INSERT OVERWRITE TABLE OIC.TF_customers_sp_day_201611
SELECT
  Cast(trim(id_cust) As String),
  Cast(trim(serv_provider) As String)
FROM OIC.TT_customers_sp_day_201611;

--
INSERT OVERWRITE TABLE OIC.TF_customers_sp_day_201612
SELECT
  Cast(trim(id_cust) As String),
  Cast(trim(serv_provider) As String)
FROM OIC.TT_customers_sp_day_201612;

--
INSERT OVERWRITE TABLE OIC.TF_customers_sp_day_201701
SELECT
  Cast(trim(id_cust) As String),
  Cast(trim(serv_provider) As String)
FROM OIC.TT_customers_sp_day_201701;

--
INSERT OVERWRITE TABLE OIC.TF_customers_sp_day_201702
SELECT
  Cast(trim(id_cust) As String),
  Cast(trim(serv_provider) As String)
FROM OIC.TT_customers_sp_day_201702;

--
INSERT OVERWRITE TABLE OIC.TF_customers_sp_day_201703
SELECT
  Cast(trim(id_cust) As String),
  Cast(trim(serv_provider) As String)
FROM OIC.TT_customers_sp_day_201703;

--
INSERT OVERWRITE TABLE OIC.TF_customers_sp_day_201704
SELECT
  Cast(trim(id_cust) As String),
  Cast(trim(serv_provider) As String)
FROM OIC.TT_customers_sp_day_201704;

-- 14- Service Provider per month
INSERT OVERWRITE TABLE OIC.TF_customers_sp_month_201611
SELECT
  Cast(trim(id_cust) As String),
  Cast(trim(serv_provider) As String)
FROM OIC.TT_customers_sp_month_201611;

--
INSERT OVERWRITE TABLE OIC.TF_customers_sp_month_201612
SELECT 
  Cast(trim(id_cust) As String),
  Cast(trim(serv_provider) As String)
FROM OIC.TT_customers_sp_month_201612;

--
INSERT OVERWRITE TABLE OIC.TF_customers_sp_month_201701
SELECT
  Cast(trim(id_cust) As String),
  Cast(trim(serv_provider) As String)
FROM OIC.TT_customers_sp_month_201701;

--
INSERT OVERWRITE TABLE OIC.TF_customers_sp_month_201702
SELECT
  Cast(trim(id_cust) As String),
  Cast(trim(serv_provider) As String)
FROM OIC.TT_customers_sp_month_201702;

--
INSERT OVERWRITE TABLE OIC.TF_customers_sp_month_201703
SELECT
  Cast(trim(id_cust) As String),
  Cast(trim(serv_provider) As String)
FROM OIC.TT_customers_sp_month_201703;

--
INSERT OVERWRITE TABLE OIC.TF_customers_sp_month_201704
SELECT
  Cast(trim(id_cust) As String),
  Cast(trim(serv_provider) As String)
FROM OIC.TT_customers_sp_month_201704;

------------------------------------------------------------------
------------------------------------------------------------------

INSERT INTO TABLE OIC.TT_customers_sp_month_201705
SELECT id_cust, serv_provider, '2017-05-08'
from OIC.TT_customers_sp_day_20170508;

INSERT INTO TABLE OIC.TT_customers_sp_month_201705
SELECT id_cust, serv_provider, '2017-05-08'
from OIC.TT_customers_sp_day_20170509;

INSERT INTO TABLE OIC.TT_customers_sp_month_201705
SELECT id_cust, serv_provider, '2017-05-10'
from OIC.TT_customers_sp_day_20170510;

INSERT INTO TABLE OIC.TT_customers_sp_month_201705
SELECT id_cust, serv_provider, '2017-05-11'
from OIC.TT_customers_sp_day_20170511;

INSERT INTO TABLE OIC.TT_customers_sp_month_201705
SELECT id_cust, serv_provider, '2017-05-12'
from OIC.TT_customers_sp_day_20170512;

INSERT INTO TABLE OIC.TT_customers_sp_month_201705
SELECT id_cust, serv_provider, '2017-05-13'
from OIC.TT_customers_sp_day_20170513;

INSERT INTO TABLE OIC.TT_customers_sp_month_201705
SELECT id_cust, serv_provider, '2017-05-14'
from OIC.TT_customers_sp_day_20170514;

INSERT INTO TABLE OIC.TT_customers_sp_month_201705
SELECT id_cust, serv_provider, '2017-05-15'
from OIC.TT_customers_sp_day_20170515;

INSERT INTO TABLE OIC.TT_customers_sp_month_201705
SELECT id_cust, serv_provider, '2017-05-16'
from OIC.TT_customers_sp_day_20170516;

INSERT INTO TABLE OIC.TT_customers_sp_month_201705
SELECT id_cust, serv_provider, '2017-05-17'
from OIC.TT_customers_sp_day_20170517;

INSERT INTO TABLE OIC.TT_customers_sp_month_201705
SELECT id_cust, serv_provider, '2017-05-18'
from OIC.TT_customers_sp_day_20170518;

INSERT INTO TABLE OIC.TT_customers_sp_month_201705
SELECT id_cust, serv_provider, '2017-05-19'
from OIC.TT_customers_sp_day_20170519;

INSERT INTO TABLE OIC.TT_customers_sp_month_201705
SELECT id_cust, serv_provider, '2017-05-20'
from OIC.TT_customers_sp_day_20170520;

INSERT INTO TABLE OIC.TT_customers_sp_month_201705
SELECT id_cust, serv_provider, '2017-05-21'
from OIC.TT_customers_sp_day_20170521;

INSERT INTO TABLE OIC.TT_customers_sp_month_201705
SELECT id_cust, serv_provider, '2017-05-22'
from OIC.TT_customers_sp_day_20170522;

INSERT INTO TABLE OIC.TT_customers_sp_month_201705
SELECT id_cust, serv_provider, '2017-05-23'
from OIC.TT_customers_sp_day_20170523;

INSERT INTO TABLE OIC.TT_customers_sp_month_201705
SELECT id_cust, serv_provider, '2017-05-24'
from OIC.TT_customers_sp_day_20170524;

INSERT INTO TABLE OIC.TT_customers_sp_month_201705
SELECT id_cust, serv_provider, '2017-05-25'
from OIC.TT_customers_sp_day_20170525;

INSERT INTO TABLE OIC.TT_customers_sp_month_201705
SELECT id_cust, serv_provider, '2017-05-26'
from OIC.TT_customers_sp_day_20170526;

------------------------------------------------------------------
------------------------------------------------------------------

INSERT INTO TABLE OIC.TF_customers_sp_month_201705
SELECT
  Cast(trim(id_cust) As String),
  Cast(trim(serv_provider) As String),
  FROM_UNIXTIME(UNIX_TIMESTAMP(Period, 'yyyy-MM-dd'))
FROM OIC.TT_customers_sp_month_201705;
