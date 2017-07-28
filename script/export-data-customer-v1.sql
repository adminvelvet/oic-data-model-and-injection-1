
mkdir -p /home/data_tech/export-data/export-hive/SVA
mkdir -p /home/data_tech/export-data/export-hive/ACTIF_DATA_ET_DATE_ACTIVATION
mkdir -p /home/data_tech/export-data/export-hive/BUNDLE
mkdir -p /home/data_tech/export-data/export-hive/BUNDLE_2
mkdir -p /home/data_tech/export-data/export-hive/CONVERSIONS_ID_APN
mkdir -p /home/data_tech/export-data/export-hive/CONVERSIONS_ID_APPLI
mkdir -p /home/data_tech/export-data/export-hive/CONVERSIONS_ID_CUST_CONTACT
mkdir -p /home/data_tech/export-data/export-hive/CONVERSIONS_ID_GGSN
mkdir -p /home/data_tech/export-data/export-hive/CONVERSIONS_ID_RAT
mkdir -p /home/data_tech/export-data/export-hive/INFO_TERMINAUX
mkdir -p /home/data_tech/export-data/export-hive/MSIM
mkdir -p /home/data_tech/export-data/export-hive/REFERENTIEL_TERMINAUX
mkdir -p /home/data_tech/export-data/export-hive/CONVERSIONS_ID_CUST_MSISDN
mkdir -p /home/data_tech/export-data/export-hive/LOCALISATION_CELLULES
mkdir -p /home/data_tech/export-data/export-hive/RECHARGE
mkdir -p /home/data_tech/export-data/export-hive/RECHARGE_2
mkdir -p /home/data_tech/export-data/export-hive/TRANSACTION_OM
mkdir -p /home/data_tech/export-data/export-hive/customers_sp_day_201611
mkdir -p /home/data_tech/export-data/export-hive/customers_sp_day_201612
mkdir -p /home/data_tech/export-data/export-hive/customers_sp_day_201701
mkdir -p /home/data_tech/export-data/export-hive/customers_sp_day_201702
mkdir -p /home/data_tech/export-data/export-hive/customers_sp_day_201703
mkdir -p /home/data_tech/export-data/export-hive/customers_sp_day_201704
mkdir -p /home/data_tech/export-data/export-hive/customers_sp_month_201611
mkdir -p /home/data_tech/export-data/export-hive/customers_sp_month_201612
mkdir -p /home/data_tech/export-data/export-hive/customers_sp_month_201701
mkdir -p /home/data_tech/export-data/export-hive/customers_sp_month_201702
mkdir -p /home/data_tech/export-data/export-hive/customers_sp_month_201703
mkdir -p /home/data_tech/export-data/export-hive/customers_sp_month_201704
mkdir -p /home/data_tech/export-data/export-hive/customers_sp_month_201705
mkdir -p /home/data_tech/export-data/export-hive/DONNEES_HISTO_MENSUELLES
hadoop fs -mkdir -p /user/data_tech/export-hive/CDR_SORTANT
hadoop fs -mkdir -p /user/data_tech/export-hive/CDR_ENTRANT
hadoop fs -mkdir -p /user/data_tech/export-hive/CDR_TRAFFIC_CUSTOMER
hadoop fs -mkdir -p /user/data_tech/export-hive/CDR_TRAFFIC_CUSTOMER_CELL


-- Export de la table TT_SVA	--ok
INSERT OVERWRITE LOCAL DIRECTORY '/home/data_tech/export-data/export-hive/SVA'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
select MONTH_ID, (CAST(MSISDN AS BIGINT) * 31 + 19527368) as MSISDN, TYPE_SOUSCRIPTION, NOMBRE_SOUSCRIPTION, MONTANT_SOUSCRIPTION
from oic.tt_sva where msisdn <> upper('msisdn');

-- Export de la table TT_ACTIF_DATA_ET_DATE_ACTIVATION		--ok
INSERT OVERWRITE LOCAL DIRECTORY '/home/data_tech/export-data/export-hive/ACTIF_DATA_ET_DATE_ACTIVATION'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
select (CAST(MSISDN AS BIGINT) * 31 + 19527368) as MSISDN, DATE_ACTIVATION
from oic.tt_ACTIF_DATA_ET_DATE_ACTIVATION where msisdn <> upper('msisdn');

-- Export de la table OIC.TT_BUNDLE		--ok
INSERT OVERWRITE LOCAL DIRECTORY '/home/data_tech/export-data/export-hive/BUNDLE'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
select DATE_SOUSCRIPTION, (CAST(MSISDN AS BIGINT) * 31 + 19527368) as MSISDN, TYPE_SOUSCRIPTION, NOMBRE_SOUSCRIPTION, MONTANT_SOUSCRIPTION 
from OIC.TT_BUNDLE where msisdn <> upper('msisdn');

INSERT OVERWRITE LOCAL DIRECTORY '/home/data_tech/export-data/export-hive/BUNDLE_2'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
select DATE_SOUSCRIPTION, (CAST(MSISDN AS BIGINT) * 31 + 19527368) as MSISDN, TYPE_SOUSCRIPTION, NOMBRE_SOUSCRIPTION, MONTANT_SOUSCRIPTION 
from OIC.TT_BUNDLE_2 where msisdn <> upper('msisdn');

-- Export de la table OIC.TT_CONVERSIONS_ID_APN		--ok
INSERT OVERWRITE LOCAL DIRECTORY '/home/data_tech/export-data/export-hive/CONVERSIONS_ID_APN'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
select *
from OIC.TT_CONVERSIONS_ID_APN;

-- Export de la table OIC.TT_CONVERSIONS_ID_APPLI	--ok
INSERT OVERWRITE LOCAL DIRECTORY '/home/data_tech/export-data/export-hive/CONVERSIONS_ID_APPLI'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
select *
from OIC.TT_CONVERSIONS_ID_APPLI;

-- Export de la table OIC.TT_CONVERSIONS_ID_CUST_CONTACT	--ok
INSERT OVERWRITE LOCAL DIRECTORY '/home/data_tech/export-data/export-hive/CONVERSIONS_ID_CUST_CONTACT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
select *
from OIC.TT_CONVERSIONS_ID_CUST_CONTACT;

-- Export de la table OIC.TT_CONVERSIONS_ID_GGSN	--ok
INSERT OVERWRITE LOCAL DIRECTORY '/home/data_tech/export-data/export-hive/CONVERSIONS_ID_GGSN'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
select *
from OIC.TT_CONVERSIONS_ID_GGSN;

-- Export de la table OIC.TT_CONVERSIONS_ID_RAT		--ok
INSERT OVERWRITE LOCAL DIRECTORY '/home/data_tech/export-data/export-hive/CONVERSIONS_ID_RAT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
select *
from OIC.TT_CONVERSIONS_ID_RAT;

-- Export de la table OIC.TT_INFO_TERMINAUX 	--ok
INSERT OVERWRITE LOCAL DIRECTORY '/home/data_tech/export-data/export-hive/INFO_TERMINAUX'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
select DATE_ID, (CAST(MSISDN AS BIGINT) * 31 + 19527368) as MSISDN, CODE_TAC, MODELE, MARQUE, DEVICE_TYPE
from OIC.TT_INFO_TERMINAUX where msisdn <> upper('msisdn');

-- Export de la table OIC.TT_MSIM	--ok
INSERT OVERWRITE LOCAL DIRECTORY '/home/data_tech/export-data/export-hive/MSIM'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
select (CAST(MSISDN AS BIGINT) * 31 + 19527368) as MSISDN, STATUT_ORANGE, STATUT_MTN, STATUT_MOOV
from OIC.TT_MSIM where msisdn not like upper('%msisdn%');

-- Export de la table OIC.TT_REFERENTIEL_TERMINAUX	--ok
INSERT OVERWRITE LOCAL DIRECTORY '/home/data_tech/export-data/export-hive/REFERENTIEL_TERMINAUX'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
select *
from OIC.TT_REFERENTIEL_TERMINAUX where TAC not like upper('%tac%');

-- Export de la table OIC.TT_CONVERSIONS_ID_CUST_MSISDN	--ok
INSERT OVERWRITE LOCAL DIRECTORY '/home/data_tech/export-data/export-hive/CONVERSIONS_ID_CUST_MSISDN'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
select Id_customer, (CAST(MSISDN AS BIGINT) * 31 + 19527368) as MSISDN
from OIC.TT_CONVERSIONS_ID_CUST_MSISDN ;

-- Export de la table OIC.TT_LOCALISATION_CELLULES	--ok
INSERT OVERWRITE LOCAL DIRECTORY '/home/data_tech/export-data/export-hive/LOCALISATION_CELLULES'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
select *
from OIC.TT_LOCALISATION_CELLULES ;

-- Export de la table OIC.TT_RECHARGE	--ok
INSERT OVERWRITE LOCAL DIRECTORY '/home/data_tech/export-data/export-hive/RECHARGE'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
select DATE_RECHARGEMENT, (CAST(MSISDN AS BIGINT) * 31 + 19527368) as MSISDN, TYPE_RECHARGEMENT, MONTANT_RECHARGEMNT, NOMBRE_RECHARGEMNT
from OIC.TT_RECHARGE where upper(msisdn) <> upper('msisdn');

INSERT OVERWRITE LOCAL DIRECTORY '/home/data_tech/export-data/export-hive/RECHARGE_2'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
select DATE_RECHARGEMENT, (CAST(MSISDN AS BIGINT) * 31 + 19527368) as MSISDN, TYPE_RECHARGEMENT, MONTANT_RECHARGEMNT, NOMBRE_RECHARGEMNT
from OIC.TT_RECHARGE_2 where upper(msisdn) <> upper('msisdn');

-- Export de la table OIC.TT_TRANSACTION_OM	--ok
INSERT OVERWRITE LOCAL DIRECTORY '/home/data_tech/export-data/export-hive/TRANSACTION_OM'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
select MONTH_ID, (CAST(MSISDN AS BIGINT) * 31 + 19527368) as MSISDN, NBRE_TRANSACTION, MONTANT_TRANSACTION, NBRE_TRANSACTION_IN, MONTANT_TRANSACTION_IN, NBRE_TRANSACTION_OUT, MONTANT_TRANSACTION_OUT
from OIC.TT_TRANSACTION_OM where upper(msisdn) <> upper('msisdn');
------------------------------------------------------------------------------
------------------------------------------------------------------------------

-- Export de la table OIC.TT_customers_sp_day_201611	--ok
INSERT OVERWRITE LOCAL DIRECTORY '/home/data_tech/export-data/export-hive/customers_sp_day_201611'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
select * from OIC.TT_customers_sp_day_201611 ;

-- Export de la table OIC.TT_customers_sp_day_201612	--ok
INSERT OVERWRITE LOCAL DIRECTORY '/home/data_tech/export-data/export-hive/customers_sp_day_201612'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
select * from OIC.TT_customers_sp_day_201612;

-- Export de la table OIC.TT_customers_sp_day_201701	--ok
INSERT OVERWRITE LOCAL DIRECTORY '/home/data_tech/export-data/export-hive/customers_sp_day_201701'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
select * from OIC.TT_customers_sp_day_201701;

-- Export de la table OIC.TT_customers_sp_day_201702	--ok
INSERT OVERWRITE LOCAL DIRECTORY '/home/data_tech/export-data/export-hive/customers_sp_day_201702'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
select * from OIC.TT_customers_sp_day_201702;

-- Export de la table OIC.TT_customers_sp_day_201703	--ok
INSERT OVERWRITE LOCAL DIRECTORY '/home/data_tech/export-data/export-hive/customers_sp_day_201703'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
select * from OIC.TT_customers_sp_day_201703;

-- Export de la table OIC.TT_customers_sp_day_201704	--ok
INSERT OVERWRITE LOCAL DIRECTORY '/home/data_tech/export-data/export-hive/customers_sp_day_201704'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
select * from OIC.TT_customers_sp_day_201704;

-- Export de la table OIC.TT_customers_sp_month_201611	--ok
INSERT OVERWRITE LOCAL DIRECTORY '/home/data_tech/export-data/export-hive/customers_sp_month_201611'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
select * from OIC.TT_customers_sp_month_201611;

-- Export de la table OIC.TT_customers_sp_month_201612	--ok
INSERT OVERWRITE LOCAL DIRECTORY '/home/data_tech/export-data/export-hive/customers_sp_month_201612'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
select * from OIC.TT_customers_sp_month_201612;

-- Export de la table OIC.TT_customers_sp_month_201701	--ok
INSERT OVERWRITE LOCAL DIRECTORY '/home/data_tech/export-data/export-hive/customers_sp_month_201701'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
select * from OIC.TT_customers_sp_month_201701;

-- Export de la table OIC.TT_customers_sp_month_201702	--ok
INSERT OVERWRITE LOCAL DIRECTORY '/home/data_tech/export-data/export-hive/customers_sp_month_201702'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
select * from OIC.TT_customers_sp_month_201702;

-- Export de la table OIC.TT_customers_sp_month_201703	--ok
INSERT OVERWRITE LOCAL DIRECTORY '/home/data_tech/export-data/export-hive/customers_sp_month_201703'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
select * from OIC.TT_customers_sp_month_201703;

-- Export de la table OIC.TT_customers_sp_month_201704	--ok
INSERT OVERWRITE LOCAL DIRECTORY '/home/data_tech/export-data/export-hive/customers_sp_month_201704'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
select * from OIC.TT_customers_sp_month_201704;

-- Export de la table OIC.TT_customers_sp_month_201705	--ok
INSERT OVERWRITE LOCAL DIRECTORY '/home/data_tech/export-data/export-hive/customers_sp_month_201705'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
select * from OIC.TT_customers_sp_month_201705;

-- Export de la table OIC.TT_DONNEES_HISTO_MENSUELLES;	--ok
INSERT OVERWRITE LOCAL DIRECTORY '/home/data_tech/export-data/export-hive/DONNEES_HISTO_MENSUELLES'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
select MONTH_ID, (CAST(MSISDN AS BIGINT) * 31 + 19527368) as MSISDN, GEOLOCALITE, COMMUNE, DEPARTEMENT, REGION, REGION_CAT, DISTRIBUTEUR,
DATE_ACTIVATION, STATUT_OM, STATUT_MSIM, STATUT_MOOV, STATUT_MTN, SEGMENT_VALEUR, PLAN_TARIFAIRE, MONTANT_RECHARGE, REV_TOT_CONSOMME,
REV_SORTANT, REV_ENTRANT, REV_VOIX, REV_SMS, REV_DATA, REV_PASS, REV_SVA, VOL_TOT_VOIX_SORTANT, NB_TOT_VOIX_SORTANT, NB_TOT_SMS_SORTANT,
NB_VOIX_OFFNET_SORTANT, NB_VOIX_ONNET_SORTANT, NB_VOIX_INTER, NB_VOIX_ROAM_SORTANT, NB_VOIX_ROAM_ENTRANT, NB_SMS_OFFNET_SORTANT,
NB_SMS_INTER_SORTANT, NB_SMS_ONET_SORTANT, NB_SMS_ROAM_SORTANT, VOL_TOT_VOIX_OFFNET_SORTANT, VOL_TOT_VOIX_ONNET_SORTANT,
VOL_TOT_VOIX_INTER_SORTANT, VOL_VOIX_ROAM_SORTANT, VOL_VOIX_ROAM_ENTRANT, VOL_DATA_ROAM, VOL_CONSOMME
from OIC.TT_DONNEES_HISTO_MENSUELLES where msisdn not like upper('%msisdn%');

-- Export de la table OIC.TT_CDR_SORTANT	--ok
INSERT OVERWRITE DIRECTORY '/user/data_tech/export-hive/CDR_SORTANT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
select DATE_ID, (CAST(MSISDN AS BIGINT) * 31 + 19527368) as MSISDN, (CAST(MSISDN_APPELANT AS BIGINT) * 31 + 19527368) as MSISDN_APPELANT,
(CAST(MSISDN_APPELE AS BIGINT) * 31 + 19527368) as MSISDN_APPELE, DESTINATION, TYPE_EVENT, SENS_EVENT, OPERATEUR_APPELANT, OPERATEUR_APPELE,
TYPE_CELLULE, CI, DUREE_EVENT, MONTANT_VALORISE, MONTANT_BUNDLE, MONTANT_BONUS, VOL_DATA_CONSOMME, VOL_DATA_DOWN, VOL_DATA_UP
from OIC.TT_CDR_SORTANT where msisdn not like upper('%msisdn%');

-- Export de la table OIC.TT_CDR_ENTRANT	--
INSERT OVERWRITE DIRECTORY '/user/data_tech/export-hive/CDR_ENTRANT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
select DATE_ID, (CAST(MSISDN AS BIGINT) * 31 + 19527368) as MSISDN, (CAST(MSISDN_APPELANT AS BIGINT) * 31 + 19527368) as MSISDN_APPELANT,
(CAST(MSISDN_APPELE AS BIGINT) * 31 + 19527368) as MSISDN_APPELE, DESTINATION, TYPE_EVENT, SENS_EVENT, OPERATEUR_APPELE, OPERATEUR_APPELANT,
TYPE_CELLULE, CI, DUREE_EVENT, MONTANT_VALORISE, MONTANT_BUNDLE, MONTANT_BONUS, VOL_DATA_CONSOMME, VOL_DATA_DOWN, VOL_DATA_UP
from OIC.TT_CDR_ENTRANT where msisdn not like upper('%msisdn%');

-- Export de la table OIC.TT_CDR_TRAFFIC_CUSTOMER	--
INSERT OVERWRITE DIRECTORY '/user/data_tech/export-hive/CDR_TRAFFIC_CUSTOMER'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
select day, idCust, idAppli, idAPN, idTAC, idRAT, idGGSN, Roaming, nBytesDn, nBytesUp, nBytesT, Terminal_type, Terminal_brand,
Terminal_model, Subscriber_type, Service_type, idContract
from OIC.TT_CDR_TRAFFIC_CUSTOMER where idCust not like upper('%idCust%');

-- Export de la table OIC.TT_CDR_TRAFFIC_CUSTOMER_CELL	--
INSERT OVERWRITE DIRECTORY '/user/data_tech/export-hive/CDR_TRAFFIC_CUSTOMER_CELL'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
select day, idCust, idGGSN, idRAT, idTAC, idCell, roaming, nBytesDn, nBytesUp
from OIC.TT_CDR_TRAFFIC_CUSTOMER_CELL where idCust not like upper('%idCust%');

