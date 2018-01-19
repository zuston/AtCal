CREATE DATABASE IF NOT EXISTS ane DEFAULT CHARSET utf8 COLLATE utf8_general_ci;

GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' IDENTIFIED BY 'shacha' WITH GRANT OPTION;


CREATE TABLE `activeTrace` (
  `TRACE_ID`        varchar(64),
  `EWB_NO` 			varchar(64),
  `STEP`		varchar(64),
  `SITE_ID`			varchar(64),
  `SITE_CODE`		varchar(64),
  `SITE_NAME`		varchar(64),
  `SITE_TYPE`		varchar(64),
  `CITY_NAME`		varchar(64),
  `CLERK_ID`		varchar(64),
  `CLERK_NAME`		varchar(64),
  `SCAN_TIME`		varchar(64),
  `DEST_SITE_ID`	varchar(64),
  `DEST_SITE_NAME` 	varchar(64),
  `DEST_SITE_CODE` 	varchar(64),
  `CONTACTER`		varchar(64),
  `CONTACT_PHONE`	varchar(64),
  `DESCPT` 			varchar(64),
  `STATUS` 			varchar(64),
  `CREATE_TIME`		varchar(64),
  `ISPUSH` 			varchar(64),
  `DISTRICT`		varchar(64),
  `WEIGHT`			varchar(64),
  `EC_ID`			varchar(64),
  `ELECPUSH`		varchar(64),
  `NEXTCITY`		varchar(64),
  `DATA_SOURCE`		varchar(64),
  `TASK_NO`		varchar(64),
  `PREDICT_TIME`varchar(64),
) ENGINE=MyISAM  DEFAULT CHARSET=utf8;


-- ./sqoop-export   --connect "jdbc:mysql://10.10.0.91:3306/ane?useUnicode=true&characterEncoding=utf-8"  --username root  --password shacha --table activeTrace  --input-fields-terminated-by '#' --export-dir /aneOutput/activeTrace/mysql_1