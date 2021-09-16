# IngesitionOraKinesis
Select CURRENT_SCN from v$database; 

desc V$LOGMNR_CONTENTS
desc v$logfile

create table sys.MYSCN(SCNWINSTART NUMBER, SCNWINEND NUMBER, SCNWINCURRENT NUMBER)
ALTER SESSION SET NLS_DATE_FORMAT = 'DD-MON-YYYY HH24:MI:SS';

SQL> select group#,sequence#,status,first_change#,first_time from v$log;

    GROUP#  SEQUENCE# STATUS	       FIRST_CHANGE# FIRST_TIME
---------- ---------- ---------------- ------------- --------------------
	 1	    7 INACTIVE		     2160901 05-AUG-2021 11:00:16
	 2	    8 INACTIVE		     2177958 05-AUG-2021 14:47:45
	 3	    9 CURRENT		     2276103 06-AUG-2021 09:00:33

SQL> select name,sequence#,status,first_change#,first_time from v$archived_log;

NAME
--------------------------------------------------------------------------------
 SEQUENCE# S FIRST_CHANGE# FIRST_TIME
---------- - ------------- --------------------
/opt/oracle/oradata/ORCLCDB/archivelog/2021_08_05/o1_mf_1_7_jjqyjk9z_.arc
	 7 A	   2160901 05-AUG-2021 11:00:16

/opt/oracle/oradata/ORCLCDB/archivelog/2021_08_06/o1_mf_1_8_jjsykkty_.arc
	 8 A	   2177958 05-AUG-2021 14:47:45

SELECT NAME FROM V$ARCHIVED_LOG
   WHERE FIRST_TIME = (SELECT MAX(FIRST_TIME) FROM V$ARCHIVED_LOG);

EXECUTE DBMS_LOGMNR.ADD_LOGFILE( LOGFILENAME => '/opt/oracle/oradata/ORCLCDB/redo01.log',    OPTIONS => DBMS_LOGMNR.NEW);
EXECUTE DBMS_LOGMNR.ADD_LOGFILE( LOGFILENAME => '/opt/oracle/oradata/ORCLCDB/redo02.log',    OPTIONS => DBMS_LOGMNR.ADDFILE);
EXECUTE DBMS_LOGMNR.ADD_LOGFILE( LOGFILENAME => '/opt/oracle/oradata/ORCLCDB/redo03.log',    OPTIONS => DBMS_LOGMNR.ADDFILE);

EXECUTE DBMS_LOGMNR.ADD_LOGFILE( LOGFILENAME => '/opt/oracle/oradata/ORCLCDB/archivelog/2021_08_06/o1_mf_1_8_jjsykkty_.arc',    OPTIONS => DBMS_LOGMNR.ADDFILE);


exec dbms_logmnr.start_logmnr(startscn=>2356186,endscn=>2357890 , options=>DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG+DBMS_LOGMNR.COMMITTED_DATA_ONLY)
select min(scn),max(scn) from v$logmnr_contents;


select con_id from V$LOGMNR_CONTENTS;


EXECUTE DBMS_LOGMNR.END_LOGMNR();	


Insert into OT.CUSTOMERS (CUSTOMER_ID,NAME,ADDRESS,CREDIT_LIMIT,WEBSITE) values (12600,'PParker-Hannifin','Canale Grande 2, Roma, ',700,'http://www.parker.com');
commit;

