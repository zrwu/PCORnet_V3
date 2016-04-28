CREATE OR REPLACE PROCEDURE                "HARVEST_PRC"
AS
  /* OMOP to PCORnet HARVEST
  Change Log
  Who  WHEN  What
  ZWu     02/16/2016   Initial Version
  */
  --TranCount     INT := 0;
  rowsInserted  VARCHAR(15);
  err_msg       VARCHAR2(2000) :='';
  err_code      VARCHAR2(200)  :='';
  NOENCOUNTERID VARCHAR(20)    := 'no encounter found';
  EHRCONDTYPE   INT            := 38000245;
  CONDCONCEPTOT INT            := 44814649;
BEGIN
  -- truncate statement
  -- EXECUTE immediate 'truncate table pcornet_HARVEST';
  INSERT
  INTO etl_log
    (
      log_id,
      log_entry,
      log_time
    )
  SELECT seq_log_id.NextVal,'HARVEST TRUNCATED' AS what, sysdate FROM dual;
  INSERT INTO etl_log
    (log_id,log_entry, log_time
    )
  SELECT seq_log_id.NextVal,'HARVEST START' AS what, sysdate FROM dual;
  -- update statement
  UPDATE OMOP.PCORNET_HARVEST
  SET ADMIT_DATE_MGMT='01',
    BIRTH_DATE_MGMT  ='04',
    CDM_VERSION      =3.0,
    --DATAMARTID='NA',
    DATAMART_CLAIMS    ='02',
    DATAMART_EHR       ='02',
    DATAMART_NAME      ='UC Davis',
    DATAMART_PLATFORM  ='02',
    DISCHARGE_DATE_MGMT='01',
    DISPENSE_DATE_MGMT ='01',
    ENR_END_DATE_MGMT  ='01',
    ENR_START_DATE_MGMT='01',
    LAB_ORDER_DATE_MGMT='NI',
    MEASURE_DATE_MGMT  ='01',
    NETWORK_NAME              ='pSCANNER',
    ONSET_DATE_MGMT           ='NI',
    PRO_DATE_MGMT             ='NI',
    PX_DATE_MGMT              ='01',
    REFRESH_CONDITION_DATE    =sysdate,
    REFRESH_DEATH_CAUSE_DATE  =sysdate,
    REFRESH_DEATH_DATE        =sysdate,
    REFRESH_DEMOGRAPHIC_DATE  =sysdate,
    REFRESH_DIAGNOSIS_DATE    =sysdate,
    REFRESH_DISPENSING_DATE   =sysdate,
    REFRESH_ENCOUNTER_DATE    =sysdate,
    REFRESH_ENROLLMENT_DATE   =sysdate,
    REFRESH_LAB_RESULT_CM_DATE=sysdate,
    REFRESH_PCORNET_TRIAL_DATE=sysdate,
    REFRESH_PRESCRIBING_DATE  =sysdate,
    REFRESH_PROCEDURES_DATE   =sysdate,
    REFRESH_PRO_CM_DATE       =NULL,
    REFRESH_VITAL_DATE        =sysdate,
    REPORT_DATE_MGMT          ='01',
    RESOLVE_DATE_MGMT         ='02',
    RESULT_DATE_MGMT          ='01',
    RX_END_DATE_MGMT          ='NI',
    RX_ORDER_DATE_MGMT        ='01',
    RX_START_DATE_MGMT        ='NI',
    SPECIMEN_DATE_MGMT        ='NI'
  WHERE NETWORKID             ='C3'
  AND datamartid              ='C3UCD';
  COMMIT;
  rowsInserted := f_formatNumber( SQL%ROWCOUNT );
  INSERT INTO etl_log
    (log_id,log_entry, log_time
    )
  SELECT seq_log_id.NextVal,
    'HARVEST: '
    || rowsInserted
    || ' inserted' ,
    sysdate
  FROM dual;
  COMMIT;
EXCEPTION
WHEN OTHERS THEN
  err_code := SQLCODE;
  err_msg  := DBMS_UTILITY.FORMAT_ERROR_BACKTRACE || ' ' || SUBSTR(SQLERRM, 1, 200);
  INSERT INTO etl_log
    (log_id, log_entry, log_time
    )
  SELECT seq_log_id.NextVal,
    'HARVEST failed => '
    || err_code
    ||' '
    || err_msg AS what,
    sysdate
  FROM dual;
  COMMIT;
  raise_application_error(-20001,'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM);
END HARVEST_PRC ;
/
