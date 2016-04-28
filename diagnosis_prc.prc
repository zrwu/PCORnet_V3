CREATE OR REPLACE PROCEDURE                               "DIAGNOSIS_PRC"
AS
  /* OMOP to PCORnet diagnosis
  PreCondition:
  1) PCORnet specific Source to Concept Map entries in vocabulary.source_to_concept_map
  2) uses f_formatNumber()
  3) Synonyms: omop.person; voc.source_to_concept_map
  PostConditions:
  1) diagnosis table truncated and then repopulated
  2) entry in etl_log table
  Change Log
  Who  WHEN  What
  dtk   26Feb2015    Make the procedure table driven
  DTK   27feb15      Change order encounter date then condition date
  DTk   28Feb15  Add encounterid default
  dtk  02Apr2015  Port to Oracle
  dtk 03Apr2015   remove schema name from omop tables
  ZWu  Feb.8 2016 converted to Oracle PLSQL from TSQL
  */
  PROC_NAME           VARCHAR2(30) := 'PCORnet Diagnosis V3';
  rowsInserted        VARCHAR(15);
  EHR_problem_list    INT        := 38000245;
  conceptNotFound     INT        := 0;
  conceptOther        INT        := 44814649 ;
  PCOR_UNKNOWN        VARCHAR(2) := 'UN';
  PCOR_OTHER          VARCHAR(2) := 'OT';
  PCOR_SNOMED         VARCHAR(2) := 'SM';
  PCOR_AMBULATORY     VARCHAR(2) := 'AV';
  PCOR_OTH_AMBULATORY VARCHAR(2) := 'OA';
  PCOR_EMERGENCY      VARCHAR(2) := 'ED';
  PCOR_INPATIENT      VARCHAR(2) := 'IP';
  PCOR_INSTITUTIONAL  VARCHAR(2) := 'IS';
  PCOR_FINAL          VARCHAR(2) := 'FI';
  PCOR_NOT_CLASSIFIED VARCHAR(2) := 'X';
  PCOR_PRINCIPAL      VARCHAR(2) := 'P';
  PCOR_SECONDARY      VARCHAR(2) := 'S';
  CONCEPT_PRIMARY_1   INT        := 38000183;
  /* Inpatient detail - primary */
  CONCEPT_PRIMARY_2 INT := 38000215;
  /* Outpatient detail - 1st position */
  CONCEPT_PRIMARY_3 INT := 44786627;
  /*  Primary Condition */
  CONCEPT_PRIMARY_4 INT := 44786630;
  /* Primary Procedure */
  CONCEPT_SECONDARY INT            :=44786629;
  NOENCOUNTERID     VARCHAR(20)    := '0000';
  DEFAULT_ENC_TYPE  VARCHAR(2)     := 'NI';
  err_msg           VARCHAR2(2000) :='';
  err_code          VARCHAR2(200)  :='';
BEGIN
  -- truncate statement
  EXECUTE immediate 'truncate table pcornet_diagnosis';
  INSERT INTO etl_log
    (log_entry, log_time
    )
  SELECT PROC_NAME || ' TRUNCATED' AS what, sysDate FROM DUAL;
  INSERT INTO etl_log
    (log_entry, log_time
    )
  SELECT PROC_NAME || ' START' AS what, sysDate FROM DUAL ;
  -- INSERT statement
  --INSERT INTO pcornet_diagnosis( patId, encounterid, enc_type, admit_date, providerid
  -- , dx, dx_type, dx_source, pdx, raw_dx, raw_dx_type, raw_dx_source, raw_pdx )
  INSERT INTO pcornet_diagnosis
    (
      DIAGNOSISID,
      PATID,
      ENCOUNTERID,
      ENC_TYPE,
      ADMIT_DATE,
      PROVIDERID,
      DX,
      DX_TYPE,
      DX_SOURCE,
      PDX,
      RAW_DX,
      RAW_DX_TYPE,
      RAW_DX_SOURCE,
      RAW_PDX
    )
  SELECT seq_diag.NextVal,
    a.*
 
      from
    (SELECT DISTINCT CAST( condition.person_id AS VARCHAR(20) )                       AS patid ,
      COALESCE( CAST( condition.visit_occurrence_id AS VARCHAR(20) ), NOENCOUNTERID ) AS encounterid ,
      COALESCE( encounter.enc_type, DEFAULT_ENC_TYPE )                                AS enc_type ,
      COALESCE(encounter.admit_date,  condition.condition_start_date )admit_date ,
      --COALESCE(to_date(encounter.admit_date,'YYYY-MM-DD'),  condition.condition_start_date )admit_date ,
      CAST( encounter.providerid AS VARCHAR(12) ) providerid ,
      CASE
        WHEN condition.condition_concept_id IN( conceptNotFound, conceptOther )
        THEN condition.condition_source_value
        ELSE condition_concept.concept_code
      END AS dx ,
      CASE
        WHEN condition.condition_concept_id IN( conceptNotFound, conceptOther )
        THEN PCOR_UNKNOWN
        ELSE PCOR_SNOMED
      END AS dx_type ,
      CASE encounter.enc_type
        WHEN PCOR_AMBULATORY
        THEN PCOR_FINAL
        ELSE PCOR_UNKNOWN
      END AS dx_source ,
      CASE
        WHEN encounter.enc_type IN (PCOR_EMERGENCY, PCOR_AMBULATORY, PCOR_OTH_AMBULATORY )
        THEN PCOR_NOT_CLASSIFIED
        WHEN encounter.enc_type IN( PCOR_INPATIENT , PCOR_INSTITUTIONAL )
        THEN
          CASE
            WHEN condition.condition_type_concept_id IN (CONCEPT_PRIMARY_1, CONCEPT_PRIMARY_2, CONCEPT_PRIMARY_3, CONCEPT_PRIMARY_4 )
            THEN PCOR_PRINCIPAL
            WHEN condition.condition_type_concept_id IN( CONCEPT_SECONDARY )
            THEN PCOR_SECONDARY
            ELSE PCOR_OTHER
          END
        ELSE PCOR_OTHER
      END AS pdx ,
      condition.condition_source_value raw_dx ,
      NULL AS raw_dx_type ,
      NULL AS raw_dx_source ,
      NULL AS raw_pdx
    FROM -- Exclude diagnoses from EHR Problem List
      (SELECT /*+ parallel(condition_occurrence,8)*/ *
      FROM condition_occurrence
      WHERE condition_type_concept_id <> EHR_problem_list
      ) condition
    LEFT OUTER JOIN pcornet_encounter encounter
    ON condition.visit_occurrence_id=encounter.encounterid
    LEFT OUTER JOIN concept condition_concept
    ON condition_concept.concept_id = condition_concept_id
    ) a ;
  rowsInserted := f_formatNumber( SQL%ROWCOUNT );
  INSERT INTO etl_log
    (log_id, log_entry, log_time
    )
  SELECT seq_log_id.NextVal ,
    PROC_NAME
    || ' inserted: '
    || rowsInserted,
    sysdate
  FROM dual;
  --------
  COMMIT ;
EXCEPTION
WHEN OTHERS THEN
  err_code := SQLCODE;
  err_msg  := DBMS_UTILITY.FORMAT_ERROR_BACKTRACE || ' ' || SUBSTR(SQLERRM, 1, 200);
  ROLLBACK;
  INSERT INTO etl_log
    (log_entry, log_time
    )
  SELECT PROC_NAME || ' failed => ' || err_msg, sysdate FROM dual;
  COMMIT;
  raise_application_error(-20001,'An error was encountered '||err_msg );
END diagnosis_prc;
/
