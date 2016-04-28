CREATE OR REPLACE PROCEDURE                     "VITAL_PRC"
AS
  /* OMOP to PCORnet vital
  PreCondition:
  1) PCORnet specific Source to Concept Map entries in vocabulary.source_to_concept_map
  2) uses f_formatNumber()
  3) Synonyms: person; source_to_concept_map
  PostConditions:
  1) vital table truncated and then repopulated
  2) entry in etl_log table
  Change Log
  Who  WHEN  What
  dtk    26Feb2015   Make the procedure table driven
  dtk    01Mar2015   Correct obs_type_ehr concept id
  dtk    02Apr2015   Port to Oracle
  dtk 03Apr2015   remove schema name from omop tables
  yb  29May2015 Removed raw_vital_source and added tobacco and tobacco_type
  as part of PCORnet V2 changes
  Zwu Feb. 3 2016 Coverted from TSQL as part of CDM V3
  */
  PROC_NAME               VARCHAR2(30) := 'PCORnet Vital V3';
  rowsInserted            VARCHAR(15);
  OBS_TYPE_EHR            INT := 38000280;
  OBS_TYPE_PATIENT_REPORT INT := 44814721;
  HEIGHT                  INT := 3036277;
  WEIGHT                  INT := 3025315;
  BMI                     INT := 3038553;
  DIASTOLIC_SITTING       INT := 3034703;
  DIASTOLIC_STANDING      INT := 3019962;
  DIASTOLIC_SUPINE        INT := 3013940;
  DIASTOLIC               INT := 3012888;
  PIPE                    INT := 4041509;
  SYSTOLIC_SITTING        INT := 3018586;
  SYSTOLIC_STANDING       INT := 3035856;
  SYSTOLIC_SUPINE         INT := 3009395;
  SYSTOLIC                INT := 3004249;
  --
  TOBACCO INT := 4041306;
  CIGARET INT := 4041508;
  VPIPE   INT := 4041509;
  CIGAR   INT := 4047454;
  SNUFF   INT := 4036084;
  CHEW    INT := 4038735;
  --
  vYES                VARCHAR(3) := 'YES';
  vNO                 VARCHAR(2) := 'NO';
  OUNCE               INT        := 9373;
  PCOR_NO_INFORMATION VARCHAR(2) := 'NI';
  PCOR_BP_SITTING     VARCHAR(2) := '01';
  PCOR_BP_STANDING    VARCHAR(2) := '02';
  PCOR_BP_SUPINE      VARCHAR(2) := '03';
  PCOR_HEALTHCARE     VARCHAR(2) := 'HC';
  PCOR_PATIENT_REPORT VARCHAR(2) := 'PR';
  DEFAULT_TIME        VARCHAR(5) := '00:00';
  --
  VNEVER     VARCHAR2(5)    := 'NEVER';
  vNOT_ASKED VARCHAR2(9)    := 'NOT ASKED';
  vQUIT      VARCHAR2(4)    := 'QUIT';
  vPASSIVE   VARCHAR2(7)    := 'PASSIVE';
  err_msg    VARCHAR2(2000) :='';
  err_code   VARCHAR2(200)  :='';
BEGIN
  -- truncate statement
  EXECUTE immediate 'truncate table pcornet_vital';
  INSERT INTO etl_log
    (log_entry, log_time
    )
  SELECT PROC_NAME || ' TRUNCATED' AS what, sysDate FROM DUAL;
  INSERT INTO etl_log
    (log_entry, log_time
    )
  SELECT PROC_NAME || ' START' AS what, sysDate FROM DUAL ;
  -- INSERT statement
  /*INSERT INTO pcornet_vital( patId, encounterid, measure_date, measure_time, vital_source
  , HT, WT, diastolic, systolic, original_bmi, bp_position
  , tobacco, tobacco_type
  , raw_diastolic, raw_systolic, raw_bp_position
  , raw_tobacco, raw_tobacco_type)  */
  INSERT
  INTO pcornet_vital
    (
      VITALID,
      PATID,
      ENCOUNTERID,
      MEASURE_DATE,
      MEASURE_TIME,
      VITAL_SOURCE,
      HT,
      WT,
      DIASTOLIC,
      SYSTOLIC,
      ORIGINAL_BMI,
      BP_POSITION,
      TOBACCO,
      TOBACCO_TYPE,
      RAW_DIASTOLIC,
      RAW_SYSTOLIC,
      RAW_BP_POSITION,
      RAW_TOBACCO,
      RAW_TOBACCO_TYPE,
      SMOKING
    )
  SELECT seq_vital.nextval AS VITALID,
    a.*
  FROM
    (SELECT vitals.person_id                        AS PATID,
      vitals.visit_occurrence_id                    AS ENCOUNTERID ,
      vitals.measure_date                           AS measure_date,
      COALESCE( vitals.measure_time, DEFAULT_TIME ) AS measure_time ,
      vitals.obs_source                             AS VITAL_SOURCE ,
      MAX( vitals.HT )                              AS HT,
      MAX( vitals.WT )                              AS WT ,
      MAX( vitals.diastolic )                       AS diastolic,
      MAX( vitals.systolic )                        AS systolic ,
      MAX( vitals.bmi )                             AS bmi,
      MAX( vitals.bp_position )                     AS BP_POSITION,
      MAX(vitals.tobacco)                           AS TOBACCO,
      CASE MAX(cigaret)
        WHEN vYES
        THEN
          CASE MAX(other_tobacco)
            WHEN vYES
            THEN '03'
            WHEN VNO
            THEN '01'
            WHEN NULL
            THEN '01'
          END
        WHEN VNO
        THEN
          CASE MAX(other_tobacco)
            WHEN vYES
            THEN '02'
            WHEN VNO
            THEN '04'
          END
        WHEN NULL
        THEN
          CASE MAX(other_tobacco)
            WHEN vYES
            THEN '02'
            WHEN VNO
            THEN '04'
            WHEN NULL
            THEN 'NI'
          END
        ELSE NULL
      END  AS TOBACCO_TYPE,
      NULL AS RAW_DIASTOLIC ,
      NULL AS RAW_SYSTOLIC,
      NULL AS RAW_BP_POSITION,
      MAX(raw_tobacco) RAW_TOBACCO,
      NULL RAW_TOBACCO_TYPE,
      CASE
        WHEN MAX(cigaret_num) >= 0.5
        THEN '07'
        WHEN MAX(cigaret_num) < 0.5
        THEN '08'
        WHEN MAX(cigaret_num) IS NULL
        THEN
          CASE MAX(cigaret)
            WHEN vYES
            THEN '02'
            ELSE
              CASE --max(smoking_base)
                WHEN MAX(smoking_base) IS NULL
                THEN
                  CASE MAX(smoking_other)
                      --WHEN NULL THEN 'NI'
                    WHEN vYES
                    THEN '02'
                    WHEN vNO
                    THEN 'NI'
                    ELSE 'NI'
                  END
                ELSE MAX(smoking_base)
              END
          END
        ELSE 'NI'
      END AS smoking
    FROM -- vitals
      (SELECT CAST( person_id AS     VARCHAR(20) ) AS person_id ,
        CAST( visit_occurrence_id AS VARCHAR(20) ) AS visit_occurrence_id ,
        observation_date as measure_date ,
        --TO_CHAR( observation_date, 'YYYY-MM-DD' ) measure_date ,
        TO_CHAR( observation_time, 'HH24:SS') measure_time ,
        CASE OBSERVATION_TYPE_CONCEPT_ID
          WHEN OBS_TYPE_EHR
          THEN PCOR_HEALTHCARE
          WHEN OBS_TYPE_PATIENT_REPORT
          THEN PCOR_PATIENT_REPORT
          ELSE PCOR_NO_INFORMATION
        END AS obs_source ,
        CASE observation_concept_id
          WHEN HEIGHT
          THEN f_ephir_ht_to_inches(value_as_string)
          ELSE NULL
        END AS HT ,
        CASE observation_concept_id
          WHEN WEIGHT
          THEN
            CASE unit_concept_id
              WHEN OUNCE
              THEN value_as_number / 16
              ELSE value_as_number
            END
          ELSE NULL
        END AS WT ,
        CASE
          WHEN observation_concept_id IN( DIASTOLIC, DIASTOLIC_SITTING, DIASTOLIC_STANDING, DIASTOLIC_SUPINE )
          THEN value_as_number
          ELSE NULL
        END AS diastolic ,
        CASE
          WHEN observation_concept_id IN( SYSTOLIC, SYSTOLIC_SITTING, SYSTOLIC_STANDING, SYSTOLIC_SUPINE )
          THEN value_as_number
          ELSE NULL
        END AS systolic ,
        CASE observation_concept_id
            /* key off of systolic to get bp_position */
          WHEN SYSTOLIC
          THEN PCOR_NO_INFORMATION
          WHEN SYSTOLIC_SITTING
          THEN PCOR_BP_SITTING
          WHEN SYSTOLIC_STANDING
          THEN PCOR_BP_STANDING
          WHEN SYSTOLIC_SUPINE
          THEN PCOR_BP_SUPINE
          ELSE NULL
        END AS bp_position ,
        CASE observation_concept_id
          WHEN BMI
          THEN value_as_number
          ELSE NULL
        END AS bmi,
        --
        CASE observation_concept_id
          WHEN TOBACCO
          THEN
            CASE upper(value_as_string)
              WHEN vYES
              THEN '05'
              WHEN vNEVER
              THEN '04'
              WHEN vNOT_ASKED
              THEN 'NI'
              WHEN vQUIT
              THEN '03'
              ELSE 'NI'
            END
          ELSE NULL
        END AS smoking_base ,
        CASE OBSERVATION_TYPE_CONCEPT_ID
          WHEN OBS_TYPE_EHR
          THEN
            CASE observation_concept_id
              WHEN PIPE
              THEN upper(value_as_string)
              WHEN CIGAR
              THEN upper(value_as_string)
              ELSE NULL
            END
          ELSE NULL
        END AS smoking_other,
        CASE observation_concept_id
          WHEN TOBACCO
          THEN
            CASE upper(value_as_string)
              WHEN vYES
              THEN '01'
              WHEN vNEVER
              THEN '02'
              WHEN vNOT_ASKED
              THEN '06'
              WHEN vQUIT
              THEN '03'
              WHEN vPASSIVE
              THEN '04'
               ELSE
              CASE is_numeric(f_ephir_ParseNum(value_as_string))
                WHEN 1
                THEN '01'
                ELSE 'OT'
              END
          END
          ELSE NULL
        END AS tobacco ,
        CASE observation_concept_id
          WHEN CIGARET
          THEN
            CASE is_number(f_ephir_ParseNum(value_as_string))
              WHEN 1
              THEN CAST(f_ephir_ParseNum(value_as_string) AS NUMERIC(4,2))
              ELSE NULL
            END
          ELSE NULL
        END AS cigaret_num,
        CASE observation_concept_id
          WHEN CIGARET
          THEN upper(value_as_string)
          ELSE NULL
        END AS cigaret ,
        CASE OBSERVATION_TYPE_CONCEPT_ID
          WHEN OBS_TYPE_EHR
          THEN
            CASE observation_concept_id
              WHEN VPIPE
              THEN upper(value_as_string)
              WHEN CIGAR
              THEN upper(value_as_string)
              WHEN SNUFF
              THEN upper(value_as_string)
              WHEN CHEW
              THEN upper(value_as_string)
              ELSE NULL
            END
          ELSE NULL
        END AS other_tobacco,
        CASE observation_concept_id
          WHEN TOBACCO
          THEN upper(value_as_string)
          ELSE NULL
        END AS raw_tobacco
      FROM observation
      WHERE observation_concept_id IN ( HEIGHT, WEIGHT, BMI , DIASTOLIC_SITTING, DIASTOLIC_STANDING, DIASTOLIC_SUPINE, DIASTOLIC , SYSTOLIC_SITTING, SYSTOLIC_STANDING, SYSTOLIC_SUPINE, SYSTOLIC , TOBACCO, CIGARET, CIGAR, VPIPE, SNUFF, CHEW )
      ) vitals
    GROUP BY person_id,
      visit_occurrence_id,
      measure_date,
      measure_time,
      obs_source
    ) a;
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
END vital_prc;
/
