CREATE OR REPLACE PROCEDURE                                                                        "PROCEDURE_PRC" 
AS
/*	OMOP to PCORnet Procedure
	PreCondition:
            1) the encounter table has already been created
		2) uses f_formatNumber()
            3) synonyms: procedure_occurrence, concept
	PostConditions:
		1) Encounter table truncated and then repopulated
		2) entry in etl_log table
	Change Log
	Who		WHEN		What
    dtk     26Feb15         Make proc table driven
	dtk		28Feb15		Default encounterid and encounter type
	dtk     28Feb15     Add Distinct
	dtk  02Apr2015   Port to oracle
  yb		29May15		Added px_date, px_source columns load as part of PCORnet V2 changes
   Zwu Feb. 3 2016 Coverted from TSQL as part of CDM V3
   Zwu Apr. 26 2016 substr(procedure_source_value, 1,11) because SAS limited the field PX to 11 characters
 */

	PROC_NAME varchar2(30) := 'PCORnet Procedure v3';
	rowsInserted VARCHAR(15);
	Counter Int := 1;
	ICD9_VOCAB INT := 3 ;
	ICD9_CODE VARCHAR(2) := '09';
	ICD10_VOCAB INT := 35;
	ICD10_CODE VARCHAR(2) := '10' ;
	CPT4_VOCAB INT := 4;
	CPT4_CODE VARCHAR(2) := 'C4';
	HCPCS_VOCAB INT := 5;
	HCPCS_CODE VARCHAR(2) := 'HC';
	LOINC_VOCAB INT := 6;
	LOINC_CODE VARCHAR(2) := 'LC' ;
	NDC_VOCAB INT := 9;
	NDC_CODE VARCHAR(2) := 'ND';
	REV_CODE_VOCAB INT := 43;
	REV_CODE_CODE VARCHAR(2) := 'RE';
	OTHER_CODE VARCHAR(2) := 'OT';
  NOENCOUNTERID  VARCHAR(20) := '0000';
	DEFAULT_ENC_TYPE VARCHAR(2) := 'NI';
  --V2
  PROC_TYPE_C_HSP_TXN INT := 38000250;
	PROC_TYPE_C_BIL_TXN INT := 38000268;
	PROC_TYPE_C_HSP_ACCT INT := 42865905;
	PROC_TYPE_C_PROC_ORD INT := 38000275;
  PROC_TYPE_C_UKN INT := 0;
  PX_SRC_BIL VARCHAR(2) := 'BI';
  PX_SRC_ORD VARCHAR(2) := 'OD';
  PX_SRC_UKN VARCHAR(2) := 'UN';
	err_msg  varchar2(2000) :='';
	err_code  varchar2(200) :='';
    
  BEGIN
	-- truncate statement
	execute immediate 'truncate table pcornet_procedures';
	INSERT INTO  etl_log(log_entry, log_time)
	select PROC_NAME || ' TRUNCATED' as what, sysDate FROM DUAL;
	
	insert into etl_log(log_entry, log_time)
	select PROC_NAME || ' START' as what, sysDate FROM DUAL ;
  --etl_utility.rebuidTableIndex('OMOP', 'pcornet_procedure', 'D');
    INSERT INTO PCORNET_PROCEDURES( PROCEDURESID, patid, encounterid, enc_type, admit_date
                                 ,  providerid, px_date, 
                                 px, 
                                 px_type, 
                                 px_source, raw_px, raw_px_type	                                                    
                                 )                                           
    select seq_proc.nextval, a.* from (SELECT /*+ FULL(encounter) parallel(encounter,4)   */ DISTINCT
	       CAST( proc_occ.person_id AS VARCHAR(20) )AS patid
	     , COALESCE( CAST( encounterid AS VARCHAR(20) ), NOENCOUNTERID ) AS encounterId
		 , COALESCE( enc_type, DEFAULT_ENC_TYPE ) AS enc_type
              , COALESCE(encounter.admit_date, proc_occ.procedure_date ) AS admit_date
               --COALESCE( to_date(encounter.admit_date, 'YYYY-MM-DD') 
                                --, proc_occ.procedure_date ) AS admit_date
              , COALESCE( encounter.providerid, CAST( proc_occ.associated_provider_id AS VARCHAR2(20) ) )
              , proc_occ.procedure_date AS px_date
              , CASE WHEN procedure_concept_id>0 
                    THEN proc_concept.concept_code
                    --ELSE null
                    --SAS limited the field PX to 11 characters, but our data length (38) exceeds that  limitation
					 ELSE substr(procedure_source_value, 1,11)
			    END AS px
          --,proc_concept.concept_code px
              , CASE proc_concept.vocabulary_id 
                    WHEN ICD9_VOCAB           THEN ICD9_CODE
                    WHEN ICD10_VOCAB        THEN ICD10_CODE
                    WHEN CPT4_VOCAB          THEN CPT4_CODE
                    WHEN HCPCS_VOCAB       THEN HCPCS_CODE
                    WHEN LOINC_VOCAB       THEN LOINC_CODE
                    WHEN NDC_VOCAB          THEN NDC_CODE
                    WHEN REV_CODE_VOCAB THEN REV_CODE_CODE
                    ELSE OTHER_CODE
                END px_type
              , CASE proc_occ.procedure_type_concept_id 
                    WHEN PROC_TYPE_C_HSP_TXN  THEN PX_SRC_BIL
                    WHEN PROC_TYPE_C_BIL_TXN  THEN PX_SRC_BIL
                    WHEN PROC_TYPE_C_HSP_ACCT THEN PX_SRC_BIL
                    WHEN PROC_TYPE_C_PROC_ORD THEN PX_SRC_ORD
                    WHEN PROC_TYPE_C_UKN	   THEN PX_SRC_UKN
                    ELSE PX_SRC_UKN
                END px_source    
              , procedure_source_value raw_px
              , vocabulary.vocabulary_name AS raw_px_type																					
      FROM procedure_occurrence proc_occ
       LEFT OUTER JOIN pcornet_encounter encounter  ON  visit_occurrence_id = encounterid
       LEFT OUTER JOIN concept proc_concept 
          ON  procedure_concept_id=proc_concept.concept_id 
       LEFT OUTER JOIN vocabulary
          ON  vocabulary.vocabulary_id = proc_concept.vocabulary_id) a;

		 rowsInserted := f_formatNumber( SQL%ROWCOUNT );
		INSERT INTO etl_log(log_id, log_entry, log_time)
		SELECT seq_log_id.NextVal
			, PROC_NAME || ' inserted: ' || rowsInserted, sysdate from dual;

		commit ;
		--etl_utility.rebuidTableIndex('OMOP', 'pcornet_procedure', 'R');
	EXCEPTION
	  WHEN OTHERS THEN
		err_code := SQLCODE;
		err_msg :=  DBMS_UTILITY.FORMAT_ERROR_BACKTRACE || ' ' || SUBSTR(SQLERRM, 1, 200);
		ROLLBACK;

		insert into etl_log(log_entry, log_time)
		select PROC_NAME || ' failed => ' || err_msg, sysdate from dual;   
		commit;
          
		raise_application_error(-20001,'An error was encountered '||err_msg );
END procedure_prc;
/
