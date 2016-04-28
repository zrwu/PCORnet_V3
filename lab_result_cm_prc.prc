CREATE OR REPLACE PROCEDURE                     lab_result_cm_prc
AS
/*	OMOP to PCORnet Lab_result_cm
	Change Log
	Who		WHEN		What
     yb     11May2015   Initial Version
     ZWu 02/10/2016 CONVERTED FROM tsql
 */
 
err_msg  varchar2(2000) :='';
err_code  varchar2(200) :='';
rowsInserted  varchar2(15);
NOENCOUNTERID  VARCHAR2(20):= 'no encounter found';

BEGIN

-- truncate statement
	execute immediate 'truncate table pcornet_lab_result_cm';
	insert into etl_log(log_id,log_entry, log_time)
	select seq_log_id.NextVal,'lab_result_cm TRUNCATED' as what, sysdate from dual;
	insert into etl_log(log_id,log_entry, log_time)
	select seq_log_id.NextVal,'lab_result_cm START' as what, sysdate from dual;


	    -- INSERT statement
		INSERT  INTO pcornet_lab_result_cm
				( lab_result_cm_id, patid, encounterid,lab_name, specimen_source
					, lab_loinc, priority
					--
					, result_loc, lab_px
					,lab_px_type, lab_order_date, specimen_date
					--
					, specimen_time, result_date, result_time
					, result_qual, result_num
					--
					, result_modifier
					, result_unit
					, norm_range_low, NORM_MODIFIER_LOW
					, norm_range_high, NORM_MODIFIER_HIGH, abn_ind
					, raw_lab_name, raw_lab_code, raw_panel
					, raw_result, raw_unit, raw_order_dept
					, raw_facility_code)
		SELECT	SEQ_LAB.NEXTVAL, A.* FROM (
				SELECT /*+ parallel(o,8)*/ o.person_id AS patid
				, coalesce(cast(o.visit_occurrence_id as varchar(20)), NOENCOUNTERID) AS encounterid
				, substr(l1.source_code,1, 10) AS lab_name
				, coalesce(l2.source_code, 'OT') AS specimen_source
				, c1.concept_code AS lab_loinc 
				, NULL AS priority
				--
				, CASE o.visit_occurrence_id
						WHEN NULL THEN 'L'
						ELSE 'P'
				  END AS result_loc
				, c1.concept_code AS lab_px
				, 'LC' AS lab_px_type
				, NULL AS lab_order_date
				, NULL AS specimen_date
				--
				, NULL AS specimen_time
				, TO_DATE(to_char(o.observation_date)) AS result_date
        --, TO_DATE(to_char(o.observation_date, 'YYYY-MM-DD')) AS result_date
				, to_char(o.observation_time, 'HH24:MI') AS result_time
				, coalesce(l3.source_code, 'NI') AS result_qual
				, o.value_as_number AS result_num
				--
				, NULL AS result_modifier
				, l4.source_code AS result_unit
				, o.range_low AS norm_range_low -- remove '<>='
				, NULL AS modifier_low
				, o.range_high AS norm_range_high
				, NULL AS modifier_high
				, coalesce(l5.source_code, 'NI') AS abn_ind
				, o.observation_source_value AS raw_lab_name
				, NULL AS raw_lab_code
				, c2.concept_name AS raw_panel
				, c3.concept_name AS raw_result
				, o.unit_source_value AS raw_unit
				, NULL AS raw_order_dept 
				, NULL AS raw_facility_code 
		FROM omop.observation o
			LEFT JOIN concept c1 ON o.observation_concept_id = c1.concept_id
			LEFT JOIN concept c2 ON o.observation_type_concept_id = c2.concept_id
			LEFT JOIN concept c3 ON o.value_as_concept_id = c3.concept_id
			JOIN local_source_to_concept_map l1 ON o.observation_concept_id = l1.target_concept_id 
														AND l1.mapping_type = 'Test name'  
			LEFT JOIN local_source_to_concept_map l2 ON o.observation_concept_id = l2.target_concept_id 
														AND l2.mapping_type = 'Lab specimen'
			LEFT JOIN local_source_to_concept_map l3 ON o.value_as_concept_id = l3.target_concept_id 
														AND l3.mapping_type = 'Result qualifier'
			LEFT JOIN local_source_to_concept_map l4 ON o.unit_concept_id = l4.target_concept_id 
														AND l4.mapping_type = 'Result unit'
			LEFT JOIN local_source_to_concept_map l5 ON o.value_as_concept_id = l5.target_concept_id 
														AND l5.mapping_type = 'Abnormal indicator') A;		

	rowsInserted := f_formatNumber( SQL%ROWCOUNT );
		insert into etl_log(log_id,log_entry, log_time)
		select seq_log_id.NextVal,'lab_result_cm: ' || rowsInserted || ' inserted' , sysdate from dual;
		COMMIT; 
    --------
	EXCEPTION
        WHEN OTHERS THEN

          err_code := SQLCODE;
          err_msg :=  DBMS_UTILITY.FORMAT_ERROR_BACKTRACE || ' ' || SUBSTR(SQLERRM, 1, 200);
    
          insert into etl_log(log_id, log_entry, log_time)
          select seq_log_id.NextVal,'LAB_RESULT_CM failed => ' || err_code ||' ' || err_msg as what, sysdate from dual;   
          commit;
          
          raise_application_error(-20001,'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM); 
END  LAB_RESULT_CM_PRC
;
/
