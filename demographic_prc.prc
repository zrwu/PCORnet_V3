CREATE OR REPLACE PROCEDURE             "DEMOGRAPHIC_PRC" 
AS
/*	OMOP to PCORnet Demographic
	PreCondition:
		1) PCORnet specific Source to Concept Map entries in vocabulary.source_to_concept_map
		2) uses f_formatNumber()
            3) Synonyms: omop.person; voc.source_to_concept_map
	PostConditions:
		1) demographic table truncated and then repopulated
		2) entry in etl_log table
	Change Log
	Who		WHEN		What
     dtk            26Feb2015       Make the procedure table driven
     ZWu  Feb.8 2016 converted to Oracle PLSQL from TSQL
 */

	rowsInserted  varchar(15);
	BIO_BANK INT := 4001345;
	PROC_NAME varchar2(30) := 'PCORnet Demographic';
	err_msg  varchar2(2000) :='';
	err_code  varchar2(200) :='';

BEGIN
-- truncate statement
	execute immediate 'truncate table pcornet_demographic';
	INSERT INTO  etl_log(log_entry, log_time)
	select PROC_NAME || ' TRUNCATED' as what, sysDate FROM DUAL;
	
	insert into etl_log(log_entry, log_time)
	select PROC_NAME || ' START' as what, sysDate FROM DUAL ;

    -- INSERT statement
		INSERT  INTO pcornet_demographic( patId, birth_date, birth_time, sex, hispanic, race, biobank_flag
		                        , raw_sex, raw_hispanic, raw_race )
		SELECT person.person_id AS patid
			, to_date(cast( person.year_of_birth as varchar2(4) )  || '-'
			|| COALESCE( cast( person.month_of_birth as varchar(2) ), '01') || '-' 
			|| COALESCE( cast( person.month_of_birth as varchar2(2) ), '01'), 'YYYY-MM-DD') birth_date
			, NULL birth_time
			, COALESCE( gender.source_code, 'OT' ) AS sex
			, COALESCE( ethnicity.source_code, 'OT' ) AS hispanic
			, COALESCE( race.source_code, 'OT' ) AS race
			, COALESCE( biobank_map.source_code, 'N' ) AS biobank_flag
			, person.gender_source_value raw_sex
			, person.ethnicity_source_value raw_hispanic
			, person.race_source_value
		FROM person
		LEFT OUTER JOIN -- gender
			( SELECT source_code, target_concept_id AS concept_id
				FROM source_to_concept_map
			   WHERE source_vocabulary_id = 60 AND mapping_type ='Gender' 
			) gender ON gender.concept_id = gender_concept_id
		LEFT OUTER JOIN -- ethnicity
		    ( SELECT source_code, target_concept_id AS concept_id
			    FROM source_to_concept_map
			   WHERE source_vocabulary_id = 60 AND mapping_type ='Ethnicity' 
		    ) ethnicity ON ethnicity.concept_id = ethnicity_concept_id
		LEFT OUTER JOIN -- race
		    ( SELECT source_code, target_concept_id AS concept_id
               FROM  source_to_concept_map
			  WHERE source_vocabulary_id = 60 AND mapping_type ='Race' 
			) race ON race.concept_id = race_concept_id
		LEFT OUTER JOIN -- biobank record
		    ( SELECT person_id, min( value_as_concept_id ) concept_id  /* the yes concept id is less than no */
				FROM observation
				WHERE observation_concept_id = BIO_BANK
				GROUP BY person_id
			) biobank_obs ON biobank_obs.person_id = person.person_id
		LEFT OUTER JOIN -- biobank map
		    ( SELECT source_code, target_concept_id AS concept_id
				FROM source_to_concept_map
			   WHERE source_vocabulary_id = 60 AND mapping_type ='Race' 
			) biobank_map ON biobank_map.concept_id = biobank_obs.concept_id;		
		
		 rowsInserted := f_formatNumber( SQL%ROWCOUNT );
		INSERT INTO etl_log(log_id, log_entry, log_time)
		SELECT seq_log_id.NextVal
			, PROC_NAME || ' inserted: ' || rowsInserted, sysdate from dual;
		--------
		commit ;
		
	EXCEPTION
	  WHEN OTHERS THEN
		err_code := SQLCODE;
		err_msg :=  DBMS_UTILITY.FORMAT_ERROR_BACKTRACE || ' ' || SUBSTR(SQLERRM, 1, 200);
		ROLLBACK;

		insert into etl_log(log_entry, log_time)
		select PROC_NAME || ' failed => ' || err_msg, sysdate from dual;   
		commit;
          
		raise_application_error(-20001,'An error was encountered '||err_msg );
END demographic_prc;
/
