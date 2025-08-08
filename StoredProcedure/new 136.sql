CREATE OR REPLACE PROCEDURE LOAD_AND_VALIDATE_ALL_FILES()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    temp_table_name STRING;
    v_file_name STRING;
    total_rows_loaded INT DEFAULT 0;
    duplicate_rows    INT DEFAULT 0;
    null_rows         INT DEFAULT 0;
    status STRING;
    error_message STRING;
    result_message STRING;

    c1 CURSOR FOR
        SELECT DISTINCT METADATA$FILENAME AS file_name
        FROM @Raw_STAGE (FILE_FORMAT => 'ff_csv');
BEGIN
    FOR rec IN c1 DO
        v_file_name := rec.file_name;
        temp_table_name := 'TEMP_CUST_DATA';
        status := 'Failed';
        error_message := NULL;

        -- Create temporary table
        EXECUTE IMMEDIATE
            'CREATE OR REPLACE TEMPORARY TABLE ' || temp_table_name || ' (
                C_CUSTKEY       INT,
                C_NAME          STRING,
                C_ADDRESS       STRING,
                C_NATIONKEY     INT,
                C_PHONE         STRING,
                C_ACCTBAL       INT,
                C_MKTSEGMENT    STRING,
                C_COMMENT       STRING
            )';

        BEGIN
            -- Build dynamic COPY INTO command with specific file
            EXECUTE IMMEDIATE
                'COPY INTO ' || temp_table_name || '
                 FROM @Raw_STAGE/' || v_file_name  || '
                 FILE_FORMAT = (FORMAT_NAME = ''ff_csv'')
                 ON_ERROR = ''ABORT_STATEMENT''';

          -- Validation
            total_rows_loaded := (SELECT COUNT(*) FROM IDENTIFIER(:temp_table_name));
            null_rows         := ( SELECT COUNT(*) FROM IDENTIFIER(:temp_table_name) 
			                                       WHERE C_PHONE IS NULL OR C_NATIONKEY IS NULL
								  );
            duplicate_rows := ( SELECT COUNT(*) 
			                    FROM ( SELECT C_CUSTKEY
								             ,ROW_NUMBER() OVER (PARTITION BY C_CUSTKEY ORDER BY C_CUSTKEY) AS rn
                                                       FROM IDENTIFIER(:temp_table_name)
                                                     ) a
                                                WHERE rn > 1
                              );
           
            IF (null_rows = 0 AND duplicate_rows = 0 AND total_rows_loaded > 0) THEN
                INSERT INTO CUSTOMER_DATA (
                      C_CUSTKEY
					, C_NAME
					, C_ADDRESS
					, C_NATIONKEY
					, C_PHONE
					, C_ACCTBAL
					, C_MKTSEGMENT
					, C_COMMENT
                )
                SELECT C_CUSTKEY
				     , C_NAME
				     , C_ADDRESS
				     , C_NATIONKEY
				     , C_PHONE
				     , C_ACCTBAL
				     , C_MKTSEGMENT
				     , C_COMMENT
                FROM IDENTIFIER(:temp_table_name);

                status := 'Success';
                result_message := 'File ' || v_file_name || ' loaded successfully.';
            ELSE
                status := 'Failed - Validation Errors';
                error_message := 'File: ' || v_file_name || ' failed. ' || null_rows || ' null rows, ' || duplicate_rows || ' duplicates.';
                result_message := error_message;
            END IF;


        EXCEPTION
            WHEN OTHER THEN
                status := 'Failed - Execution Error';
                error_message := SQLERRM;
                result_message := 'Error in file ' || v_file_name || ': ' || error_message;
        END;
     -- Log
        INSERT INTO AUDIT_LOG (
            status,
            load_timestamp,
            rows_loaded,
            duplicate_rows,
            null_rows,
            error_message,
            source_file
        )
        VALUES (
            :status,
            CURRENT_TIMESTAMP(),
            :total_rows_loaded,
            :duplicate_rows,
            :null_rows,
            :error_message,
            :v_file_name
        );
    END FOR;

    RETURN 'All files processed and audited.';
END;
$$;
