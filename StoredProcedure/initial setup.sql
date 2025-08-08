--✅ Step 1: Define a  Stage
Create stage Raw_STAGE 
--✅ Step 2: Define a File Format
CREATE OR REPLACE FILE FORMAT ff_csv
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
NULL_IF = ('', 'NULL')
EMPTY_FIELD_AS_NULL = TRUE
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
TRIM_SPACE = TRUE;
--✅ Step 3: Create the Target Table
CREATE OR REPLACE TABLE customer_data (
    C_CUSTKEY     INT,
    C_NAME        STRING,
    C_ADDRESS     STRING,
    C_NATIONKEY   INT,
    C_PHONE       STRING,
    C_ACCTBAL     INT,
    C_MKTSEGMENT  STRING,
    C_COMMENT     STRING
);
--✅ Step 4: Create the AUDIT Table
CREATE OR REPLACE TABLE AUDIT_LOG (
    STATUS           STRING,
    LOAD_TIMESTAMP   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    ROWS_LOADED      INT,
    DUPLICATE_ROWS   INT,
    NULL_ROWS        INT,
    ERROR_MESSAGE    STRING,
    SOURCE_FILE      STRING
);

--------------------------------------------------
--✅ Step 4: Create the PROCEDURE LOAD_AND_VALIDATE
CREATE OR REPLACE PROCEDURE LOAD_AND_VALIDATE()
RETURNS VARCHAR()
LANGUAGE SQL
AS
$$
DECLARE
    temp_table_name STRING;
    total_rows_loaded INT DEFAULT 0;
    duplicate_rows INT DEFAULT 0;
    null_rows INT DEFAULT 0;
    status VARCHAR(50);
    error_message VARCHAR();
    result_message STRING;

BEGIN
    temp_table_name := 'TEMP_CUST_DATA';
    status := 'Failed';
    error_message := NULL;
   -- Drop temp table if it exists
    CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(:temp_table_name) 
	(
     C_CUSTKEY       INT,
     C_NAME          STRING,
     C_ADDRESS       STRING,
     C_NATIONKEY     INT,
     C_PHONE         STRING,
     C_ACCTBAL       INT,
     C_MKTSEGMENT    STRING,
     C_COMMENT       STRING,
     -- Metadata Columns
     SOURCE_FILE      STRING,
     FILE_ROW_NUMBER  INT,
     LOAD_TIMESTAMP   TIMESTAMP_NTZ
    );

    BEGIN
	  
        -- Step 1: Load data into temporary table
        COPY INTO IDENTIFIER(:temp_table_name)
        FROM (
              SELECT 
                  t.$1 AS C_CUSTKEY,
                  t.$2 AS C_NAME,
                  t.$3 AS C_ADDRESS,
                  t.$4 AS C_NATIONKEY,
                  t.$5 AS C_PHONE,
                  t.$6 AS C_ACCTBAL,
                  t.$7 AS C_MKTSEGMENT,
                  t.$8 AS C_COMMENT,
                  METADATA$FILENAME AS SOURCE_FILE,
                  METADATA$FILE_ROW_NUMBER AS FILE_ROW_NUMBER,
                  CURRENT_TIMESTAMP AS LOAD_TIMESTAMP
              FROM @Raw_STAGE t
             )
        FILE_FORMAT = (FORMAT_NAME = 'ff_csv')
        ON_ERROR = 'ABORT_STATEMENT';


        -- Step 2: Validation checks
        total_rows_loaded := (SELECT COUNT(*) FROM IDENTIFIER(:temp_table_name));

        null_rows := (
            SELECT COUNT(*) FROM IDENTIFIER(:temp_table_name)
            WHERE C_PHONE IS NULL OR C_NATIONKEY IS NULL
        );

        duplicate_rows := (
            SELECT COUNT(*)
            FROM (
                SELECT C_CUSTKEY,
                       ROW_NUMBER() OVER (PARTITION BY C_CUSTKEY ORDER BY C_CUSTKEY) AS rn
                FROM IDENTIFIER(:temp_table_name)
            ) a
            WHERE rn > 1
        );

        IF (null_rows = 0 AND duplicate_rows = 0 AND total_rows_loaded > 0) THEN
            INSERT INTO CUSTOMER_DATA (
                C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT
            )
            SELECT C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT  
            FROM IDENTIFIER(:temp_table_name);

            status := 'Success';
            result_message := 'Successfully loaded ' || total_rows_loaded || ' rows with no validation errors.';
        ELSE
            status := 'Failed - Validation Errors';
            error_message := 'Validation failed. ' || null_rows || ' rows with nulls, ' || duplicate_rows || ' duplicate rows found.';
            result_message := error_message;
        END IF;

    EXCEPTION
        WHEN OTHER THEN
            status := 'Failed - Execution Error';
            error_message := SQLERRM;
            result_message := error_message;
    END;

    -- Step 3: Log the outcome always
    INSERT INTO AUDIT_LOG (
        status,
        load_timestamp,
        rows_loaded,
        duplicate_rows,
        null_rows,
        error_message
    )
    VALUES (
        :status,
        CURRENT_TIMESTAMP(),
        :total_rows_loaded,
        :duplicate_rows,
        :null_rows,
        :error_message
    );

    RETURN 'Procedure execution completed with status: ' || status || '. Message: ' || result_message;

END;
$$;

CALL LOAD_AND_VALIDATE();

select * from CUSTOMER_DATA
select * from AUDIT_LOG
