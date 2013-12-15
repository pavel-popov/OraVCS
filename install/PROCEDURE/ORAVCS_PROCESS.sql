
  CREATE OR REPLACE PROCEDURE "ORAVCS_PROCESS" (
  schema_in IN VARCHAR2 DEFAULT USER
)
AUTHID CURRENT_USER
IS
  xml_h       NUMBER;         -- XML handle returned by OPEN
  xml_data    XMLTYPE;        -- metadata is returned in a XML

  clob_h      NUMBER;         -- CLOB handle
  clob_th     NUMBER;         -- CLOB transformation handle

  clob_data   CLOB;           -- metadata is returned in a CLOB

  i           NUMBER := 1;

  object_type VARCHAR2(255);
  object_name VARCHAR2(255);
  xml_type    VARCHAR2(255);

  node_not_found exception;
  pragma exception_init(node_not_found, -30625);
BEGIN

  oravcs_logger('Start of exporting objects in schema '||schema_in);
  xml_h := DBMS_METADATA.OPEN('SCHEMA_EXPORT');

  DBMS_METADATA.SET_FILTER(xml_h,'SCHEMA', schema_in);
  --DBMS_METADATA.SET_FILTER(xml_h,'EXCLUDE_NAME_EXPR','LIKE ''ORADDL%''');

  LOOP

    object_type := NULL;
    object_name := NULL;

    clob_data := NULL;

    xml_data := DBMS_METADATA.FETCH_XML(xml_h);
    EXIT WHEN xml_data IS NULL;

    BEGIN
      object_type := xml_data.extract('/ROWSET/ROW/*[1]/SCHEMA_OBJ/TYPE_NAME/text()').getStringVal();
      object_name := xml_data.extract('/ROWSET/ROW/*[1]/SCHEMA_OBJ/NAME/text()').getStringVal();
      xml_type    := XMLType(xml_data.extract('/ROWSET/ROW/*[1]').getClobVal()).getRootElement();

      oravcs_logger('object_type '||object_type);
      oravcs_logger('object_name '||object_name);

      IF object_type IS NOT NULL
        AND object_name IS NOT NULL
        AND NOT object_type in ('TABLE PARTITION', 'SCHEDULER JOB')
        AND NOT xml_type like 'ALTER%'
        AND NOT xml_type = 'TABLE_DATA_T'
      THEN
        IF object_type in ('TYPE','PACKAGE')
        THEN
          object_type := object_type||'_SPEC';
        END IF;

        BEGIN

          clob_h := DBMS_METADATA.OPEN(object_type);

          -- set filters
          DBMS_METADATA.SET_FILTER(clob_h, 'SCHEMA', schema_in);
          DBMS_METADATA.SET_FILTER(clob_h, 'NAME', object_name);

          IF object_type IN ('INDEX_T','TRIGGER_T')
          THEN
            DBMS_METADATA.SET_FILTER(clob_h,'SYSTEM_GENERATED',FALSE);
          END IF;

          clob_th := DBMS_METADATA.ADD_TRANSFORM(clob_h, 'MODIFY');
          DBMS_METADATA.SET_REMAP_PARAM(clob_th, 'REMAP_SCHEMA', USER, NULL);

          clob_th := DBMS_METADATA.ADD_TRANSFORM(clob_h, 'DDL');

          DBMS_METADATA.SET_TRANSFORM_PARAM(clob_th, 'SQLTERMINATOR', TRUE);

          BEGIN
            DBMS_METADATA.SET_TRANSFORM_PARAM(clob_th, 'SEGMENT_ATTRIBUTES', FALSE);
            EXCEPTION
              WHEN DBMS_METADATA.INVALID_ARGVAL
                THEN NULL; --dbms_output.put_line('INVALID_ARGVAL');
          END;

          clob_data := DBMS_METADATA.FETCH_CLOB(clob_h);

        END;

        oravcs_logger('Inserting metadata for '||object_type||' '||schema_in||'.'||object_name);

        INSERT INTO oravcs.oravcs_metadata(id, schema, created_at,
                                           md, xml, obj_type, obj_name)
        VALUES (oravcs.oravcs_seq.nextval, schema_in, systimestamp,
                clob_data, xml_data, object_type, object_name);

        DBMS_METADATA.CLOSE(clob_h);

        -- dbms_output.put_line('inserted');
      END IF;

      i := i+1;

      EXCEPTION
        WHEN node_not_found
        THEN oravcs_logger('Node not found: '||object_type||' '||schema_in||'.'||object_name);
    END;

  END LOOP;

  DBMS_METADATA.CLOSE(xml_h);

  EXCEPTION
    WHEN others
    THEN oravcs_logger('Error stack...'||chr(10)||DBMS_UTILITY.FORMAT_ERROR_STACK(), SQLCODE);
         oravcs_logger('Error backtrace...'||chr(10)||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE(), SQLCODE);
         RAISE;
END;
/
