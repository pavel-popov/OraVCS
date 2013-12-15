
  CREATE OR REPLACE PROCEDURE "ORAVCS_LOGGER" (
  message_in IN VARCHAR2,
  errlevel_in IN INTEGER DEFAULT 0)
IS
  PRAGMA AUTONOMOUS_TRANSACTION;
  v_errlevel INTEGER;
BEGIN

  IF SQLCODE != 0 AND errlevel_in = 0
  THEN
    v_errlevel := SQLCODE;
  ELSE
    v_errlevel := errlevel_in;
  END IF;

  INSERT INTO oravcs_log(id, dt, message, errlevel)
  VALUES (oravcs_log_seq.nextval, systimestamp,
          message_in, v_errlevel);

  COMMIT;

END oravcs_logger;
/
