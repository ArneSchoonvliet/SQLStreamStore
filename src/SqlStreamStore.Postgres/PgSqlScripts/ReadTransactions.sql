CREATE OR REPLACE FUNCTION __schema__.read_transactions(
    _datname NAME
)
    RETURNS TABLE(
        backend_xid      XID
    ) AS $F$
BEGIN
    RETURN QUERY
        SELECT a.backend_xid FROM pg_stat_activity a
        WHERE datname = _datname AND a.backend_xid IS NOT NULL;
END;

$F$
LANGUAGE 'plpgsql';