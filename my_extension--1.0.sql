CREATE FUNCTION create_table_in_cfunc(relname text, schemaname text)
RETURNS VOID VOLATILE
LANGUAGE C AS 'MODULE_PATHNAME', 'create_table_in_cfunc';
