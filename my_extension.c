#include "postgres.h"
#include "miscadmin.h"
#include "executor/spi.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/skey.h"
#include "access/table.h"
#include "catalog/namespace.h"
#include "postmaster/bgworker.h"
#include "storage/lwlock.h"
#include "storage/ipc.h"
#include "utils/snapmgr.h"
#include "utils/snapmgr.h"
#include "utils/fmgroids.h"
#include "utils/builtins.h"
#include "fmgr.h"

PG_MODULE_MAGIC;

void _PG_init(void);
void my_extension_main(void);
void my_extension_dynamic_main(void);

typedef struct SharedState {
	char relname[NAMEDATALEN];
	char schemaname[NAMEDATALEN];
	LWLock *lck;
} SharedState;

/* Links to shared memory state */
static SharedState *myext = NULL;
/* Saved hook values in case of unload */
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static void
myext_shmem_request(void)
{
    if (prev_shmem_request_hook)
        prev_shmem_request_hook();

	RequestAddinShmemSpace(sizeof(SharedState));
	/* Contains lock and the shared variables */
    RequestNamedLWLockTranche("myext_lock", 1);
}

/*
 * shmem_startup hook: allocate or attach to shared memory.
 */
static void
myext_shmem_startup(void)
{
	bool		found = false;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	/* reset in case of a restart within the postmaster */
	myext = NULL;

	/*
	 * Create or attach to the shared memory state
	 */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	myext = ShmemInitStruct("myext_lock",
						  sizeof(SharedState),
						  &found);
	if (!found)
	{
		/* first time through */
		LWLockPadded *locks = GetNamedLWLockTranche("myext_lock");

		myext->lck = &(locks[0].lock);
		myext->relname[0] = '\0';
		myext->schemaname[0] = '\0';
	}
	LWLockRelease(AddinShmemInitLock);
}


void
_PG_init(void)
{
	BackgroundWorker bgw;
	/*
	 * In order to create our shared memory area, we have to be loaded via
	 * shared_preload_libraries.
	 * Check which ensures this is called via load_libraries() call in postgres
	 */	
	if (!process_shared_preload_libraries_in_progress)
		elog(ERROR, "my_extension is not in shared_preload_libraries");
	
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = myext_shmem_request;
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = myext_shmem_startup;
	
	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags =	BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_ConsistentState;
	snprintf(bgw.bgw_library_name, BGW_MAXLEN,
			 "my_extension");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN,
			 "my_extension_main");
	snprintf(bgw.bgw_name, BGW_MAXLEN,
			 "my_extension worker");
	/* To maintain a continously running background worker even if it dies */
	bgw.bgw_restart_time = 5;

	RegisterBackgroundWorker(&bgw);
}
void my_extension_main(void)
{
	/* Register a dynamic background worker */
//	BackgroundWorker bgw;
//BackgroundWorkerHandle *bgw_handle;
	Relation classrel;
	ScanKeyData key[2];
	NameData   *table_name = (NameData *) palloc0(NAMEDATALEN);
	NameData   *sch_name = (NameData *) palloc0(NAMEDATALEN);
	TableScanDesc scan;
	HeapTuple tuple;
	int relnamelen;
	int schemanamelen;
	Oid namespace_id = InvalidOid;

/*	
	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags =	BGWORKER_SHMEM_ACCESS |
	BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_ConsistentState;
	snprintf(bgw.bgw_library_name, BGW_MAXLEN,
			 "my_extension");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN,
			 "my_extension_dynamic_main");
	snprintf(bgw.bgw_name, BGW_MAXLEN,
			 "my_extension dynamic worker");
	bgw.bgw_restart_time = 5;
	
	RegisterDynamicBackgroundWorker(&bgw, &bgw_handle); */
	
	/* Connect to database */
	BackgroundWorkerInitializeConnection("postgres", NULL, 0);

	/* Your own logic that runs periodically in background */
	elog(LOG, "Running my_extension static worker in a loop\n");
	while (true)
	{
		/* If background worker does not get signalled, pg_ctl stop will fail to stop the server */
		BackgroundWorkerUnblockSignals();
		CHECK_FOR_INTERRUPTS();
		LWLockAcquire(myext->lck, LW_EXCLUSIVE);
		relnamelen = strlen(myext->relname);
		strncpy(table_name->data, myext->relname, relnamelen);
		table_name->data[relnamelen] = '\0';
		schemanamelen = strlen(myext->schemaname);
		strncpy(sch_name->data, myext->schemaname, schemanamelen);
		sch_name->data[schemanamelen] = '\0';
		LWLockRelease(myext->lck);

		elog(LOG, "Table name %s", table_name->data);
		elog(LOG, "Schema name %s", sch_name->data);
	
		StartTransactionCommand();
		/* Access system caches to retrieve namespace id */
		namespace_id = get_namespace_oid(sch_name->data, true);
		if (!OidIsValid(namespace_id))
			continue;
		/* Read system catalog and store information in backend memory */
		classrel = table_open(RelationRelationId, AccessShareLock);
		ScanKeyInit(&key[0],
                	Anum_pg_class_relname,
                	BTEqualStrategyNumber, F_NAMEEQ,
                	NameGetDatum(table_name));
		ScanKeyInit(&key[1],
                	Anum_pg_class_relnamespace,
                	BTEqualStrategyNumber, F_OIDEQ,
                	ObjectIdGetDatum(namespace_id));

		scan = table_beginscan_catalog(classrel, 2, key);

		if ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
    	{
			Form_pg_class classform = (Form_pg_class) GETSTRUCT(tuple);
			Oid shm_oid;
			shm_oid = classform->oid;
			elog(LOG, "Saved oid %u", shm_oid);
    	}
	
    	table_endscan(scan);
    	table_close(classrel, AccessShareLock);
		CommitTransactionCommand();

	}
	/*XXX Share information with dynamic worker using shared memory */
}

void
my_extension_dynamic_main(void)
{
	elog(LOG, "Entered the dynamic worker logic");
	
	while (true)
	{
		BackgroundWorkerUnblockSignals();
		CHECK_FOR_INTERRUPTS();
		/* Read system catalog and store information in backend memory */
	}
}

PG_FUNCTION_INFO_V1(create_table_in_cfunc);
/* Create a table using spi interface */
Datum
create_table_in_cfunc(PG_FUNCTION_ARGS)
{
	char *relname;
	char *sch_name = palloc0(NAMEDATALEN);
	int relnamelen;
	int schnamelen;
	StringInfoData buf;
	int ret;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("relation name can not be null")));
	relname = text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (PG_ARGISNULL(1))
	{
		strncpy(sch_name, "public", 6);
		sch_name[6] = '\0';
	}
	else
		sch_name = text_to_cstring(PG_GETARG_TEXT_PP(1));
	
	PushActiveSnapshot(GetTransactionSnapshot());
	initStringInfo(&buf);
	
	/* Create a table using SPI interface */
	/* Build query */
	
	appendStringInfo(&buf, "CREATE table %s.%s", sch_name, relname);
	appendStringInfo(&buf, "(a int)");
	
	SPI_connect();

	/* Execute create table command */
	ret = SPI_execute(buf.data, false, 0);
	if (ret != SPI_OK_UTILITY)
		elog(ERROR, "Create table failed with : SPI_execute failed with error %s", SPI_result_code_string(ret));
	elog(LOG, "Create table: executed " UINT64_FORMAT, SPI_processed);

	/* Terminate transaction */
	SPI_finish();
	PopActiveSnapshot();
	
	relnamelen = strlen(relname);
	schnamelen = strlen(sch_name);
	
	LWLockAcquire(myext->lck, LW_EXCLUSIVE);
	strncpy(myext->relname, relname, relnamelen);
	myext->relname[relnamelen] = '\0'; 
	strncpy(myext->schemaname, sch_name, schnamelen);
	myext->schemaname[schnamelen] = '\0'; 
	LWLockRelease(myext->lck);

	PG_RETURN_VOID();
}

