/*----------------------------------------------------------------------
 *
 * tableamapi.c
 *		Support routines for API for Postgres table access methods
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/table/tableamapi.c
 *----------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"      // For GetHeapamTableAmRoutine, HeapAmOid
#include "access/blockchainam.h" // For GetBlockchainAmTableAmRoutine
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/pg_am.h"      // For BLOCKCHAINAM_OID
#include "commands/defrem.h"
#include "miscadmin.h"
#include "utils/builtins.h"     // For strcmp
#include "utils/guc_hooks.h"
#include "utils/lsyscache.h"    // For get_am_oid
#include "nodes/pg_list.h"      // For List


/*
 * TableAmInfo - One-stop shopping for information about a table AM.
 */
typedef struct TableAmInfo
{
	Oid			amoid;			/* OID of the access method itself */
	const char *amname;			/* Name of the access method */
	Oid			amhandler;		/* OID of the handler function (in pg_proc) */
	const TableAmRoutine *(*handler_hook) (void);	/* Hook to get the routine struct */
} TableAmInfo;

/*
 * Built-in table access methods.
 * This array is searched by GetTableAmRoutineByAmOidAndName().
 *
 * Note: amhandler should ideally be a real OID from pg_proc_d.h.
 * For "blockchain", we'll use InvalidOid placeholder and rely on handler_hook.
 */
static const TableAmInfo table_am_handlers[] = {
	{HeapAmOid, HEAP_TABLE_AM_NAME, HEAP_TABLE_AM_HANDLER_OID, GetHeapamTableAmRoutine},
	{BLOCKCHAINAM_OID, "blockchain", InvalidOid /* TODO: Define BlockchainTableAmHandlerOid */, GetBlockchainAmTableAmRoutine}
	/* Add new table AMs here. */
};

/*
 * GetTableAmRoutineByAmOid has been removed, its logic is merged into GetTableAmRoutine.
 */

/*
 * GetTableAmRoutine
 *		Return the TableAmRoutine struct for the given access method OID
 *		(from pg_am.oid).
 *
 * It first attempts to find a built-in AM by its OID in the
 * table_am_handlers array and use its C hook. If not found, it falls
 * back to calling the provided OID as a function, assuming it's an
 * AM handler function OID (this is legacy behavior and should ideally
 * not be hit for known AMs).
 */
const TableAmRoutine *
GetTableAmRoutine(Oid am_oid) /* Parameter is now treated as pg_am.oid */
{
	Datum		datum;
	const TableAmRoutine *routine = NULL;

	/* Try to find a built-in AM by its OID first */
	for (int i = 0; i < lengthof(table_am_handlers); i++)
	{
		if (table_am_handlers[i].amoid == am_oid)
		{
			if (table_am_handlers[i].handler_hook)
			{
				routine = table_am_handlers[i].handler_hook();
				/* Found via hook, no need to check amhandler OID */
			}
			else if (OidIsValid(table_am_handlers[i].amhandler))
			{
				/* This case is for AMs in the array without a direct C hook */
				elog(LOG, "Table AM OID %u found in internal handlers, using OidFunctionCall on its amhandler OID %u",
					 am_oid, table_am_handlers[i].amhandler);
				datum = OidFunctionCall0(table_am_handlers[i].amhandler);
				routine = (TableAmRoutine *) DatumGetPointer(datum);
			}
			else
			{
				/* This case should ideally not be reached for built-ins */
				elog(ERROR, "no handler_hook or amhandler OID for table access method OID %u in table_am_handlers", am_oid);
			}
			break; /* Found in array, exit loop */
		}
	}

	if (routine == NULL)
	{
		/*
		 * Not found in table_am_handlers. Fall back to treating the input OID
		 * as a direct handler function OID. This supports dynamically loaded
		 * AMs or cases where table_am_handlers might not be exhaustive.
		 */
		elog(WARNING, "Table AM OID %u not found in table_am_handlers. Falling back to OidFunctionCall0, assuming it is a handler function OID.",
			 am_oid);
		datum = OidFunctionCall0(am_oid);
		routine = (TableAmRoutine *) DatumGetPointer(datum);
	}

	if (routine == NULL || !IsA(routine, TableAmRoutine))
		elog(ERROR, "table access method OID/handler %u did not return a valid TableAmRoutine struct",
			 am_oid);

	/*
	 * Assert that all required callbacks are present. That makes it a bit
	 * easier to keep AMs up to date, e.g. when forward porting them to a new
	 * major version.
	 */
	Assert(routine->scan_begin != NULL);
	Assert(routine->scan_end != NULL);
	Assert(routine->scan_rescan != NULL);
	Assert(routine->scan_getnextslot != NULL);

	Assert(routine->parallelscan_estimate != NULL);
	Assert(routine->parallelscan_initialize != NULL);
	Assert(routine->parallelscan_reinitialize != NULL);

	Assert(routine->index_fetch_begin != NULL);
	Assert(routine->index_fetch_reset != NULL);
	Assert(routine->index_fetch_end != NULL);
	Assert(routine->index_fetch_tuple != NULL);

	Assert(routine->tuple_fetch_row_version != NULL);
	Assert(routine->tuple_tid_valid != NULL);
	Assert(routine->tuple_get_latest_tid != NULL);
	Assert(routine->tuple_satisfies_snapshot != NULL);
	Assert(routine->index_delete_tuples != NULL);

	Assert(routine->tuple_insert != NULL);

	/*
	 * Could be made optional, but would require throwing error during
	 * parse-analysis.
	 */
	Assert(routine->tuple_insert_speculative != NULL);
	Assert(routine->tuple_complete_speculative != NULL);

	Assert(routine->multi_insert != NULL);
	Assert(routine->tuple_delete != NULL);
	Assert(routine->tuple_update != NULL);
	Assert(routine->tuple_lock != NULL);

	Assert(routine->relation_set_new_filelocator != NULL);
	Assert(routine->relation_nontransactional_truncate != NULL);
	Assert(routine->relation_copy_data != NULL);
	Assert(routine->relation_copy_for_cluster != NULL);
	Assert(routine->relation_vacuum != NULL);
	Assert(routine->scan_analyze_next_block != NULL);
	Assert(routine->scan_analyze_next_tuple != NULL);
	Assert(routine->index_build_range_scan != NULL);
	Assert(routine->index_validate_scan != NULL);

	Assert(routine->relation_size != NULL);
	Assert(routine->relation_needs_toast_table != NULL);

	Assert(routine->relation_estimate_size != NULL);

	Assert(routine->scan_sample_next_block != NULL);
	Assert(routine->scan_sample_next_tuple != NULL);

	return routine;
}

/* check_hook: validate new default_table_access_method */
bool
check_default_table_access_method(char **newval, void **extra, GucSource source)
{
	if (**newval == '\0')
	{
		GUC_check_errdetail("\"%s\" cannot be empty.",
							"default_table_access_method");
		return false;
	}

	if (strlen(*newval) >= NAMEDATALEN)
	{
		GUC_check_errdetail("\"%s\" is too long (maximum %d characters).",
							"default_table_access_method", NAMEDATALEN - 1);
		return false;
	}

	/*
	 * If we aren't inside a transaction, or not connected to a database, we
	 * cannot do the catalog access necessary to verify the method.  Must
	 * accept the value on faith.
	 */
	if (IsTransactionState() && MyDatabaseId != InvalidOid)
	{
		if (!OidIsValid(get_table_am_oid(*newval, true)))
		{
			/*
			 * When source == PGC_S_TEST, don't throw a hard error for a
			 * nonexistent table access method, only a NOTICE. See comments in
			 * guc.h.
			 */
			if (source == PGC_S_TEST)
			{
				ereport(NOTICE,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("table access method \"%s\" does not exist",
								*newval)));
			}
			else
			{
				GUC_check_errdetail("Table access method \"%s\" does not exist.",
									*newval);
				return false;
			}
		}
	}

	return true;
}
