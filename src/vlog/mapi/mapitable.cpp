#if MAPI
#include <vlog/mapi/mapitable.h>

#include <unistd.h>
#include <string>


MapiHdl MAPITable::doquery(Mapi dbh, string q) { 
    MapiHdl ret = NULL; 

    if ((ret = mapi_query(dbh, q.c_str())) == NULL || mapi_error(dbh) != MOK)  {
	if (ret != NULL) { 
	    mapi_explain_query(ret, stderr); 
	    do { 
		if (mapi_result_error(ret) != NULL) 
		    mapi_explain_result(ret, stderr); 
	    } while (mapi_next_result(ret) == 1); 
	    mapi_close_handle(ret); 
	    mapi_destroy(dbh); 
	} else if (dbh != NULL) { 
	    mapi_explain(dbh, stderr); 
	    mapi_destroy(dbh); 
	}
	throw 10;
    }

    return(ret); 
}

MAPITable::MAPITable(PredId_t predid, string host, int port,
			string user, string pwd, string dbname,
                        string tablename, string tablefields,
		        EDBLayer *layer) : SQLTable(predid, tablename, tablefields, layer) {
    con = mapi_connect(host.c_str(), port, user.c_str(), pwd.c_str(), "sql", dbname.c_str()); 
    if (mapi_error(con)) {
	mapi_explain(con, stderr); 
	mapi_destroy(con); 
	throw 10;
    }
}

void MAPITable::executeQuery(const std::string &query, SegmentInserter *inserter) {

    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();

    MapiHdl handle = doquery(con, query);
    // Cache results at client side, fetching them in one go.
    mapi_int64 nrows = mapi_fetch_all_rows(handle);
    int nfields;
    while ((nfields = mapi_fetch_row(handle)) > 0) {
	if (nfields != arity) {
	    LOG(ERRORL) << "Number of fields in query result " << nfields << ", arity should be " << arity << ", this result is ignored";
	    continue;
	}
	uint64_t row[128];
	for (int i = 0; i < nfields; i++) {
	    char *field = mapi_fetch_field(handle, i);
	    layer->getOrAddDictNumber(field, strlen(field), row[i]);
	}
	inserter->addRow(row);
    }

    std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    LOG(DEBUGL) << "SQL Query: " << query << " took " << sec.count();
}

uint64_t MAPITable::getSizeFromDB(const std::string &query) {

    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();

    MapiHdl handle = doquery(con, query);
    mapi_fetch_row(handle);
    char *res = mapi_fetch_field(handle, 0);
    char *p;
    uint64_t result = (uint64_t) strtoll(res, &p, 10);
    mapi_close_handle(handle);

    std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    LOG(DEBUGL) << "SQL Query: " << query << " took " << sec.count();

    return result;
}

MAPITable::~MAPITable() {
    mapi_destroy(con);
}
#endif
