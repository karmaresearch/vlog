#if ODBC
#include <vlog/odbc/odbctable.h>

#include <unistd.h>
#include <string>


SQLRETURN ODBCTable::check(SQLRETURN rc, std::string msg) {
    if (SQL_SUCCEEDED(rc)) {
	return rc;
    }
    if (rc == SQL_NO_DATA) {
	return rc;
    }
    LOG(ERRORL) << "Failed ODBC call: " << msg;
    throw 10;
}

ODBCTable::ODBCTable(PredId_t predid, std::string user, std::string pwd, std::string dbname,
                       std::string tablename, std::string tablefields, EDBLayer *layer) :
	SQLTable(predid, tablename, tablefields, layer) {

    check(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env), "allocate environment handle");
    check(SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER) SQL_OV_ODBC3, 0), "setting ODBC version");
    check(SQLAllocHandle(SQL_HANDLE_DBC, env, &con), "allocate connection handle");
    check(SQLConnectA(con, (SQLCHAR *) dbname.c_str(), SQL_NTS, (SQLCHAR *) user.c_str(), SQL_NTS, (SQLCHAR *) pwd.c_str(), SQL_NTS), "connect");
}

uint64_t ODBCTable::getSizeFromDB(const std::string &query) {

    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();

    SQLHANDLE stmt;
    check(SQLAllocHandle(SQL_HANDLE_STMT, con, &stmt), "allocate statement handle");
    check(SQLExecDirectA(stmt, (SQLCHAR *) query.c_str(), SQL_NTS), "execute query");
    SQLRETURN res = SQLFetch(stmt);
    check(res, "fetch result");
    SQLLEN numBytes;
    SQLUBIGINT result;
    check(SQLGetData(stmt, 1, SQL_C_UBIGINT, &result, sizeof(SQLUBIGINT), &numBytes), "get result");
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);

    std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    LOG(DEBUGL) << "SQL Query: " << query << " took " << sec.count();
    return (uint64_t) result;
}

void ODBCTable::executeQuery(const std::string &query, SegmentInserter *inserter) {

    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();

    SQLHANDLE stmt;
    check(SQLAllocHandle(SQL_HANDLE_STMT, con, &stmt), "allocate statement handle");
    check(SQLExecDirectA(stmt, (SQLCHAR *) query.c_str(), SQL_NTS), "execute query");
    SQLRETURN res;
    while (SQL_SUCCEEDED(res = check(SQLFetch(stmt), "fetch result"))) {
	uint64_t row[256];
	for (int i = 0; i < arity; i++) {
	    SQLLEN indicator;
	    char buf[16384];
	    res = SQLGetData(stmt, i+1, SQL_C_CHAR,
                         buf, 16384, &indicator);
	    if (SQL_SUCCEEDED(res)) {
		/* Handle null columns */
		if (indicator == SQL_NULL_DATA) strcpy(buf, "NULL");
		layer->getOrAddDictNumber(buf, strlen(buf), row[i]);
	    } else {
		check(res, "get data");
		LOG(ERRORL) << "Oops ...";
	    }
	}
	inserter->addRow(row);
    }

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);

    std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    LOG(DEBUGL) << "SQL Query: " << query << " took " << sec.count();
}

ODBCTable::~ODBCTable() {
    SQLDisconnect(con);
    SQLFreeHandle(SQL_HANDLE_DBC, con);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}
#endif
