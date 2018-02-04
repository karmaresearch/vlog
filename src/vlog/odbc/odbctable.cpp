#if ODBC
#include <vlog/odbc/odbctable.h>
#include <vlog/odbc/odbciterator.h>

#include <unistd.h>
#include <sstream>
#include <string>


void ODBCTable::check(SQLRETURN rc, string msg) {
    if (SQL_SUCCEEDED(rc)) {
	return;
    }
    if (rc == SQL_NO_DATA) {
	return;
    }
    LOG(ERRORL) << "Failed ODBC call: " << msg;
    throw 10;
}

ODBCTable::ODBCTable(string user, string pwd, string dbname,
                       string tablename, string tablefields) {

    this->tablename = tablename;
    check(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env), "allocate environment handle");
    check(SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER) SQL_OV_ODBC3, 0), "setting ODBC version");
    check(SQLAllocHandle(SQL_HANDLE_DBC, env, &con), "allocate connection handle");
    check(SQLConnectA(con, (SQLCHAR *) dbname.c_str(), SQL_NTS, (SQLCHAR *) user.c_str(), SQL_NTS, (SQLCHAR *) pwd.c_str(), SQL_NTS), "connect");

    //Extract fields
    std::stringstream ss(tablefields);
    std::string item;
    while (std::getline(ss, item, ',')) {
        this->fieldTables.push_back(item);
    }
}

// If valuesToFilter is larger than this, use a temporary table instead of a test on the individual values
#define TEMP_TABLE_THRESHOLD (2*3*4*5*7*11)

void ODBCTable::query(QSQQuery *query, TupleTable *outputTable,
                       std::vector<uint8_t> *posToFilter,
                       std::vector<Term_t> *valuesToFilter) {
    const Literal *l = query->getLiteral();
    const uint8_t npos = query->getNPosToCopy();
    uint8_t *pos = query->getPosToCopy();
    uint64_t row[npos];
    EDBIterator *iter;

    if (posToFilter == NULL || posToFilter->size() == 0) {
	iter = getIterator(*l);
    } else {
	//Create first part of query.
	string sqlQuery = "SELECT * FROM " + tablename;
	if (valuesToFilter->size() > TEMP_TABLE_THRESHOLD) {
	    // sqlQuery += " t1";
	}
	sqlQuery += " WHERE ";
	string cond = literalConstraintsToSQLQuery(*l, fieldTables);
	if (cond != "") {
	    sqlQuery += cond;
	}

	string cond1 = repeatedToSQLQuery(*l, fieldTables);
	if (cond1 != "") {
	    if (cond != "") {
		sqlQuery += " AND ";
	    }
	    sqlQuery += cond1;
	}
     
	if (cond1 != "" || cond != "") {
	    sqlQuery += " AND ";
	}

	if (valuesToFilter->size() > TEMP_TABLE_THRESHOLD) {
	    // Somewhat arbitrary threshold
	    // Create temporary table
	    string sqlCreateTable = "CREATE TEMPORARY TABLE temp (";
	    for (int i = 0; i < posToFilter->size(); i++) {
		if (i > 0) {
		    sqlCreateTable += ", ";
		}
		sqlCreateTable += "x" + to_string(i) + " BIGINT";
	    }
	    sqlCreateTable += ", PRIMARY KEY (";
	    for (int i = 0; i < posToFilter->size(); i++) {
		if (i > 0) {
		    sqlCreateTable += ", ";
		}
		sqlCreateTable += "x" + to_string(i);
	    }
	    sqlCreateTable += ")";
	    sqlCreateTable += ")";
	    SQLHANDLE stmt;
	    check(SQLAllocHandle(SQL_HANDLE_STMT, con, &stmt), "allocate statement handle");
	    LOG(DEBUGL) << "SQL create temp table: " << sqlCreateTable;
	    check(SQLExecDirectA(stmt, (SQLCHAR *) sqlCreateTable.c_str(), SQL_NTS), "create temp table");
	    SQLFreeHandle(SQL_HANDLE_STMT, stmt);

	    // Insert values into table
	    check(SQLAllocHandle(SQL_HANDLE_STMT, con, &stmt), "allocate statement handle");
	    LOG(DEBUGL) << "START TRANSACTION";
	    check(SQLExecDirectA(stmt, (SQLCHAR *) "START TRANSACTION", SQL_NTS), "start transaction");
	    SQLFreeHandle(SQL_HANDLE_STMT, stmt);

	    for (std::vector<Term_t>::iterator itr = valuesToFilter->begin();
		    itr != valuesToFilter->end();) {
		string addition = "INSERT INTO temp VALUES (";
		for (int i = 0; i < posToFilter->size(); i++, itr++) {
		    string pref = i > 0 ? ", " : "";
		    addition += pref + to_string(*itr);
		}
		addition += ")";
		LOG(DEBUGL) << "SQL insert into temp table: " << addition;
		check(SQLAllocHandle(SQL_HANDLE_STMT, con, &stmt), "allocate statement handle");
		check(SQLExecDirectA(stmt, (SQLCHAR *) addition.c_str(), SQL_NTS), "addition");
		SQLFreeHandle(SQL_HANDLE_STMT, stmt);
	    }

	    check(SQLAllocHandle(SQL_HANDLE_STMT, con, &stmt), "allocate statement handle");
	    LOG(DEBUGL) << "COMMIT";
	    check(SQLExecDirectA(stmt, (SQLCHAR *) "COMMIT", SQL_NTS), "commit");
	    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
	    
	    /*
	    sqlCreateTable = "ALTER TABLE temp ADD CONSTRAINT pkey PRIMARY KEY (";
	    for (int i = 0; i < posToFilter->size(); i++) {
		if (i > 0) {
		    sqlCreateTable += ", ";
		}
		sqlCreateTable += "x" + to_string(i);
	    }
	    sqlCreateTable += ")";
	    check(SQLAllocHandle(SQL_HANDLE_STMT, con, &stmt), "allocate statement handle");
	    LOG(DEBUGL) << "alter table: add primary key: " << sqlCreateTable;
	    check(SQLExecDirectA(stmt, (SQLCHAR *) sqlCreateTable.c_str(), SQL_NTS), "alter table");
	    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
	    */

	    // Finish the query.
	    sqlQuery += "EXISTS (SELECT * FROM temp WHERE ";

	    for (int i = 0; i < posToFilter->size(); i++) {
		if (i != 0) {
		    sqlQuery += " AND ";
		}
		sqlQuery += tablename + "." + fieldTables[posToFilter->at(i)] + " = temp.x" + to_string(i);
	    }

	    sqlQuery += ")";
	} else {
	    bool first = true;
	    sqlQuery += "(";
	    for (std::vector<Term_t>::iterator itr = valuesToFilter->begin();
		    itr != valuesToFilter->end();) {
		if (! first) {
		    sqlQuery += " OR ";
		}
		sqlQuery += "(";
		for (int i = 0; i < posToFilter->size(); i++, itr++) {
		    string pref = i > 0 ? " AND " : "";
		    sqlQuery += pref + fieldTables[posToFilter->at(i)] + " = " + to_string(*itr);
		}
		sqlQuery += ")";
		first = false;
	    }
	    sqlQuery += ")";
	}

	// sqlQuery += " ORDER BY " + fieldTables[0] + ", " + fieldTables[1];	// TO BE REMOVED

	iter = new ODBCIterator(con, sqlQuery, *l);
    }

    int count = 0;
    while (iter->hasNext()) {
	iter->next();
	for (int i = 0; i < npos; i++) {
	    row[i] = iter->getElementAt(pos[i]);
	}
	outputTable->addRow(row);
	count++;
    }
    iter->clear();
    delete iter;
    LOG(DEBUGL) << "query gave " << count << " results";

    if (valuesToFilter != NULL && valuesToFilter->size() > TEMP_TABLE_THRESHOLD) {
	SQLHANDLE stmt;
	check(SQLAllocHandle(SQL_HANDLE_STMT, con, &stmt), "allocate statement handle");
	LOG(DEBUGL) << "DROP TABLE temp";
	check(SQLExecDirectA(stmt, (SQLCHAR *) "DROP TABLE temp", SQL_NTS), "drop temp table");
	SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    }
}

uint64_t ODBCTable::getSize() {

    string query = "SELECT COUNT(*) as c from " + tablename;

    LOG(DEBUGL) << "SQL Query: " << query;

    SQLHANDLE stmt;
    check(SQLAllocHandle(SQL_HANDLE_STMT, con, &stmt), "allocate statement handle");
    check(SQLExecDirectA(stmt, (SQLCHAR *) query.c_str(), SQL_NTS), "execute query");
    SQLRETURN res = SQLFetch(stmt);
    check(res, "fetch result");
    SQLLEN numBytes;
    SQLUBIGINT result;
    check(SQLGetData(stmt, 1, SQL_C_UBIGINT, &result, sizeof(SQLUBIGINT), &numBytes), "get result");
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    return (uint64_t) result;
}

size_t ODBCTable::getCardinality(const Literal &q) {
    string query = "SELECT COUNT(*) as c from " + tablename;

    string cond = literalConstraintsToSQLQuery(q, fieldTables);
    if (cond != "") {
        query += " WHERE " + cond;
    }

    LOG(DEBUGL) << "SQL Query: " << query;
    SQLHANDLE stmt;
    check(SQLAllocHandle(SQL_HANDLE_STMT, con, &stmt), "allocate statement handle");
    check(SQLExecDirectA(stmt, (SQLCHAR *) query.c_str(), SQL_NTS), "execute query");
    SQLRETURN res = SQLFetch(stmt);
    check(res, "fetch result");
    SQLLEN numBytes;
    SQLUBIGINT result;
    check(SQLGetData(stmt, 1, SQL_C_UBIGINT, &result, sizeof(SQLUBIGINT), &numBytes), "get result");
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    return (size_t) result;
}

size_t ODBCTable::getCardinalityColumn(const Literal &q, uint8_t posColumn) {
    
    string query = "SELECT COUNT(DISTINCT " + fieldTables[posColumn] + ") as c from " + tablename;

    string cond = literalConstraintsToSQLQuery(q, fieldTables);
    if (cond != "") {
        query += " WHERE " + cond;
    }

    LOG(DEBUGL) << "SQL Query: " << query;
    SQLHANDLE stmt;
    check(SQLAllocHandle(SQL_HANDLE_STMT, con, &stmt), "allocate statement handle");
    check(SQLExecDirectA(stmt, (SQLCHAR *) query.c_str(), SQL_NTS), "execute query");
    SQLRETURN res = SQLFetch(stmt);
    check(res, "fetch result");
    SQLLEN numBytes;
    SQLUBIGINT result;
    check(SQLGetData(stmt, 1, SQL_C_UBIGINT, &result, sizeof(SQLUBIGINT), &numBytes), "get result");
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    return (size_t) result;
}

bool ODBCTable::isEmpty(const Literal &q, std::vector<uint8_t> *posToFilter,
                         std::vector<Term_t> *valuesToFilter) {

    if (posToFilter == NULL) {
        return getCardinality(q) == 0;
    }

    string cond = literalConstraintsToSQLQuery(q, fieldTables);
    for (int i = 0; i < posToFilter->size(); i++) {
	if (cond != "") {
	    cond += " and ";
	}
	cond += fieldTables[posToFilter->at(i)] + "=" +
		to_string(valuesToFilter->at(i));
    }

    string query = "SELECT COUNT(*) as c from " + tablename;

    if (cond != "") {
	query += " WHERE " + cond;
    }

    LOG(DEBUGL) << "SQL Query: " << query;
    SQLHANDLE stmt;
    check(SQLAllocHandle(SQL_HANDLE_STMT, con, &stmt), "allocate statement handle");
    check(SQLExecDirectA(stmt, (SQLCHAR *) query.c_str(), SQL_NTS), "execute query");
    SQLRETURN res = SQLFetch(stmt);
    check(res, "fetch result");
    SQLLEN numBytes;
    SQLUBIGINT result;
    check(SQLGetData(stmt, 1, SQL_C_UBIGINT, &result, sizeof(SQLUBIGINT), &numBytes), "get result");
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    return result == 0;
}

EDBIterator *ODBCTable::getIterator(const Literal &query) {
    return new ODBCIterator(con, tablename, query, fieldTables, NULL);
}

EDBIterator *ODBCTable::getSortedIterator(const Literal &query,
        const std::vector<uint8_t> &fields) {
    return new ODBCIterator(con, tablename, query, fieldTables, &fields);
}

bool ODBCTable::getDictNumber(const char *text, const size_t sizeText,
                               uint64_t &id) {
    string query = "SELECT id from revdict where value='" + string(text, sizeText) + "'";
    SQLHANDLE stmt;
    check(SQLAllocHandle(SQL_HANDLE_STMT, con, &stmt), "allocate statement handle");
    check(SQLExecDirectA(stmt, (SQLCHAR *) query.c_str(), SQL_NTS), "execute query");
    SQLRETURN res = SQLFetch(stmt);
    if (SQL_SUCCEEDED(res)) {
	SQLLEN numBytes;
	SQLUBIGINT result;
	check(SQLGetData(stmt, 1, SQL_C_UBIGINT, &result, sizeof(SQLUBIGINT), &numBytes), "get result");
	id = (uint64_t) result;
    }
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    return SQL_SUCCEEDED(res);

}

bool ODBCTable::getDictText(const uint64_t id, char *text) {
    string query = "SELECT value from dict where id=" + to_string(id);
    SQLHANDLE stmt;
    check(SQLAllocHandle(SQL_HANDLE_STMT, con, &stmt), "allocate statement handle");
    check(SQLExecDirectA(stmt, (SQLCHAR *) query.c_str(), SQL_NTS), "execute query");
    SQLRETURN res = SQLFetch(stmt);
    if (SQL_SUCCEEDED(res)) {
	SQLLEN numBytes;
	check(SQLGetData(stmt, 1, SQL_C_CHAR, text, 8192, &numBytes), "get result");
	text[numBytes] = 0;
    }
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    return SQL_SUCCEEDED(res);
}

uint64_t ODBCTable::getNTerms() {
    string query = "SELECT COUNT(*) as c from dict";
    SQLHANDLE stmt;
    check(SQLAllocHandle(SQL_HANDLE_STMT, con, &stmt), "allocate statement handle");
    check(SQLExecDirectA(stmt, (SQLCHAR *) query.c_str(), SQL_NTS), "execute query");
    SQLRETURN res = SQLFetch(stmt);
    check(res, "fetch result");
    SQLLEN numBytes;
    SQLUBIGINT result;
    check(SQLGetData(stmt, 1, SQL_C_UBIGINT, &result, sizeof(SQLUBIGINT), &numBytes), "get result");
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    return (uint64_t) result;
}

ODBCTable::~ODBCTable() {
    SQLDisconnect(con);
    SQLFreeHandle(SQL_HANDLE_DBC, con);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}
#endif
