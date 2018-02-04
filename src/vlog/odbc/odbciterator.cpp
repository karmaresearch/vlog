#if ODBC
#include <vlog/odbc/odbciterator.h>
#include <vlog/odbc/odbctable.h>

ODBCIterator::ODBCIterator(SQLHANDLE con, string tableName,
                             const Literal &query,
                             const std::vector<string> &fieldsTable,
                             const std::vector<uint8_t> *sortingFieldIdx) {
    predid = query.getPredicate().getId();
    isFirst = true;
    posFirstVar = -1;
    skipDuplicatedFirst = false;

    string projections = "*";
    /*if (query.getNVars() == query.getTupleSize()) {
        projections = "*";
    } else {
        for (int i = 0; i < query.getTupleSize(); ++i) {
            if (query.getTermAtPos(i).isVariable()) {
                if (projections != "")
                    projections += ",";
                projections += fieldsTable[i];
            }
        }
    }*/

    //Read the query
    string sqlQuery = "SELECT " + projections + " FROM " + tableName;
    string cond = ODBCTable::literalConstraintsToSQLQuery(query, fieldsTable);
    if (cond != "") {
        sqlQuery += " WHERE " + cond;
    }

    string cond1 = ODBCTable::repeatedToSQLQuery(query, fieldsTable);
    if (cond1 != "") {
	if (cond != "") {
	    sqlQuery += " AND ";
	}
	sqlQuery += cond1;
    }

    //set the order clause
    if (sortingFieldIdx != NULL && sortingFieldIdx->size() > 0) {
        string sortString = " ORDER BY ";
        for (int i = 0; i < sortingFieldIdx->size(); ++i) {
            if (i != 0)
                sortString += ",";
            //Cannot consider the constants
            int var = sortingFieldIdx->at(i);
            int j = 0;
            int idxVar = -1;
            for(; j < query.getTupleSize(); ++j) {
                if (query.getTermAtPos(j).isVariable()) {
                    idxVar++;
                }
                if (idxVar == var)
                    break;
            }
            sortString += fieldsTable[j];
	    if (posFirstVar == -1) {
		posFirstVar = j;
	    }
        }
        sqlQuery += sortString;
    }

    int count = 0;
    for (int i = 0; i < query.getTupleSize(); ++i) {
	if (query.getTermAtPos(i).isVariable()) {
	    count++;
	    if (posFirstVar == -1) {
		posFirstVar = i;
	    }
	}
    }
    if (count <= 1) {
	// If there is at most one variable, reset posFirstVar to -1, because in that case 
	// skipDuplicatedFirstColumn can be a no-op.
	posFirstVar = -1;
    }

    LOG(DEBUGL) << "SQL query: " << sqlQuery;

    ODBCTable::check(SQLAllocHandle(SQL_HANDLE_STMT, con, &stmt), "allocate statement handle");
    ODBCTable::check(SQLExecDirectA(stmt, (SQLCHAR *) sqlQuery.c_str(), SQL_NTS), "execute query");
    SQLNumResultCols(stmt, &columns);
    indicator = new SQLLEN[columns];
    values = new SQLUBIGINT[columns];
    for (int i = 0; i < columns; i++) {
	ODBCTable::check(SQLBindCol(stmt, i + 1, SQL_C_UBIGINT, &values[i], sizeof(SQLUBIGINT), &indicator[i]), "SQLBindCol");
    }
    SQLRETURN ret = SQLFetch(stmt);
    ODBCTable::check(ret, "SQLFetch");
    hasNextValue = ret != SQL_NO_DATA;
    hasNextChecked = true;
}


ODBCIterator::ODBCIterator(SQLHANDLE con, string sqlQuery,
                             const Literal &query) {
    predid = query.getPredicate().getId();
    isFirst = true;
    posFirstVar = -1;
    skipDuplicatedFirst = false;

    int count = 0;
    for (int i = 0; i < query.getTupleSize(); ++i) {
	if (query.getTermAtPos(i).isVariable()) {
	    count++;
	    if (posFirstVar == -1) {
		posFirstVar = i;
	    }
	}
    }
    if (count <= 1) {
	// If there is at most one variable, reset posFirstVar to -1, because in that case 
	// skipDuplicatedFirstColumn can be a no-op.
	posFirstVar = -1;
    }

    LOG(DEBUGL) << "SQL query: " << sqlQuery;

    ODBCTable::check(SQLAllocHandle(SQL_HANDLE_STMT, con, &stmt), "allocate statement handle");
    ODBCTable::check(SQLExecDirectA(stmt, (SQLCHAR *) sqlQuery.c_str(), SQL_NTS), "execute query");
    SQLNumResultCols(stmt, &columns);
    indicator = new SQLLEN[columns];
    values = new SQLUBIGINT[columns];
    for (int i = 0; i < columns; i++) {
	ODBCTable::check(SQLBindCol(stmt, i + 1, SQL_C_UBIGINT, &values[i], sizeof(SQLUBIGINT), &indicator[i]), "SQLBindCol");
    }
    SQLRETURN ret = SQLFetch(stmt);
    ODBCTable::check(ret, "SQLFetch");
    hasNextValue = ret != SQL_NO_DATA;
    hasNextChecked = true;
}



bool ODBCIterator::hasNext() {
    if (hasNextChecked) {
	return hasNextValue;
    }
    if (isFirst || ! skipDuplicatedFirst) {
	SQLRETURN ret = SQLFetch(stmt);
	ODBCTable::check(ret, "SQLFetch");
	hasNextValue = ret != SQL_NO_DATA;
    } else {
	Term_t oldval = getElementAt(posFirstVar);
	bool stop = false;
	while (! stop) {
	    SQLRETURN ret = SQLFetch(stmt);
	    ODBCTable::check(ret, "SQLFetch");
	    hasNextValue = ret != SQL_NO_DATA;
	    if (hasNextValue) {
		if (getElementAt(posFirstVar) != oldval) {
		    stop = true;
		}
	    } else {
		break;
	    }
	}
	hasNextValue = stop;
    }
    hasNextChecked = true;
    // LOG(DEBUGL) << "ODBCIterator::hasNext(): skipDuplicatedFirst = " << skipDuplicatedFirst << ", returns " << hasNextValue;
    return hasNextValue;
}

void ODBCIterator::next() {
    if (! hasNextChecked) {
	LOG(ERRORL) << "ODBCIterator::next called without hasNext check";
	throw 10;
    }
    // LOG(DEBUGL) << "ODBCIterator::next()";
    if (isFirst) {
        isFirst = false;
    }
    hasNextChecked = false;
}

void ODBCIterator::clear() {
    if (indicator != NULL) {
	delete indicator;
	delete values;
	SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    }
    indicator = NULL;
    values = NULL;
}

void ODBCIterator::skipDuplicatedFirstColumn() {
    if (posFirstVar != -1) {
	skipDuplicatedFirst = true;
    }
}

Term_t ODBCIterator::getElementAt(const uint8_t p) {
    // LOG(DEBUGL) << "ODBCIterator::getElementAt()";
    if (indicator[p] != SQL_NULL_DATA) {
	return values[p];
    }
    throw 10;
}

ODBCIterator::~ODBCIterator() {
    clear();
}
#endif
