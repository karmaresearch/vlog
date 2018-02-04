#if MAPI
#include <vlog/mapi/mapiiterator.h>
#include <vlog/mapi/mapitable.h>

#include <stdio.h>

MAPIIterator::MAPIIterator(Mapi con, string tableName,
	const Literal &query,
	const std::vector<string> &fieldsTable,
	const std::vector<uint8_t> *sortingFieldIdx) {
    predid = query.getPredicate().getId();
    isFirst = true;
    posFirstVar = -1;
    skipDuplicatedFirst = false;

    string projections = "*";
    /*
    if (query.getNVars() == query.getTupleSize()) {
	projections = "*";
    } else {
	for (int i = 0; i < query.getTupleSize(); ++i) {
	    if (query.getTermAtPos(i).isVariable()) {
		if (projections != "")
		    projections += ",";
		projections += fieldsTable[i];
	    }
	}
    }
    */

    //Read the query
    string sqlQuery = "SELECT " + projections + " FROM " + tableName;
    string cond = SQLTable::literalConstraintsToSQLQuery(query, fieldsTable);
    if (cond != "") {
	sqlQuery += " WHERE " + cond;
    }

    string cond1 = SQLTable::repeatedToSQLQuery(query, fieldsTable);
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

    LOG(DEBUGL) << "SQL query (MAPIIterator): " << sqlQuery;

    handle = MAPITable::doquery(con, sqlQuery);

    columns = fieldsTable.size();
    values = new uint64_t[columns];
    for (int i = 0; i < columns; i++) {
	mapi_bind_var(handle, i, MAPI_ULONGLONG, &values[i]); 
    }
    hasNextChecked = false;
}


MAPIIterator::MAPIIterator(Mapi con, string sqlQuery,
	const std::vector<string> &fieldsTable,
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

    LOG(DEBUGL) << "SQL query (MAPIIterator): " << sqlQuery;

    handle = MAPITable::doquery(con, sqlQuery);

    columns = fieldsTable.size();
    values = new uint64_t[columns];
    for (int i = 0; i < columns; i++) {
	mapi_bind_var(handle, i, MAPI_ULONGLONG, &values[i]); 
    }
    hasNextChecked = false;
}



bool MAPIIterator::hasNext() {
    if (hasNextChecked) {
	return hasNextValue;
    }
    if (isFirst || ! skipDuplicatedFirst) {
	hasNextValue = mapi_fetch_row(handle) != 0;
    } else {
	Term_t oldval = getElementAt(posFirstVar);
	bool stop = false;
	while (! stop) {
	    hasNextValue = mapi_fetch_row(handle) != 0;
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
    // LOG(DEBUGL) << "MAPIIterator::hasNext(): skipDuplicatedFirst = " << skipDuplicatedFirst << ", returns " << hasNextValue;
    return hasNextValue;
}

void MAPIIterator::next() {
    if (! hasNextChecked) {
	LOG(ERRORL) << "MAPIIterator::next called without hasNext check";
	throw 10;
    }
    // LOG(DEBUGL) << "MAPIIterator::next()";
    if (isFirst) {
	isFirst = false;
    }
    hasNextChecked = false;
}

void MAPIIterator::clear() {
    if (handle != NULL) {
	mapi_close_handle(handle);
	handle = NULL;
    }
    if (values != NULL) {
	delete values;
	values = NULL;
    }
}

void MAPIIterator::skipDuplicatedFirstColumn() {
    if (posFirstVar != -1) {
	skipDuplicatedFirst = true;
    }
}

Term_t MAPIIterator::getElementAt(const uint8_t p) {
    // LOG(DEBUGL) << "MAPIIterator::getElementAt()";
    return values[p];
}

MAPIIterator::~MAPIIterator() {
    clear();
}
#endif
