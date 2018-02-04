#if MYSQL
#include <vlog/mysql/mysqliterator.h>
#include <vlog/mysql/mysqltable.h>

MySQLIterator::MySQLIterator(sql::Connection *con, string tableName,
                             const Literal &query,
                             const std::vector<string> &fieldsTable,
                             const std::vector<uint8_t> *sortingFieldIdx) {
    predid = query.getPredicate().getId();
    res = NULL;
    stmt = NULL;
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
    string cond = MySQLTable::literalConstraintsToSQLQuery(query, fieldsTable);
    if (cond != "") {
        sqlQuery += " WHERE " + cond;
    }

    string cond1 = MySQLTable::repeatedToSQLQuery(query, fieldsTable);
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

    stmt = con->createStatement();
    res = stmt->executeQuery(sqlQuery);
    if (res != NULL && res->first()) {
        hasNextValue = true;
    } else {
        hasNextValue = false;
    }
    hasNextChecked = true;
}

MySQLIterator::MySQLIterator(sql::Connection *con, string sqlQuery,
                             const Literal &query) {
    predid = query.getPredicate().getId();
    res = NULL;
    stmt = NULL;
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

    stmt = con->createStatement();
    res = stmt->executeQuery(sqlQuery);
    if (res != NULL && res->first()) {
        hasNextValue = true;
    } else {
        hasNextValue = false;
    }
    hasNextChecked = true;
}

bool MySQLIterator::hasNext() {
    if (hasNextChecked) {
	return hasNextValue;
    }
    if (isFirst || ! skipDuplicatedFirst) {
	hasNextValue = !res->isLast();
    } else {
	Term_t oldval = res->getUInt64(posFirstVar+1);
	bool stop = false;
	while (! stop && res->next()) {
	    if (res->getUInt64(posFirstVar + 1) != oldval) {
		stop = true;
	    }
	}
	// Must go back one value so that user can call "next()".
	// We read one too far here.
	res->previous();
	hasNextValue = stop;
    }
    hasNextChecked = true;
    // LOG(DEBUGL) << "MySQLIterator::hasNext(): skipDuplicatedFirst = " << skipDuplicatedFirst << ", returns " << hasNextValue;
    return hasNextValue;
}

void MySQLIterator::next() {
    if (! hasNextChecked) {
	LOG(ERRORL) << "MySQLIterator::next called without hasNext check";
	throw 10;
    }
    // LOG(DEBUGL) << "MySQLIterator::next()";
    if (isFirst) {
        isFirst = false;
    } else if (!res->next()) {
	throw 10;
    }
    hasNextChecked = false;
}

void MySQLIterator::clear() {
    if (res)
        delete res;
    if (stmt)
        delete stmt;
    res = NULL;
    stmt = NULL;
}

void MySQLIterator::skipDuplicatedFirstColumn() {
    if (posFirstVar != -1) {
	skipDuplicatedFirst = true;
    }
    LOG(DEBUGL) << "posFirstVar = " << posFirstVar << ", skipDuplicatedFirst = " << skipDuplicatedFirst;
}

Term_t MySQLIterator::getElementAt(const uint8_t p) {
    // LOG(DEBUGL) << "MySQLIterator::getElementAt()";
    return res->getUInt64(p + 1);
}

MySQLIterator::~MySQLIterator() {
    clear();
}
#endif
