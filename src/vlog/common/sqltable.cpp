#include <sstream>
#include <string>

#include <vlog/sqltable.h>



string SQLTable::literalConstraintsToSQLQuery(const Literal &q,
        const std::vector<string> &fieldTables) {
    string cond = "";
    int idxField = 0;
    while (idxField < q.getTupleSize()) {
        if (!q.getTermAtPos(idxField).isVariable()) {
            if (cond != "") {
                cond += " and ";
            }
            cond += fieldTables[idxField] + "=" +
                    to_string(q.getTermAtPos(idxField).getValue());
        }
        idxField++;
    }
    return cond;
}

string SQLTable::repeatedToSQLQuery(const Literal &q,
        const std::vector<string> &fieldTables) {
    string cond = "";
    std::vector<std::pair<uint8_t, uint8_t>> repeated = q.getRepeatedVars();
    for (int i = 0; i < repeated.size(); i++) {
	if (i != 0) {
	    cond += " and ";
	}
	cond += fieldTables[repeated[i].first] + "=" + fieldTables[repeated[i].second];
    }
    return cond;
}

size_t SQLTable::estimateCardinality(const Literal &query) {
    //TODO: This should be improved
    return getCardinality(query);
}

void SQLTable::releaseIterator(EDBIterator *itr) {
    delete itr;
}

