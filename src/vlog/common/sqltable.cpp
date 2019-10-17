#include <sstream>
#include <string>

#include <vlog/sqltable.h>
#include <vlog/inmemory/inmemorytable.h>

SQLTable::SQLTable(PredId_t predid, std::string name, std::string fieldnames, EDBLayer *layer) :
    predid(predid), tablename(name), layer(layer) {
    std::stringstream ss(fieldnames);
    std::string item;
    while (std::getline(ss, item, ',')) {
        this->fieldTables.push_back(item);
    }
    arity = fieldTables.size();
}

std::string SQLTable::mapToField(uint64_t value, uint8_t ind) {
    std::string s = layer->getDictText(value);
    int pos = s.find("'");
    if (pos == std::string::npos) {
	return "'" + s + "'";
    }
    int oldpos = 0;
    std::string retval = "'";
    while (pos != std::string::npos) {
	retval += s.substr(oldpos, pos+1) + "'";
	oldpos = pos+1;
	pos = s.find("'", oldpos);
    }
    retval += s.substr(oldpos, s.size()) + "'";
    return retval;
}

std::string SQLTable::literalConstraintsToSQLQuery(const Literal &q) {
    std::string cond = "";
    int idxField = 0;
    while (idxField < q.getTupleSize()) {
        if (!q.getTermAtPos(idxField).isVariable()) {
            if (!cond.empty()) {
                cond += " and ";
            }
            cond += fieldTables[idxField] + "=" +
                    mapToField(q.getTermAtPos(idxField).getValue(), idxField);
        }
        idxField++;
    }
    return cond;
}

std::string SQLTable::repeatedToSQLQuery(const Literal &q) {
    std::string cond = "";
    std::vector<std::pair<uint8_t, uint8_t>> repeated = q.getRepeatedVars();
    for (int i = 0; i < repeated.size(); i++) {
	if (i != 0) {
	    cond += " and ";
	}
	cond += fieldTables[repeated[i].first] + "=" + fieldTables[repeated[i].second];
    }
    return cond;
}

size_t SQLTable::getCardinality(const Literal &q) {
    LOG(DEBUGL) << "getCardinality: query = " << q.tostring(NULL, layer);
    std::string query = "SELECT COUNT(*) from ";
    std::string cond = literalConstraintsToSQLQuery(q);
    std::string cond1 = repeatedToSQLQuery(q);
    if (!cond.empty() || !cond1.empty()) {
	query += "( SELECT DISTINCT * FROM " + tablename;
	query += " WHERE " + cond;
	if (!cond1.empty()) {
	    if (!cond.empty()) {
		query += " and ";
	    }
	    query += cond1;
	}
	query += ") Temp";
    } else {
	query += tablename;
    }
    size_t result = getSizeFromDB(query);
    return result;
}

size_t SQLTable::getCardinalityColumn(const Literal &q, uint8_t posColumn) {
    LOG(DEBUGL) << "getCardinalityColumn: col = " << (int)posColumn << ", query = " << q.tostring(NULL, layer);
    std::string query = "SELECT COUNT(DISTINCT " + fieldTables[posColumn] + ") as c from " + tablename;
    std::string cond = literalConstraintsToSQLQuery(q);
    std::string cond1 = repeatedToSQLQuery(q);
    if (!cond.empty() || !cond1.empty()) {
	query += " WHERE " + cond;
	if (!cond1.empty()) {
	    if (!cond.empty()) {
		query += " and ";
	    }
	    query += cond1;
	}
    }
    size_t result = getSizeFromDB(query);
    return result;
}

bool SQLTable::isEmpty(const Literal &q, std::vector<uint8_t> *posToFilter,
                         std::vector<Term_t> *valuesToFilter) {

    LOG(DEBUGL) << "isEmpty: query = " << q.tostring(NULL, layer);
    if (posToFilter == NULL) {
        return getCardinality(q) == 0;
    }

    std::string query = "SELECT COUNT(*) as c from ";

    std::string cond = literalConstraintsToSQLQuery(q);
    std::string cond1 = repeatedToSQLQuery(q);
    if (cond.empty()) {
	cond = cond1;
    } else if (!cond1.empty()) {
	cond += " and " + cond1;
    }
    for (int i = 0; i < posToFilter->size(); i++) {
        if (!cond.empty()) {
            cond += " and ";
        }
        cond += fieldTables[posToFilter->at(i)] + "=" +
                mapToField(valuesToFilter->at(i), posToFilter->at(i));
    }

    if (!cond.empty()) {
        query += "( SELECT DISTINCT * FROM " + tablename + " WHERE " + cond + ") Temp";
    } else {
	query += tablename;
    }

    size_t result = getSizeFromDB(query);
    return result == 0;
}

SegmentInserter *SQLTable::getInserter(const Literal &q) {
    std::string cond = literalConstraintsToSQLQuery(q);
    std::string cond1 = repeatedToSQLQuery(q);
    if (cond.empty()) {
	cond = cond1;
    } else if (!cond1.empty()) {
	cond += " and " + cond1;
    }
    std::string query = "SELECT DISTINCT * FROM " + tablename;
    if (!cond.empty()) {
	query += " WHERE " + cond;
    }
    SegmentInserter *inserter = new SegmentInserter(arity);
    executeQuery(query, inserter);
    return inserter;
}

EDBIterator *SQLTable::getIterator(const Literal &q) {
    std::vector<uint8_t> sortFields;
    // TODO: caching
    if (q.getTupleSize() != arity) {
        return new InmemoryIterator(NULL, predid, sortFields);
    }
    LOG(DEBUGL) << "getIterator: query = " << q.tostring(NULL, layer);
    SegmentInserter *inserter = getInserter(q);
    std::shared_ptr<const Segment> segment = inserter->getSegment();
    delete inserter;
    return new InmemoryIterator(segment, predid, sortFields);
}

EDBIterator *SQLTable::getSortedIterator(const Literal &q,
        const std::vector<uint8_t> &fields) {
    // TODO: caching
    // Awful semantics: "fields" counts the variable numbers, not the actual fields of the literal...
    std::vector<uint8_t> offsets;
    int nConstantsSeen = 0;
    int varNo = 0;
    for (int i = 0; i < q.getTupleSize(); i++) {
        if (! q.getTermAtPos(i).isVariable()) {
            nConstantsSeen++;
        } else {
            offsets.push_back(nConstantsSeen);
        }

    }
    std::vector<uint8_t> newFields;
    for (auto f : fields) {
        newFields.push_back(offsets[f] + f);
    }

    if (q.getTupleSize() != arity) {
        return new InmemoryIterator(NULL, predid, newFields);
    }
    LOG(DEBUGL) << "getSortedIterator: query = " << q.tostring(NULL, layer);
    SegmentInserter *inserter = getInserter(q);
    std::shared_ptr<const Segment> segment = inserter->getSegment();
    delete inserter;
    segment = segment->sortBy(&newFields);
    return new InmemoryIterator(segment, predid, newFields);
}

// If valuesToFilter is larger than this, we should filter ourselves. TODO.
#define TEMP_TABLE_THRESHOLD (2*3*4*5*7*11)

void SQLTable::query(QSQQuery *query, TupleTable *outputTable,
                       std::vector<uint8_t> *posToFilter,
                       std::vector<Term_t> *valuesToFilter) {
    const Literal *l = query->getLiteral();
    const uint8_t npos = query->getNPosToCopy();
    uint8_t *pos = query->getPosToCopy();
	std::unique_ptr<uint64_t> row = std::unique_ptr<uint64_t>(new uint64_t[npos]);
    EDBIterator *iter;

    if (posToFilter == NULL || posToFilter->size() == 0) {
	iter = getIterator(*l);
    } else {
	//Create first part of query.
	std::string sqlQuery = "SELECT DISTINCT * FROM " + tablename;
	if (valuesToFilter->size() > TEMP_TABLE_THRESHOLD) {
	    // TODO
	    LOG(ERRORL) << "Not implemented: SQLTable::query with many values to filter.";
	    throw 10;
	}
	sqlQuery += " WHERE ";
	std::string cond = literalConstraintsToSQLQuery(*l);
	if (!cond.empty()) {
	    sqlQuery += cond;
	}

	std::string cond1 = repeatedToSQLQuery(*l);
	if (!cond1.empty()) {
	    if (!cond.empty()) {
		sqlQuery += " AND ";
	    }
	    sqlQuery += cond1;
	}
     
	if (!cond1.empty() || !cond.empty()) {
	    sqlQuery += " AND ";
	}

	bool first = true;
	sqlQuery += "(";
	for (std::vector<Term_t>::iterator itr = valuesToFilter->begin();
		itr != valuesToFilter->end();) {
	    if (! first) {
		sqlQuery += " OR ";
	    }
	    sqlQuery += "(";
	    for (int i = 0; i < posToFilter->size(); i++, itr++) {
		std::string pref = i > 0 ? " AND " : "";
		uint8_t pos = posToFilter->at(i);
		sqlQuery += pref + fieldTables[pos] + " = " + mapToField(*itr, pos);
	    }
	    sqlQuery += ")";
	    first = false;
	}
	sqlQuery += ")";
	SegmentInserter *inserter = new SegmentInserter(arity);
	executeQuery(sqlQuery, inserter);
	std::shared_ptr<const Segment> segment = inserter->getSegment();
	delete inserter;
	std::vector<uint8_t> sortFields;
	iter = new InmemoryIterator(segment, predid, sortFields);
    }

    int count = 0;
    while (iter->hasNext()) {
	iter->next();
	for (int i = 0; i < npos; i++) {
	    row.get()[i] = iter->getElementAt(pos[i]);
	}
	outputTable->addRow(row.get());
	count++;
    }
    iter->clear();
    delete iter;
    LOG(DEBUGL) << "query gave " << count << " results";
}

uint64_t SQLTable::getSize() {
    std::string query = "SELECT COUNT(*) from " + tablename;
    uint64_t result = getSizeFromDB(query);
    return result;
}

size_t SQLTable::estimateCardinality(const Literal &query) {
    //TODO: This should be improved
    return getCardinality(query);
}

void SQLTable::releaseIterator(EDBIterator *itr) {
    delete itr;
}
