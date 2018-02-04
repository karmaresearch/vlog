#if MAPI
#include <vlog/mapi/mapitable.h>
#include <vlog/mapi/mapiiterator.h>

#include <unistd.h>
#include <sstream>
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

void MAPITable::update(Mapi dbh, string q) { 
    MapiHdl ret = doquery(dbh, q); 

    if (mapi_close_handle(ret) != MOK) {
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
}

MAPITable::MAPITable(string host, int port, string user, string pwd, string dbname,
                       string tablename, string tablefields) {

    this->tablename = tablename;
    con = mapi_connect(host.c_str(), port, user.c_str(), pwd.c_str(), "sql", dbname.c_str()); 
    if (mapi_error(con)) {
	mapi_explain(con, stderr); 
	mapi_destroy(con); 
	throw 10;
    }

    //Extract fields
    std::stringstream ss(tablefields);
    std::string item;
    while (std::getline(ss, item, ',')) {
        this->fieldTables.push_back(item);
    }
}

uint64_t MAPITable::getSize() {

    string query = "SELECT COUNT(*) as c from " + tablename;

    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();

    MapiHdl handle = doquery(con, query);
    mapi_fetch_row(handle);
    char *res = mapi_fetch_field(handle, 0);
    char *p;
    size_t result = strtol(res, &p, 10);
    mapi_close_handle(handle);

    std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;

    LOG(DEBUGL) << "SQL Query (getSize): " << query << " took " << sec.count();

    return result;
}

// If valuesToFilter is larger than this, use a temporary table instead of a test on the individual values
#define TEMP_TABLE_THRESHOLD (2*3*4*5*7*11)

void MAPITable::query(QSQQuery *query, TupleTable *outputTable,
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
	    LOG(DEBUGL) << "SQL create temp table: " << sqlCreateTable;
	    update(con, sqlCreateTable);

	    // Insert values into table
	    LOG(DEBUGL) << "START TRANSACTION";
	    update(con, "START TRANSACTION");

	    for (std::vector<Term_t>::iterator itr = valuesToFilter->begin();
		    itr != valuesToFilter->end();) {
		string addition = "INSERT INTO temp VALUES (";
		for (int i = 0; i < posToFilter->size(); i++, itr++) {
		    string pref = i > 0 ? ", " : "";
		    addition += pref + to_string(*itr);
		}
		addition += ")";
		LOG(DEBUGL) << "SQL insert into temp table: " << addition;
		update(con, addition);
	    }

	    LOG(DEBUGL) << "COMMIT";
	    update(con, "COMMIT");
	    
	    /*
	    sqlCreateTable = "ALTER TABLE temp ADD CONSTRAINT pkey PRIMARY KEY (";
	    for (int i = 0; i < posToFilter->size(); i++) {
		if (i > 0) {
		    sqlCreateTable += ", ";
		}
		sqlCreateTable += "x" + to_string(i);
	    }
	    sqlCreateTable += ")";
	    LOG(DEBUGL) << "alter table: add primary key: " << sqlCreateTable;
	    update(con, sqlCreateTable);
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

	iter = new MAPIIterator(con, sqlQuery, fieldTables, *l);
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
	LOG(DEBUGL) << "DROP TABLE temp";
	update(con, "DROP TABLE temp");
    }
}

size_t MAPITable::getCardinality(const Literal &q) {
    string query = "SELECT COUNT(*) as c from " + tablename;

    string cond = literalConstraintsToSQLQuery(q, fieldTables);
    if (cond != "") {
        query += " WHERE " + cond;
    }

    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();

    MapiHdl handle = doquery(con, query);
    mapi_fetch_row(handle);
    char *res = mapi_fetch_field(handle, 0);
    char *p;
    size_t result = strtol(res, &p, 10);
    mapi_close_handle(handle);

    std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;

    LOG(DEBUGL) << "SQL Query (getCardinality): " << query << " took " << sec.count();

    return (size_t) result;
}

size_t MAPITable::getCardinalityColumn(const Literal &q, uint8_t posColumn) {
    
    string query = "SELECT COUNT(DISTINCT " + fieldTables[posColumn] + ") as c from " + tablename;

    string cond = literalConstraintsToSQLQuery(q, fieldTables);
    if (cond != "") {
        query += " WHERE " + cond;
    }

    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();

    MapiHdl handle = doquery(con, query);
    mapi_fetch_row(handle);
    char *res = mapi_fetch_field(handle, 0);
    char *p;
    size_t result = strtol(res, &p, 10);
    mapi_close_handle(handle);

    std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;

    LOG(DEBUGL) << "SQL Query (getCardinalityColumn): " << query << " took " << sec.count();

    return (size_t) result;
}

bool MAPITable::isEmpty(const Literal &q, std::vector<uint8_t> *posToFilter,
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

    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();

    MapiHdl handle = doquery(con, query);
    mapi_fetch_row(handle);
    char *res = mapi_fetch_field(handle, 0);
    char *p;
    size_t result = strtol(res, &p, 10);
    mapi_close_handle(handle);

    std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;

    LOG(DEBUGL) << "SQL Query (isEmpty): " << query << " took " << sec.count();

    return result == 0;
}

EDBIterator *MAPITable::getIterator(const Literal &query) {
    return new MAPIIterator(con, tablename, query, fieldTables, NULL);
}

EDBIterator *MAPITable::getSortedIterator(const Literal &query,
        const std::vector<uint8_t> &fields) {
    return new MAPIIterator(con, tablename, query, fieldTables, &fields);
}

bool MAPITable::getDictNumber(const char *text, const size_t sizeText,
                               uint64_t &id) {
    string query = "SELECT id from revdict where value='" + string(text, sizeText) + "'";
    MapiHdl handle = doquery(con, query);
    if (mapi_fetch_row(handle)) {
	char *res = mapi_fetch_field(handle, 0);
	char *p;
	id = strtol(res, &p, 10);
	mapi_close_handle(handle);
	return true;
    }
    mapi_close_handle(handle);
    return false;
}

bool MAPITable::getDictText(const uint64_t id, char *text) {
    string query = "SELECT value from dict where id=" + to_string(id);
    MapiHdl handle = doquery(con, query);
    if (mapi_fetch_row(handle)) {
	char *res = mapi_fetch_field(handle, 0);
	strcpy(text, res);
	mapi_close_handle(handle);
	return true;
    }
    mapi_close_handle(handle);
    return false;
}

uint64_t MAPITable::getNTerms() {
    string query = "SELECT COUNT(*) as c from dict";
    MapiHdl handle = doquery(con, query);
    mapi_fetch_row(handle);
    char *res = mapi_fetch_field(handle, 0);
    char *p;
    uint64_t result = strtol(res, &p, 10);
    mapi_close_handle(handle);
    return result;
}

MAPITable::~MAPITable() {
    mapi_destroy(con);
}
#endif
