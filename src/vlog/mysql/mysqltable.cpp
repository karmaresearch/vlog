#if MYSQL
#include <vlog/mysql/mysqltable.h>
#include <vlog/mysql/mysqliterator.h>

//Mysql connectors header
#include <mysql_connection.h>
#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>

#include <sstream>
#include <string>


MySQLTable::MySQLTable(string host, string user, string pwd, string dbname,
                       string tablename, string tablefields) {
    con = NULL;
    this->tablename = tablename;
    driver = get_driver_instance();
    con = driver->connect(host, user, pwd);
    con->setSchema(dbname);

    //Extract fields
    std::stringstream ss(tablefields);
    std::string item;
    while (std::getline(ss, item, ',')) {
        this->fieldTables.push_back(item);
    }
}

// If valuesToFilter is larger than this, use a temporary table instead of a test on the individual values
#define TEMP_TABLE_THRESHOLD (2*3*4*5*7*11)

void MySQLTable::query(QSQQuery *query, TupleTable *outputTable,
                       std::vector<uint8_t> *posToFilter,
                       std::vector<Term_t> *valuesToFilter) {
    const Literal *l = query->getLiteral();
    const uint8_t npos = query->getNPosToCopy();
    uint8_t *pos = query->getPosToCopy();
    uint64_t row[npos];
    int count = 0;
    EDBIterator *iter;
    if (posToFilter == NULL || posToFilter->size() == 0) {
	iter = getIterator(*l);
    } else {
	//Create first part of query.
	string sqlQuery = "SELECT * FROM " + tablename + " WHERE ";
	string cond = SQLTable::literalConstraintsToSQLQuery(*l, fieldTables);
	if (cond != "") {
	    sqlQuery += cond;
	}

	string cond1 = SQLTable::repeatedToSQLQuery(*l, fieldTables);
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
	    sqlCreateTable += ")";
	    sql::Statement *stmt;
	    stmt = con->createStatement();
	    LOG(DEBUGL) << "SQL create temp table: " << sqlCreateTable;
	    MYSQLCALL(stmt->execute(sqlCreateTable))
	    delete stmt;

	    // Insert values into table
	    stmt = con->createStatement();
	    LOG(DEBUGL) << "START TRANSACTION";
	    stmt->execute("START TRANSACTION");
	    delete stmt;

	    for (std::vector<Term_t>::iterator itr = valuesToFilter->begin();
		    itr != valuesToFilter->end();) {
		string addition = "INSERT INTO temp VALUES (";
		for (int i = 0; i < posToFilter->size(); i++, itr++) {
		    string pref = i > 0 ? ", " : "";
		    addition += pref + to_string(*itr);
		}
		addition += ")";
		LOG(DEBUGL) << "SQL insert into temp table: " << addition;
		stmt = con->createStatement();
		stmt->execute(addition);
		delete stmt;
	    }

	    stmt = con->createStatement();
	    LOG(DEBUGL) << "COMMIT";
	    stmt->execute("COMMIT");
	    delete stmt;
	    
	    sqlCreateTable = "ALTER TABLE temp ADD CONSTRAINT pkey PRIMARY KEY (";
	    for (int i = 0; i < posToFilter->size(); i++) {
		if (i > 0) {
		    sqlCreateTable += ", ";
		}
		sqlCreateTable += "x" + to_string(i);
	    }
	    sqlCreateTable += ")";
	    stmt = con->createStatement();
	    LOG(DEBUGL) << "alter table: add primary key";
	    stmt->execute(sqlCreateTable);
	    delete stmt;


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

	iter = new MySQLIterator(con, sqlQuery, *l);
    }

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
	sql::Statement *stmt = con->createStatement();
	LOG(DEBUGL) << "DROP TABLE temp";
	stmt->execute("DROP TABLE temp");
	delete stmt;
    }
}

uint64_t MySQLTable::getSize() {

    string query = "SELECT COUNT(*) as c from " + tablename;

    LOG(DEBUGL) << "SQL Query: " << query;

    sql::Statement *stmt;
    stmt = con->createStatement();
    sql::ResultSet *res = stmt->executeQuery(query);
    uint64_t result = 0;
    if (res->first()) {
        result = res->getInt(1);
    }
    delete res;
    delete stmt;
    return result;
}

size_t MySQLTable::getCardinality(const Literal &q) {
    string query = "SELECT COUNT(*) as c from " + tablename;

    string cond = literalConstraintsToSQLQuery(q, fieldTables);
    if (cond != "") {
        query += " WHERE " + cond;
    }

    sql::Statement *stmt;
    stmt = con->createStatement();
    sql::ResultSet *res = stmt->executeQuery(query);
    size_t card = 0;
    if (res->first()) {
        card = res->getInt(1);
    }
    LOG(DEBUGL) << "SQL Query: " << query << ", Cardinality = " << card;
    delete res;
    delete stmt;
    return card;
}

size_t MySQLTable::getCardinalityColumn(const Literal &q, uint8_t posColumn) {
    
    string query = "SELECT COUNT(DISTINCT " + fieldTables[posColumn] + ") as c from " + tablename;

    string cond = literalConstraintsToSQLQuery(q, fieldTables);
    if (cond != "") {
        query += " WHERE " + cond;
    }

    sql::Statement *stmt;
    stmt = con->createStatement();
    sql::ResultSet *res = stmt->executeQuery(query);
    size_t card = 0;
    if (res->first()) {
        card = res->getInt(1);
    }
    LOG(DEBUGL) << "SQL Query: " << query << ", cardinalityColumn = " << card;
    delete res;
    delete stmt;
    return card;
}

bool MySQLTable::isEmpty(const Literal &q, std::vector<uint8_t> *posToFilter,
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

    sql::Statement *stmt;
    stmt = con->createStatement();
    sql::ResultSet *res = stmt->executeQuery(query);
    size_t card = 0;
    if (res->first()) {
	card = res->getInt(1);
    }
    LOG(DEBUGL) << "SQL Query: " << query << ", in isEmpty, cardinality = " << card;
    delete res;
    delete stmt;
    return card == 0;
}

EDBIterator *MySQLTable::getIterator(const Literal &query) {
    return new MySQLIterator(con, tablename, query, fieldTables, NULL);
}

EDBIterator *MySQLTable::getSortedIterator(const Literal &query,
        const std::vector<uint8_t> &fields) {
    return new MySQLIterator(con, tablename, query, fieldTables, &fields);
}

bool MySQLTable::getDictNumber(const char *text, const size_t sizeText,
                               uint64_t &id) {
    string query = "SELECT id from revdict where value='" + string(text, sizeText) + "'";
    sql::Statement *stmt;
    stmt = con->createStatement();
    sql::ResultSet *res = stmt->executeQuery(query);
    bool resp = false;
    if (res->first()) {
        id = res->getUInt64(1);
        resp = true;
    }
    //LOG(DEBUGL) << "Value=" << string(text, sizeText) << " ID=" << id;
    delete res;
    delete stmt;
    return resp;

}

bool MySQLTable::getDictText(const uint64_t id, char *text) {
    string query = "SELECT value from dict where id=" + to_string(id);
    sql::Statement *stmt;
    stmt = con->createStatement();
    sql::ResultSet *res = stmt->executeQuery(query);
    bool resp = false;
    if (res->first()) {
        string t = res->getString(1);
        memcpy(text, t.c_str(), t.size());
        text[t.size()] = 0;
        resp = true;
    }
    delete res;
    delete stmt;
    return resp;
}

uint64_t MySQLTable::getNTerms() {
    string query = "SELECT COUNT(*) as c from dict";
    sql::Statement *stmt;
    stmt = con->createStatement();
    sql::ResultSet *res = stmt->executeQuery(query);
    uint64_t card = 0;
    if (res->first()) {
        card = res->getUInt64(1);
    }
    delete res;
    delete stmt;
    return card;
}
#endif
