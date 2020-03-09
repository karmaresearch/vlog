#if MYSQL
#include <vlog/mysql/mysqltable.h>

//Mysql connectors header
#include <mysql_connection.h>
#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>

#include <unistd.h>
#include <string>

MySQLTable::MySQLTable(PredId_t predid, std::string host, std::string user, std::string pwd,
        std::string dbname, std::string tablename, std::string tablefields, EDBLayer *layer)
	: SQLTable(predid, tablename, tablefields, layer) {
    con = NULL;
    driver = get_driver_instance();
    con = driver->connect(host, user, pwd);
    con->setSchema(dbname);
}

uint64_t MySQLTable::getSizeFromDB(const std::string &query) {

    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();

    sql::Statement *stmt = con->createStatement();
    sql::ResultSet *res = stmt->executeQuery(query);
    uint64_t result = 0;
    if (res->first()) {
        result = res->getInt(1);
    }
    delete res;
    delete stmt;

    std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    LOG(DEBUGL) << "SQL Query: " << query << " took " << sec.count();

    return result;
}

void MySQLTable::executeQuery(const std::string &query, SegmentInserter *inserter) {

    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();

    sql::Statement *stmt = con->createStatement();
    sql::ResultSet *res = stmt->executeQuery(query);

    while(res->next()) {
	uint64_t row[256];
	for (int i = 0; i < arity; i++) {
	    std::string field = res-> getString(fieldTables[i]);
	    layer->getOrAddDictNumber(field.c_str(), field.size(), row[i]);
        }
        inserter->addRow(row);
    }

    delete res;
    delete stmt;

    std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    LOG(DEBUGL) << "SQL Query: " << query << " took " << sec.count();
}

#endif
