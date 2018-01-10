#ifndef _MYSQL_TABLE_H
#define _MYSQL_TABLE_H

#include <vlog/column.h>
#include <vlog/sqltable.h>

//Mysql connectors header
#include <mysql_connection.h>
#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>

#define MYSQLCALL(stat) \
    try { \
	stat; \
    } catch (sql::SQLException &e) { \
       LOG(WARNL) << "# ERR: SQLException in " << __FILE__ \
	   << "(" << __FUNCTION__ << ") on line " << __LINE__; \
       LOG(WARNL) << "# ERR: " << e.what() \
	   << " (MySQL error code: " << e.getErrorCode() \
	   << ", SQLState: " << e.getSQLState() << " )"; \
	exit(1); \
    }

class MySQLTable : public SQLTable {
private:
    sql::Driver *driver;
    sql::Connection *con;

public:
    MySQLTable(string host, string user, string pwd, string dbname,
               string tablename, string tablefields);

    void query(QSQQuery *query, TupleTable *outputTable,
               std::vector<uint8_t> *posToFilter,
               std::vector<Term_t> *valuesToFilter);

    size_t getCardinality(const Literal &query);

    size_t getCardinalityColumn(const Literal &query, uint8_t posColumn);

    bool isEmpty(const Literal &query, std::vector<uint8_t> *posToFilter,
                 std::vector<Term_t> *valuesToFilter);

    EDBIterator *getIterator(const Literal &query);

    EDBIterator *getSortedIterator(const Literal &query,
                                   const std::vector<uint8_t> &fields);

    bool getDictNumber(const char *text, const size_t sizeText,
                       uint64_t &id);

    bool getDictText(const uint64_t id, char *text);

    uint64_t getNTerms();

    uint64_t getSize();

    ~MySQLTable() {
        if (con) {
	    con->close();
            delete con;
	    con = NULL;
	}
    }
};

#endif
