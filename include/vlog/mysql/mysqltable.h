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
        MySQLTable(PredId_t predid, string host, string user, string pwd, string dbname,
                string tablename, string tablefields, EDBLayer *layer);

	void executeQuery(const std::string &query, SegmentInserter *inserter);

	uint64_t getSizeFromDB(const std::string &query);

        ~MySQLTable() {
            if (con) {
                con->close();
                delete con;
                con = NULL;
            }
        }
};

#endif
