#ifndef _ODBC_TABLE_H
#define _ODBC_TABLE_H

#include <vlog/column.h>
#include <vlog/sqltable.h>

#include <sql.h>
#include <sqltypes.h>
#include <sqlext.h>

class ODBCTable : public SQLTable {
private:
    SQLHANDLE env;
    SQLHANDLE con;

    SQLRETURN check(SQLRETURN rc, string msg);

public:
    ODBCTable(PredId_t predid, string user, string pwd, string dbname,
               string tablename, string tablefields, EDBLayer *layer);

    void executeQuery(const std::string &query, SegmentInserter *inserter);

    uint64_t getSizeFromDB(const std::string &query);

    ~ODBCTable();
};

#endif
