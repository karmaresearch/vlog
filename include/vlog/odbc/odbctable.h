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

public:
    ODBCTable(string user, string pwd, string dbname,
               string tablename, string tablefields);

    static void check(SQLRETURN rc, string msg);

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

    ~ODBCTable();
};

#endif
