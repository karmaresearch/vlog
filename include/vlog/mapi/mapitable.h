#ifndef _MAPI_TABLE_H
#define _MAPI_TABLE_H

#include <vlog/column.h>
#include <vlog/sqltable.h>

#include <monetdb/mapi.h>

class MAPITable : public SQLTable {
private:
    Mapi con;

public:
    MAPITable(PredId_t predid, string host, int port,
                        string user, string pwd, string dbname,
                        string tablename, string tablefields, EDBLayer *layer);

    static MapiHdl doquery(Mapi dbh, string q);

    void executeQuery(const std::string &query, SegmentInserter *inserter);

    uint64_t getSizeFromDB(const std::string &query);

    ~MAPITable();
};

#endif
