#ifndef _MAPI_TABLE_H
#define _MAPI_TABLE_H

#include <vlog/column.h>
#include <vlog/sqltable.h>

#include <monetdb/mapi.h>

class MAPITable : public SQLTable {
private:
    Mapi con;

public:
    MAPITable(PredId_t predid, std::string host, int port,
                        std::string user, std::string pwd, std::string dbname,
                        std::string tablename, std::string tablefields, EDBLayer *layer);

    static MapiHdl doquery(Mapi dbh, std::string q);

    void executeQuery(const std::string &query, SegmentInserter *inserter);

    uint64_t getSizeFromDB(const std::string &query);

    ~MAPITable();
};

#endif
