#ifndef _MAPI_TABLE_H
#define _MAPI_TABLE_H

#include <vlog/column.h>
#include <vlog/sqltable.h>

#include <monetdb/mapi.h>

class MAPITable : public SQLTable {
private:
    Mapi con;

public:
    MAPITable(string host, int port, string user, string pwd, string dbname,
	                           string tablename, string tablefields);

    static MapiHdl doquery(Mapi dbh, string q);
    
    static void update(Mapi dbh, string q);

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

    ~MAPITable();
};

#endif
