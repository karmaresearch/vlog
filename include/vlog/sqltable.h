#ifndef _SQL_TABLE_H
#define _SQL_TABLE_H

#include <vlog/column.h>
#include <vlog/sqltable.h>
#include <vlog/edbiterator.h>

class SQLTable : public EDBTable {
public:
    string tablename;
    std::vector<string> fieldTables;

    std::vector<std::shared_ptr<Column>> checkNewIn(const Literal &l1,
            std::vector<uint8_t> &posInL1,
            const Literal &l2,
            std::vector<uint8_t> &posInL2);

    std::vector<std::shared_ptr<Column>> checkNewIn(
                std::vector <
                std::shared_ptr<Column >> &checkValues,
                const Literal &l2,
                std::vector<uint8_t> &posInL2);

    std::shared_ptr<Column> checkIn(
        std::vector<Term_t> &values,
        const Literal &l2,
        uint8_t posInL2,
        size_t &sizeOutput);

    void releaseIterator(EDBIterator *itr);

    size_t estimateCardinality(const Literal &query);

    static string literalConstraintsToSQLQuery(const Literal &query,
                                    const std::vector<string> &fieldTables);

    static string repeatedToSQLQuery(const Literal &query,
                                    const std::vector<string> &fieldTables);
};


#endif
