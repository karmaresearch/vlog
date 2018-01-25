#ifndef _SQL_TABLE_H
#define _SQL_TABLE_H

#include <vlog/column.h>
#include <vlog/edbiterator.h>

class SQLTable : public EDBTable {
public:
    string tablename;
    std::vector<string> fieldTables;

    void releaseIterator(EDBIterator *itr);

    size_t estimateCardinality(const Literal &query);

    static string literalConstraintsToSQLQuery(const Literal &query,
                                    const std::vector<string> &fieldTables);

    static string repeatedToSQLQuery(const Literal &query,
                                    const std::vector<string> &fieldTables);
};


#endif
