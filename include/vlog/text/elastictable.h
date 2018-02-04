#ifndef _ELASTIC_TABLE_H
#define _ELASTIC_TABLE_H

#include <vlog/concepts.h>
#include <vlog/edbtable.h>

#include <vector>

class ElasticTable: public EDBTable {
private:

public:
    ElasticTable();

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

    ~ElasticTable();
};

#endif
