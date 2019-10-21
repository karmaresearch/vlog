#ifndef _SQL_TABLE_H
#define _SQL_TABLE_H

#include <vlog/column.h>
#include <vlog/segment.h>
#include <vlog/edbiterator.h>

class SQLTable : public EDBTable {
private:
    SegmentInserter *getInserter(const Literal &query);
public:
    PredId_t predid;
    std::string tablename;
    uint8_t arity;
    std::vector<std::string> fieldTables;
    EDBLayer *layer;
    
    SQLTable(PredId_t predid, std::string name, std::string fieldNames, EDBLayer *layer);

    std::string mapToField(uint64_t value, uint8_t ind);

    std::string literalConstraintsToSQLQuery(const Literal &q);

    std::string repeatedToSQLQuery(const Literal &q);

    void releaseIterator(EDBIterator *itr);

    size_t estimateCardinality(const Literal &query);

    size_t getCardinality(const Literal &query);

    size_t getCardinalityColumn(const Literal &q, uint8_t posColumn);

    bool isEmpty(const Literal &q, std::vector<uint8_t> *posToFilter,
                         std::vector<Term_t> *valuesToFilter);

    EDBIterator *getIterator(const Literal &query);

    EDBIterator *getSortedIterator(const Literal &query,
        const std::vector<uint8_t> &fields);

    void query(QSQQuery *query, TupleTable *outputTable,
                       std::vector<uint8_t> *posToFilter,
                       std::vector<Term_t> *valuesToFilter);

    uint64_t getSize();

    bool getDictNumber(const char *text, const size_t sizeText, uint64_t &id) {
	return false;
    }

    bool getDictText(const uint64_t id, char *text) {
	return false;
    }

    bool getDictText(const uint64_t id, std::string &text) {
	return false;
    }

    uint64_t getNTerms() {
	return 0;
    }

    virtual uint8_t getArity() const {
	return arity;
    }


    virtual void executeQuery(const std::string &q, SegmentInserter *inserter) = 0;

    virtual uint64_t getSizeFromDB(const std::string &q) = 0;
};


#endif
