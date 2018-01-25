#ifndef _EDB_TABLE_H
#define _EDB_TABLE_H

class Column;
class EDBIterator;
class EDBTable {
public:
    virtual std::vector<std::shared_ptr<Column>> checkNewIn(const Literal &l1,
            std::vector<uint8_t> &posInL1,
            const Literal &l2,
            std::vector<uint8_t> &posInL2);

    virtual std::vector<std::shared_ptr<Column>> checkNewIn(
                std::vector <
                std::shared_ptr<Column >> &checkValues,
                const Literal &l2,
                std::vector<uint8_t> &posInL2);

    virtual std::shared_ptr<Column> checkIn(
        std::vector<Term_t> &values,
        const Literal &l2,
        uint8_t posInL2,
        size_t &sizeOutput);

    //execute the query on the knowledge base
    virtual void query(QSQQuery *query, TupleTable *outputTable,
                       std::vector<uint8_t> *posToFilter,
                       std::vector<Term_t> *valuesToFilter) = 0;

    virtual size_t estimateCardinality(const Literal &query) = 0;

    virtual size_t getCardinality(const Literal &query) = 0;

    virtual size_t getCardinalityColumn(const Literal &query, uint8_t posColumn) = 0;

    virtual bool isEmpty(const Literal &query, std::vector<uint8_t> *posToFilter,
                         std::vector<Term_t> *valuesToFilter) = 0;

    virtual EDBIterator *getIterator(const Literal &query) = 0;

    virtual EDBIterator *getSortedIterator(const Literal &query,
                                           const std::vector<uint8_t> &fields) = 0;

    virtual void releaseIterator(EDBIterator *itr) = 0;

    virtual bool getDictNumber(const char *text, const size_t sizeText,
                               uint64_t &id) = 0;

    virtual bool getDictText(const uint64_t id, char *text) = 0;

    virtual uint64_t getNTerms() = 0;

    virtual uint64_t getSize() = 0;
};


#endif
