#ifndef INCREMENTAL__EDB_TABLE_FROM_IDB_H__
#define INCREMENTAL__EDB_TABLE_FROM_IDB_H__

/**
 * In the overdelete and rederive steps DRed algorithm, we require the values
 * of the previous materialization stages, but they are immutable. So we treat
 * them as if they are EDB tables.
 */

#include <vlog/fcinttable.h>
#include <vlog/seminaiver.h>


/**
 * Transfer an IDB predicate of the previous SemiNaiver to be
 * a new 'EDB' predicate.
 */
class EDBonIDBTable : public EDBTable {
private:
    PredId_t    predid;
    const std::shared_ptr<SemiNaiver> prevSemiNaiver;
    const FCTable *idbTable;
    EDBTable *naturalTable;
    size_t cardinality;
    EDBLayer *layer;

    static bool isNatural(const Literal &query,
                          const std::vector<uint8_t> fields);

public:
    EDBonIDBTable(PredId_t predid, EDBLayer *layer,
                  const std::shared_ptr<SemiNaiver> prevSN);

    virtual ~EDBonIDBTable();

    //execute the query on the knowledge base
    virtual void query(QSQQuery *query, TupleTable *outputTable,
                       std::vector<uint8_t> *posToFilter,
                       std::vector<Term_t> *valuesToFilter) {
        LOG(ERRORL) << "FIXME: implement " << __func__;
    }

    virtual size_t estimateCardinality(const Literal &query) {
        return getCardinality(query);
    }

    virtual size_t getCardinality(const Literal &query) {
        return cardinality;
    }

    virtual size_t getCardinalityColumn(const Literal &query, uint8_t posColumn) {
        LOG(ERRORL) << "FIXME: implement " << __func__;
    }

    virtual bool isEmpty(const Literal &query, std::vector<uint8_t> *posToFilter,
                         std::vector<Term_t> *valuesToFilter) {
       if (posToFilter == NULL) {
           return getCardinality(query) == 0;
       }

       LOG(ERRORL) << "Not implemented:" << __func__;
       throw 10;
    }

    virtual EDBIterator *getIterator(const Literal &q);

    virtual EDBIterator *getSortedIterator(const Literal &query,
                                           const std::vector<uint8_t> &fields);

    virtual void releaseIterator(EDBIterator *itr);

    virtual bool getDictNumber(const char *text, const size_t sizeText,
                               uint64_t &id) {
        return false;
    }

    virtual bool getDictText(const uint64_t id, char *text) {
        return false;
    }

    virtual bool getDictText(const uint64_t id, std::string &text) {
        return false;
    }

    virtual uint64_t getNTerms() {
        return 0;
    }

    virtual uint8_t getArity() const {
        return idbTable->getSizeRow();
    }

    virtual uint64_t getSize() {
        return idbTable->getNAllRows();
    }

    virtual PredId_t getPredicateID() const {
        return predid;
    }

    std::ostream &dump(std::ostream &os);
};

/*
std::ostream &operator<<(ostream &os, const EDBonIDBTable &table) {
    return os << table.dump();
}
*/

#endif  // INCREMENTAL__EDB_TABLE_FROM_IDB_H__
