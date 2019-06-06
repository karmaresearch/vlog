#ifndef VLOG__INCREMENTAL__EDB_TABLE_IMPORTER__H__
#define VLOG__INCREMENTAL__EDB_TABLE_IMPORTER__H__

#include <vlog/concepts.h>
#include <vlog/edb.h>
#include <vlog/seminaiver.h>

/**
 * Wrapper to just re-use an EDB table from an older EDBLayer
 */
class EDBimporter : public EDBTable {
protected:
    PredId_t    predid;
    const std::shared_ptr<SemiNaiver> prevSemiNaiver;
    std::shared_ptr<EDBTable> edbTable;
    EDBLayer *layer;

    // should be const
    size_t countCardinality(const Literal &query);

public:
    EDBimporter(PredId_t predid, EDBLayer *layer,
                const std::shared_ptr<SemiNaiver> prevSN);

    virtual ~EDBimporter();

    //execute the query on the knowledge base
    virtual void query(QSQQuery *query, TupleTable *outputTable,
                       std::vector<uint8_t> *posToFilter,
                       std::vector<Term_t> *valuesToFilter) {
        LOG(ERRORL) << "Need to consider possible Removals";
        edbTable->query(query, outputTable, posToFilter, valuesToFilter);
    }

    size_t estimateCardinality(const Literal &query) {
        return edbTable->estimateCardinality(query);
    }

    // Would like this to be const
    size_t getCardinality(const Literal &query);

    size_t getCardinalityColumn(const Literal &query, uint8_t posColumn) {
        LOG(ERRORL) << "Need to consider possible Removals";
        return edbTable->getCardinalityColumn(query, posColumn);
    }

    bool isEmpty(const Literal &query,
                         std::vector<uint8_t> *posToFilter,
                         std::vector<Term_t> *valuesToFilter);

    EDBIterator *getIterator(const Literal &q) {
        return edbTable->getIterator(q);
    }

    EDBIterator *getSortedIterator(const Literal &query,
                                           const std::vector<uint8_t> &fields) {
        return edbTable->getSortedIterator(query, fields);
    }

    void releaseIterator(EDBIterator *itr) {
        return edbTable->releaseIterator(itr);
    }

    bool getDictNumber(const char *text, const size_t sizeText,
                               uint64_t &id) {
        return edbTable->getDictNumber(text, sizeText, id);
    }

    bool getDictText(const uint64_t id, char *text) {
        return edbTable->getDictText(id, text);
    }

    bool getDictText(const uint64_t id, std::string &text) {
        return edbTable->getDictText(id, text);
    }

    uint64_t getNTerms() {
        return edbTable->getNTerms();
    }

    uint8_t getArity() const {
        return edbTable->getArity();
    }

    uint64_t getSize() {
        return edbTable->getSize();
    }

    PredId_t getPredicateID() const {
        return predid;
    }

    std::ostream &dump(std::ostream &os);
};

#endif  // ndef VLOG__INCREMENTAL__EDB_TABLE_IMPORTER__H__
