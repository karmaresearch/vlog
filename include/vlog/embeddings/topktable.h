#ifndef _TOPK_TABLE_H
#define _TOPK_TABLE_H

#include <vlog/column.h>
#include <vlog/edbtable.h>
#include <vlog/edbiterator.h>
#include <vlog/segment.h>

#include <trident/ml/embeddings.h>

class TopKTable : public EDBTable{
    private:
        EDBLayer *layer;
        int64_t topk;
        std::string typeprediction;

    public:
        virtual uint8_t getArity() const {
            return 3;
        }

        bool areTermsEncoded() {
            return true;
        }

        TopKTable(EDBLayer *layer, std::string topk, std::string typeprediction);

        void query(QSQQuery *query, TupleTable *outputTable,
                std::vector<uint8_t> *posToFilter,
                std::vector<Term_t> *valuesToFilter);

        size_t estimateCardinality(const Literal &query);

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

        bool getDictText(const uint64_t id, std::string &text);

        bool expensiveLayer() {
            return true;
        }

        uint64_t getNTerms();

        void releaseIterator(EDBIterator *itr);

        uint64_t getSize();

        ~TopKTable();
};

#endif
