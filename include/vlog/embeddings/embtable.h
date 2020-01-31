#ifndef _EMB_LAYER_H
#define _EMB_LAYER_H

#include <vlog/column.h>
#include <vlog/edbtable.h>
#include <vlog/edbiterator.h>
#include <vlog/segment.h>

#include <trident/ml/embeddings.h>

#include <unordered_map>

class EmbTable : public EDBTable {
    private:
        const PredId_t predid;
        EDBLayer *layer;
        const std::string pathfile;
        const int64_t startRange;
        int64_t N;
        std::shared_ptr<Embeddings<double>> emb;
        std::string prefix;
        std::string dictPredName;
        std::shared_ptr<EDBTable> dictTable;

        bool usePredicateMappings;
        std::unordered_map<Term_t, size_t> predIDsMap;
        std::unordered_map<size_t, Term_t> invPredIDsMap;
        std::vector<Term_t> predIDsList;

    public:
        virtual uint8_t getArity() const {
            return 2;
        }

        bool areTermsEncoded() {
            return true;
        }

        EmbTable(PredId_t predid,
                std::string predname,
                EDBLayer *layer,
                std::string pathfile,
                std::string startRange,
                std::string dictPredName);

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

        int getEmbeddingDimension() {
            return emb->getDim();
        }

        Term_t getEntity(uint64_t embid);

        std::shared_ptr<Embeddings<double>> getEmbeddings() {
            return emb;
        }

        uint64_t getStartOffset() {
            return startRange;
        }

        uint64_t getNTerms();

        void releaseIterator(EDBIterator *itr);

        uint64_t getSize();

        ~EmbTable();
};


#endif
