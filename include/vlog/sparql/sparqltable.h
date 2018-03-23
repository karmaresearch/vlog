#ifndef _SPARQL_TABLE_H
#define _SPARQL_TABLE_H

#include <vlog/column.h>
#include <vlog/edbtable.h>
#include <vlog/edbiterator.h>

#include <trident/utils/httpclient.h>

class SparqlTable : public EDBTable {
    private:
        HttpClient::URL endpoint;
        HttpClient client;
        bool isConnected;
	EDBLayer *layer;

        std::string literalToSparql(const Literal &query,
                const std::vector<uint8_t> &fields, bool count);

    public:
        uint8_t getArity() const {
            return 3;
        }

        bool areTermsEncoded() {
            return false;
        }

        SparqlTable(string repository, EDBLayer *layer);

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

        uint64_t getNTerms();

        void releaseIterator(EDBIterator *itr);

        uint64_t getSize();

        ~SparqlTable();
};

#endif
