#ifndef _INMEMORY_H
#define _INMEMORY_H

#include <vlog/column.h>
#include <vlog/edbtable.h>

class InmemoryDict {
    private:
        std::map<uint64_t, string> dict;
        std::map<string, uint64_t> invdict;
        bool isloaded;

    public:
        InmemoryDict() : isloaded(false) {
        }

        bool isDictLoaded() {
            return isloaded;
        }

        void add(uint64_t id, string value) {
            dict.insert(make_pair(id, value));
            invdict.insert(make_pair(value, id));
        }

        bool getText(uint64_t id, char *text);

        bool getID(const char *text, uint64_t sizetext, uint64_t &id);

        uint64_t getNTerms() {
            return dict.size();
        }

        void load(string pathfile);
};

class InmemoryTable : public EDBTable {
    private:
        std::vector<string> varnames;
        uint8_t arity;

    public:
        InmemoryTable(string repository, string tablename);

        uint8_t getArity() const;

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

        uint64_t getNTerms();

        void releaseIterator(EDBIterator *itr);

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

        ~InmemoryTable();
};

#endif
