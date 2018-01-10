#ifndef _MDLITE_H
#define _MDLITE_H

#include <vlog/column.h>
#include <vlog/sqltable.h>

class MDLiteCon {
    private:
        void *conn;
        bool started;

    public:
        MDLiteCon() {
            started = false;
            conn = NULL;
        }

        bool isStarted() {
            return started;
        }

        void start();

        void *getConnection();

};

class MDLiteTable : public SQLTable {
    private:
        std::vector<string> varnames;
        uint8_t arity;

    public:
        MDLiteTable(string repository, string tablename);

        uint8_t getArity() const;

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

        ~MDLiteTable();
};

#endif
