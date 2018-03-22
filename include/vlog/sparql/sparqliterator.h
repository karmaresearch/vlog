#ifndef _SPARQL_ITR_H
#define _SPARQL_ITR_H

#include <vlog/column.h>
#include <vlog/edbtable.h>
#include <vlog/edbiterator.h>

class SparqlIterator : public EDBIterator {
    private:

    public:
        SparqlIterator();

        bool hasNext();

        void next();

        Term_t getElementAt(const uint8_t p);

        PredId_t getPredicateID();

        void skipDuplicatedFirstColumn();

        void clear();
};

#endif
