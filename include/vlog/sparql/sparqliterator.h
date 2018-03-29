#ifndef _SPARQL_ITR_H
#define _SPARQL_ITR_H

#include <vlog/column.h>
#include <vlog/edbtable.h>
#include <vlog/edbiterator.h>

#include <nlohmann/json.hpp>
using json = nlohmann::json;

class SparqlIterator : public EDBIterator {
    private:
	EDBLayer *layer;
	json queryResult;
	json::iterator it;
	uint64_t row[128];
	Literal query;
	bool hasNextChecked;
	bool hasNextValue;
	int vars[128];	// for every position, indicates which variable number to use
	std::vector<std::string> fieldVars;

    public:
        SparqlIterator(const json &qr, EDBLayer *l, const Literal &q, const std::vector<std::string> &fv);

        bool hasNext();

        void next();

        Term_t getElementAt(const uint8_t p) {
	    return row[p];
	}

        PredId_t getPredicateID() {
	    return query.getPredicate().getId();
	}

        void skipDuplicatedFirstColumn() {
	    throw 10;	// Should not happen on unsorted iterator.
	}

        void clear() {
	}
};

#endif
