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
	bool skipDuplicated;
	bool hasNextChecked;
	bool hasNextValue;
	bool isFirst;
	int vars[128];	// for every position, indicates which variable number to use
	std::vector<uint8_t> sortFields;
	std::vector<std::string> fieldVars;

    public:
        SparqlIterator(const json &qr, EDBLayer *l, const Literal &q, const std::vector<uint8_t> &sf, const std::vector<std::string> &fv);

        bool hasNext();

        void next();

        Term_t getElementAt(const uint8_t p) {
	    return row[p];
	}

        PredId_t getPredicateID() {
	    return query.getPredicate().getId();
	}

        void skipDuplicatedFirstColumn() {
	    skipDuplicated = true;
	}

        void clear() {
	}
};

#endif
