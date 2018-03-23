#include <vlog/sparql/sparqliterator.h>

SparqlIterator::SparqlIterator(const json &qr, EDBLayer *l, const Literal &q, const std::vector<uint8_t> &sf) :
    layer(l), queryResult(qr), query(q),
    skipDuplicated(false), hasNextChecked(false), isFirst(true),
    sortFields(sf) {
    it = queryResult.begin();
    // Analyze query: fill in constants in row, and determine which variable goes where.
    std::vector<uint8_t> variables;
    for (int i = 0; i < q.getTupleSize(); i++) {
	VTerm t = q.getTermAtPos(i);
	if (t.isVariable()) {
	    uint8_t v = t.getId();
	    bool found = false;
	    for (int j = 0; j < variables.size(); j++) {
		if (variables[j] == v) {
		    vars[i] = j;
		    found = true;
		    break;
		}
	    }
	    if (! found) {
		vars[i] = variables.size();
		variables.push_back(v);
	    }
	} else {
	    row[i] = t.getValue();
	    vars[i] = -1;
	}
    }
}

bool SparqlIterator::hasNext() {
    if (hasNextChecked) {
        return hasNextValue;
    }
    if (isFirst || ! skipDuplicated) {
	hasNextValue = (it != queryResult.end());
    } else {
	int elNo = 0;
	if (sortFields.size() == 1) {
	    elNo = sortFields[0];
	}
	if (sortFields.size() <= 1) {
            Term_t oldval = getElementAt(elNo);
            bool stop = false;
            while (! stop && it != queryResult.end()) {
                // Note that it points at the next value.
		std::string val = (*it)["x" + to_string(vars[elNo])]["value"];
		uint64_t v;
		layer->getOrAddDictNumber(val.c_str(), val.size(), v);
                if (v != oldval) {
                    stop = true;
                } else {
		    it++;
		}
            }
            hasNextValue = stop;
        } else {
            std::vector<Term_t> oldval;
            for (int i = 0; i < sortFields.size(); i++) {
                oldval.push_back(getElementAt(sortFields[i]));
            }
            bool stop = false;
            while (! stop && it != queryResult.end()) {
                for (int i = 0; i < sortFields.size(); i++) {
		    std::string val = (*it)["x" + to_string(vars[sortFields[i]])]["value"];
		    uint64_t v;
		    layer->getOrAddDictNumber(val.c_str(), val.size(), v);
                    if (oldval[i] != v) {
                        stop = true;
                        break;
                    }
                }
		if (! stop) {
		    it++;
		}
            }
            hasNextValue = stop;
	}
    }
    hasNextChecked = true;
    return hasNextValue;
}

void SparqlIterator::next() {
        if (! hasNextChecked) {
        LOG(ERRORL) << "InmemoryIterator::next called without hasNext check";
        throw 10;
    }
    if (! hasNextValue) {
        LOG(ERRORL) << "InmemoryIterator::next called while hasNext returned false";
        throw 10;
    }
    for (int i = 0; i < query.getTupleSize(); i++) {
	if (vars[i] != -1) {
	    std::string val = (*it)["x" + to_string(vars[i])]["value"];
	    uint64_t v;
	    layer->getOrAddDictNumber(val.c_str(), val.size(), v);
	    row[i] = v;
	}
    }
    it++;
    isFirst = false;
    hasNextChecked = false;
}
