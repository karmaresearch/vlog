#include <vlog/sparql/sparqliterator.h>

SparqlIterator::SparqlIterator(const json &qr, EDBLayer *l, const Literal &q, const std::vector<uint8_t> &sf, const std::vector<std::string> &fv) :
    layer(l), queryResult(qr), query(q),
    skipDuplicated(false), hasNextChecked(false), isFirst(true),
    sortFields(sf), fieldVars(fv) {
    it = queryResult.begin();
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
		std::string val = (*it)[fieldVars[elNo]]["value"];
		uint64_t v;
		LOG(DEBUGL) << "String: " << val;
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
		    std::string val = (*it)[fieldVars[sortFields[i]]]["value"];
		    uint64_t v;
		    LOG(DEBUGL) << "String: " << val;
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
        LOG(ERRORL) << "SparqlIterator::next called without hasNext check";
        throw 10;
    }
    if (! hasNextValue) {
        LOG(ERRORL) << "SparqlIterator::next called while hasNext returned false";
        throw 10;
    }
    for (int i = 0; i < query.getTupleSize(); i++) {
	VTerm t = query.getTermAtPos(i);
	if (t.isVariable()) {
	    std::string val = (*it)[fieldVars[i]]["value"];
	    uint64_t v;
	    LOG(DEBUGL) << "String: " << val;
	    layer->getOrAddDictNumber(val.c_str(), val.size(), v);
	    row[i] = v;

	} else {
	    row[i] = t.getValue();
	}
    }
    it++;
    isFirst = false;
    hasNextChecked = false;
}
