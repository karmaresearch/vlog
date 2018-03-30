#include <vlog/sparql/sparqliterator.h>

SparqlIterator::SparqlIterator(const json &qr, EDBLayer *l, const Literal &q, const std::vector<std::string> &fv) :
    layer(l), queryResult(qr), query(q), hasNextChecked(false), hasNextValue(false), fieldVars(fv) {
    it = queryResult.begin();
}

std::string getString(json &jref) {
    std::string val = jref["value"];
    std::string type = jref["type"];
    if (type == "uri") {
	val = "<" + val + ">";
    }
    return val;
}

bool SparqlIterator::hasNext() {
    hasNextValue = (it != queryResult.end());
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
	    std::string val = getString((*it)[fieldVars[i]]);
	    uint64_t v;
	    layer->getOrAddDictNumber(val.c_str(), val.size(), v);
	    row[i] = v;

	} else {
	    row[i] = t.getValue();
	}
    }
    it++;
    hasNextChecked = false;
}
