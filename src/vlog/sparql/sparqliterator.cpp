#include <vlog/sparql/sparqliterator.h>

SparqlIterator::SparqlIterator(const json &qr, EDBLayer *l, const Literal &q, const std::vector<std::string> &fv) :
    layer(l), queryResult(qr), query(q), hasNextChecked(false), hasNextValue(false), fieldVars(fv) {
    it = queryResult.begin();
}

std::string getString(json &jref) {
    try {
        std::string val = jref["value"];
        std::string type = jref["type"];
        if (type == "uri") {
            val = "<" + val + ">";
        } else if (type == "literal") {
            if (jref.count("datatype") > 0) {
                std::string datatype = jref["datatype"];
                val = "\"" + val + "\"^^<" + datatype +">";
            } else {
                if (jref.count("xml:lang") > 0) {
                    std::string languageTag = jref["xml:lang"];
                    val = "\"" + val + "\"@" + languageTag;
                } else {
                    val = "\"" + val + "\"^^<http://www.w3.org/2001/XMLSchema#string>";
                }
            }
        }
        return val;
    } catch(nlohmann::detail::type_error) {
        // This may happen on null values, for instance when the query contains OPTIONAL.
        // I don't know if this is the "right" way to deal with it, but surely this is better than have
        // vlog terminate.
        return "__NULL__";
    }
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
