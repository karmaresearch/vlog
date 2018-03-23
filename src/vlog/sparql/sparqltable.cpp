#include <vlog/sparql/sparqltable.h>
#include <vlog/sparql/sparqliterator.h>

#include <nlohmann/json.hpp>
using json = nlohmann::json;

SparqlTable::SparqlTable(string repository, EDBLayer *layer) :
    endpoint(HttpClient::parse(repository)),
    client(endpoint.host, endpoint.port), layer(layer) {
        isConnected = client.connect();
        if (!isConnected) {
            LOG(WARNL) << "Client failed in connecting to " << repository;
        }
    }


void extractRows(std::string queryResult, json &output) {
    output = json::parse(queryResult);
    output = output["results"];
    output = output["bindings"];
}

std::string SparqlTable::literalToSparql(const Literal &query,
        const std::vector<uint8_t> &fields, bool count) {
    //Convert the literal into a sparql query
    //
    // First, analyze query: and determine which variable goes where.
    std::vector<uint8_t> variables;
    std::vector<int> vars;
    for (int i = 0; i < query.getTupleSize(); i++) {
        VTerm t = query.getTermAtPos(i);
        if (t.isVariable()) {
            uint8_t v = t.getId();
            bool found = false;
            for (int j = 0; j < variables.size(); j++) {
                if (variables[j] == v) {
                    vars.push_back(j);
                    found = true;
                    break;
                }
            }
            if (! found) {
                vars.push_back(variables.size());
                variables.push_back(v);
            }
        } else {
	    vars.push_back(-1);
	}
    }

    std::string output = "SELECT ";
    if (count) {
	output += "(COUNT(*) as ?cnt) ";
    } else {
	output += "DISTINCT ";
	for (int i = 0; i < variables.size(); i++) {
	    output += "?x" + to_string(i) + " ";
	}
    }
    output += "{";
    for (int i = 0; i < query.getTupleSize(); i++) {
        VTerm t = query.getTermAtPos(i);
        if (t.isVariable()) {
	    output += " ?x" + to_string(vars[i]) + " ";
	} else {
	    // TODO: escape needed for string? And if value is not found in dictionary,
	    // what to do then?
	    std::string v = layer->getDictText(t.getValue());
	    if (v == "") {
		// This is almost certainly useless.
		v = to_string(t.getValue());
	    }
	    output += " " + v + " ";
	}
    }
    output += "}";
    if (! count) {
	/* TODO! ORDER BY clause seems to cause timeouts or something in server.
	std::vector<std::string> sortVars;
	for (int i = 0; i < fields.size(); i++) {
	    uint8_t fieldNo = fields[i];
	    VTerm t = query.getTermAtPos(fieldNo);
	    if (! t.isVariable()) {
		continue;
	    }
	    for (int j = 0; j < variables.size(); j++) {
		if (t.getId() == variables[j]) {
		    sortVars.push_back("?x" + to_string(j));
		    break;
		}
	    }
	}
	if (sortVars.size() > 0) {
	    output += " ORDER BY";
	    for (int i = 0; i < sortVars.size(); i++) {
		output += " " + sortVars[i];
	    }
	}
	*/
    }
    return output;
}

void SparqlTable::query(QSQQuery *query, TupleTable *outputTable,
        std::vector<uint8_t> *posToFilter,
        std::vector<Term_t> *valuesToFilter) {
    Term_t row[3];
    const Literal *lit = query->getLiteral();
    uint8_t *pos = query->getPosToCopy();
    const uint8_t npos = query->getNPosToCopy();
    size_t sz = lit->getTupleSize();
    EDBIterator *iter = getIterator(*lit);
    if (posToFilter == NULL || posToFilter->size() == 0) {
        while (iter->hasNext()) {
            iter->next();
            for (uint8_t i = 0; i < npos; ++i) {
                row[i] = iter->getElementAt(pos[i]);
            }
            outputTable->addRow(row);
        }
        return;
    }

    LOG(ERRORL) << "Not implemented yet: SparqlTable::query with posFilter";
    throw 10;
}

size_t SparqlTable::estimateCardinality(const Literal &query) {
    return getCardinality(query);
}

size_t SparqlTable::getCardinality(const Literal &query) {
    // Not efficient, just to get it to run ... TODO!
    std::vector<uint8_t> fields;
    std::string sparqlQuery = literalToSparql(query, fields, true);
    std::string request = endpoint.protocol + "://" + endpoint.host + endpoint.path;
    request += "?query=" + HttpClient::escape(sparqlQuery);
    LOG(DEBUGL) << "Launching the remote query " << sparqlQuery;
    std::string headers;
    std::string response;
    std::string format = "application/sparql-results+json";
    bool resp = client.get(request, headers, response, format);
    if (resp) {
	json output;
	extractRows(response, output);
	json::iterator it = output.begin();
	if (it != output.end()) {
	    std::string s = (*it)["cnt"]["value"];
	    LOG(DEBUGL) << "Cardinality = " << s;
	    return std::stoll(s);
	}
	return 0;
    } else {
        return 0;
    }
}

size_t SparqlTable::getCardinalityColumn(const Literal &query,
        uint8_t posColumn) {
    std::vector<uint8_t> fields;
    fields.push_back(posColumn);
    // probably not efficient... TODO
    EDBIterator *iter = getSortedIterator(query, fields);
    size_t cnt = 0;
    int64_t prev = -1;
    while (iter->hasNext()) {
	int64_t val = iter->getElementAt(posColumn);
	if (val != prev) {
	    cnt++;
	    prev = val;
	}
        iter->next();
    }
    iter->clear();
    delete iter;
    return cnt;
}

bool SparqlTable::isEmpty(const Literal &query, std::vector<uint8_t> *posToFilter,
        std::vector<Term_t> *valuesToFilter) {
    //TODO
    return false;
}

EDBIterator *SparqlTable::getIterator(const Literal &query) {
    std::vector<uint8_t> sorting;
    return getSortedIterator(query, sorting);
}

EDBIterator *SparqlTable::getSortedIterator(const Literal &query,
        const std::vector<uint8_t> &fields) {
    std::string sparqlQuery = literalToSparql(query, fields, false);
    std::string request = endpoint.protocol + "://" + endpoint.host + endpoint.path;
    request += "?query=" + HttpClient::escape(sparqlQuery);
    LOG(DEBUGL) << "Launching the remote query " << request;
    std::string headers;
    std::string response;
    std::string format = "application/sparql-results+json";
    bool resp = client.get(request, headers, response, format);
    json output;
    if (resp) {
	extractRows(response, output);
    } else {
        LOG(WARNL) << "Failed request. Returning an empty iterator ...";
    }
    return new SparqlIterator(output, layer, query, fields);
}

bool SparqlTable::getDictNumber(const char *text, const size_t sizeText,
        uint64_t &id) {
    return false;
}

bool SparqlTable::getDictText(const uint64_t id, char *text) {
    return false;
}

bool SparqlTable::getDictText(const uint64_t id, std::string &text) {
    return false;
}

uint64_t SparqlTable::getNTerms() {
    return 0;
}

void SparqlTable::releaseIterator(EDBIterator *itr) {
}

uint64_t SparqlTable::getSize() {
    //TODO. Can we ask the endpoint?
    return 0;
}

SparqlTable::~SparqlTable() {
    client.disconnect();
}
