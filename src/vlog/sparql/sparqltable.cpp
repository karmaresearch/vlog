#include <vlog/sparql/sparqltable.h>
#include <vlog/sparql/sparqliterator.h>

#include <nlohmann/json.hpp>
using json = nlohmann::json;

static bool curl_initialized = false;

SparqlTable::SparqlTable(string repository, EDBLayer *layer, string f, string whereBody) :
    repository(repository), layer(layer), whereBody(whereBody) {
	if (! curl_initialized) {
	    curl_global_init(CURL_GLOBAL_ALL);
	    curl_initialized = true;
	}
	curl = curl_easy_init();
	//Extract fields
	std::stringstream ss(f);
	std::string item;
	while (std::getline(ss, item, ',')) {
	    this->fieldVars.push_back(item);
	}
    }


std::string SparqlTable::generateQuery(const Literal &query, const std::vector<uint8_t> *fields) {
    //Convert the literal into a sparql query
    // First, analyze query: and determine which variable goes where.
    std::vector<uint8_t> variables;
    std::vector<int> vars;
    std::string binds = "";
    std::string select = "SELECT DISTINCT";
    std::string wb = whereBody;
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
		select += " ?" + fieldVars[i];
            } else {
		for (int j = 0; j < i; j++) {
		    VTerm t2 = query.getTermAtPos(j);
		    if (t2.isVariable() && t2.getId() == v) {
			// Duplicate variable in the query.
			select += " (?" + fieldVars[j] + " AS ?" + fieldVars[i] + ")";
			//Replace all occurrences of this variable with its replacement.
			std::string from = "?" + fieldVars[i];
			std::string to = "?" + fieldVars[j];
			size_t start_pos = 0;
			while ((start_pos = wb.find(from, start_pos)) != std::string::npos) {
			     wb.replace(start_pos, from.length(), to);
			     start_pos += to.length();
			}
			break;
		    }
		}
	    }
        } else {
	    std::string val = layer->getDictText(t.getValue());
	    // TODO: escape needed for string? And if value is not found in dictionary,
	    // what to do then?
	    binds += " BIND (\"" + val + "\" AS ?" + fieldVars[i] + ") ";
	    vars.push_back(-1);
	}
    }

    std::string sparqlQuery = select + " WHERE {" + binds + wb + "}";

    if (fields != NULL && fields->size() > 0) {
	sparqlQuery += " ORDER BY";
	for (int i = 0; i < fields->size(); i++) {
	    uint8_t fieldNo = (*fields)[i];
	    sparqlQuery += " ?" + fieldVars[fieldNo];
	}
    }
    return sparqlQuery;
}

void SparqlTable::query(QSQQuery *query, TupleTable *outputTable,
        std::vector<uint8_t> *posToFilter,
        std::vector<Term_t> *valuesToFilter) {

    const Literal *lit = query->getLiteral();
    size_t sz = lit->getTupleSize();

    if (sz != fieldVars.size()) {
	LOG(ERRORL) << "Wrong tuple size in query";
	throw 10;
    }

    Term_t row[128];
    uint8_t *pos = query->getPosToCopy();
    const uint8_t npos = query->getNPosToCopy();
    if (posToFilter == NULL || posToFilter->size() == 0) {
	EDBIterator *iter = getIterator(*lit);
        while (iter->hasNext()) {
            iter->next();
            for (uint8_t i = 0; i < npos; ++i) {
                row[i] = iter->getElementAt(pos[i]);
            }
            outputTable->addRow(row);
        }
        return;
    }

    //TODO: could this be implemented somehow with VALUES?

    LOG(ERRORL) << "Not implemented yet: SparqlTable::query with posFilter";
    throw 10;
}

size_t SparqlTable::estimateCardinality(const Literal &query) {
    return getCardinality(query);
}

//Inspired by https://stackoverflow.com/questions/154536/encode-decode-urls-in-c#17708801
std::string escape(const std::string &s) {
    std::ostringstream escaped;
    escaped.fill('0');
    escaped << std::hex;

    std::string::const_iterator n;
    for (std::string::const_iterator i = s.cbegin(),
            n = s.cend(); i != n; ++i) {
        std::string::value_type c = (*i);
        if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
            escaped << c;
            continue;
        }
        // Any other characters are percent-encoded
        escaped << std::uppercase;
        escaped << '%' << std::setw(2) << int((unsigned char) c);
        escaped << std::nouppercase;
    }

    return escaped.str();
}

size_t writeFunction(void *ptr, size_t size, size_t nmemb, std::string* data) {
    data->append((char*) ptr, size * nmemb);
    return size * nmemb;
}

json SparqlTable::launchQuery(std::string sparqlQuery) {

    char errorBuffer[CURL_ERROR_SIZE];
    std::string request = repository;

    request += "?query=" + escape(sparqlQuery);
    LOG(DEBUGL) << "Launching the remote query " << sparqlQuery;
    LOG(DEBUGL) << "Request = " << request;
    curl_easy_setopt(curl, CURLOPT_URL, request.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeFunction);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 50L);
    curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errorBuffer);
    std::string rheaders;
    std::string response;
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, &rheaders);
    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, "Accept: application/sparql-results+json");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    
    CURLcode resp = curl_easy_perform(curl);
    json output;
    if (resp == 0) {
	output = json::parse(response);
	output = output["results"];
	output = output["bindings"];
    } else {
	std::string em(errorBuffer);
	LOG(WARNL) << "Launching query failed: " << em;
    }
    return output;
}

size_t SparqlTable::getCardinality(const Literal &query) {
    size_t sz = query.getTupleSize();

    if (sz != fieldVars.size()) {
	LOG(WARNL) << "Wrong arity in query for getCardinality";
	return 0;
    }

    std::string sparqlQuery = "SELECT (COUNT(*) AS ?cnt) WHERE { " + generateQuery(query) + " }";
    json output = launchQuery(sparqlQuery);
    json::iterator it = output.begin();
    if (it != output.end()) {
	std::string s = (*it)["cnt"]["value"];
	LOG(DEBUGL) << "Cardinality = " << s;
	return std::stoll(s);
    }
    return 0;
}

size_t SparqlTable::getCardinalityColumn(const Literal &query,
        uint8_t posColumn) {

    size_t sz = query.getTupleSize();

    if (sz != fieldVars.size()) {
	LOG(WARNL) << "Wrong arity in query for getCardinalityColumn";
	return 0;
    }

    if (posColumn >= fieldVars.size()) {
	LOG(WARNL) << "Wrong posColumn for getCardinalityColumn";
	return 0;
    }

    std::string sparqlQuery = "SELECT (COUNT(DISTINCT ?" + fieldVars[posColumn] + ") AS ?cnt) WHERE { " + generateQuery(query) + " }";
    json output = launchQuery(sparqlQuery);
    json::iterator it = output.begin();
    if (it != output.end()) {
	std::string s = (*it)["cnt"]["value"];
	LOG(DEBUGL) << "CardinalityColumn = " << s;
	return std::stoll(s);
    }
    return 0;
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

    size_t sz = query.getTupleSize();

    if (sz != fieldVars.size()) {
	LOG(WARNL) << "Wrong arity in query for getSortedIterator";
	return 0;
    }

    std::string sparqlQuery = generateQuery(query, &fields);
    json output = launchQuery(sparqlQuery);
    return new SparqlIterator(output, layer, query, fields, fieldVars);
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
    std::string sparqlQuery = "SELECT (COUNT(*) AS ?cnt) WHERE { " + whereBody + " }";
    json output = launchQuery(sparqlQuery);
    json::iterator it = output.begin();
    if (it != output.end()) {
	std::string s = (*it)["cnt"]["value"];
	LOG(DEBUGL) << "getSize = " << s;
	return std::stoll(s);
    }
    return 0;
}

SparqlTable::~SparqlTable() {
    curl_easy_cleanup(curl);
}
