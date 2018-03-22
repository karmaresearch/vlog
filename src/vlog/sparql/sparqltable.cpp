#include <vlog/sparql/sparqltable.h>
#include <trident/utils/json.h>

SparqlTable::SparqlTable(string repository) :
    endpoint(HttpClient::parse(repository)),
    client(endpoint.host, endpoint.port) {
        isConnected = client.connect();
        if (!isConnected) {
            LOG(WARNL) << "Client failed in connecting to " << repository;
        }
    }


std::string SparqlTable::literalToSparql(const Literal &query,
        const std::vector<uint8_t> &fields) {
    //Convert the literal into a sparql query
    std::string output = "SELECT ";
    bool var1 = query.getTermAtPos(0).isVariable();
    bool var2 = query.getTermAtPos(1).isVariable();
    bool var3 = query.getTermAtPos(2).isVariable();
    if (var1) {
        output += "?x ";
    }
    if (var2) {
        output += "?y ";
    }
    if (var3) {
        output += "?z ";
    }
    output += " {";
    if (var1) {
        output += " ?x ";
    } else {
        std::string v1 = to_string(query.getTermAtPos(0).getValue());
        output += " " + v1 + " ";
    }
    if (var2) {
        output += " ?y ";
    } else {
        std::string v2 = to_string(query.getTermAtPos(1).getValue());
        output += " " + v2 + " ";
    }
    if (var3) {
        output += " ?z ";
    } else {
        std::string v3 = to_string(query.getTermAtPos(2).getValue());
        output += " " + v3 + " ";
    }
    output += "}";
    //TODO: adding the sorting
    return output;
}

void SparqlTable::query(QSQQuery *query, TupleTable *outputTable,
        std::vector<uint8_t> *posToFilter,
        std::vector<Term_t> *valuesToFilter) {
    LOG(ERRORL) << "Method not supported (SPARQLTable)";
    throw 10;
}

size_t SparqlTable::estimateCardinality(const Literal &query) {
    //TODO
    return 10;
}

size_t SparqlTable::getCardinality(const Literal &query) {
    //TODO
    return 10;
}

size_t SparqlTable::getCardinalityColumn(const Literal &query,
        uint8_t posColumn) {
    //TODO
    return 10;
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
    std::string sparqlQuery = literalToSparql(query, fields);
    std::string request = endpoint.protocol + "://" + endpoint.host + endpoint.path;
    request += "?query=" + HttpClient::escape(sparqlQuery);
    LOG(DEBUGL) << "Launching the remote query " << request;
    std::string headers;
    std::string response;
    std::string format = "application/sparql-results+json";
    bool resp = client.get(request, headers, response, format);
    if (resp) {
        JSON output;
        JSON::read(response, output);
        //TODO Parse the response which should be a JSON file and construct the iterator
    } else {
        LOG(WARNL) << "Failed request. Returning an empty iterator ...";
        //TODO: returning an empty iterator
    }

    //TODO: return an iterator from the json results
}

bool SparqlTable::getDictNumber(const char *text, const size_t sizeText,
        uint64_t &id) {
    LOG(ERRORL) << "Dictionary is not supported in a SPARQL table";
    throw 10;
}

bool SparqlTable::getDictText(const uint64_t id, char *text) {
    LOG(ERRORL) << "Dictionary is not supported in a SPARQL table";
    throw 10;
}

bool SparqlTable::getDictText(const uint64_t id, std::string &text) {
    LOG(ERRORL) << "Dictionary is not supported in a SPARQL table";
    throw 10;
}

uint64_t SparqlTable::getNTerms() {
    LOG(ERRORL) << "Dictionary is not supported in a SPARQL table";
    throw 10;
}

void SparqlTable::releaseIterator(EDBIterator *itr) {
}

uint64_t SparqlTable::getSize() {
    //TODO
}

SparqlTable::~SparqlTable() {
    client.disconnect();
}
