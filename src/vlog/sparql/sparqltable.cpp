#include <vlog/sparql/sparqltable.h>
#include <vlog/sparql/sparqliterator.h>
#include <vlog/inmemory/inmemorytable.h>

#include <nlohmann/json.hpp>
using json = nlohmann::json;

static bool curl_initialized = false;
static int  numTables = 0;

SparqlTable::SparqlTable(PredId_t predid, std::string repository,
        EDBLayer *layer, std::string f, std::string whereBody) :
    predid(predid), repository(repository), layer(layer), whereBody(whereBody) {
        if (! curl_initialized) {
            curl_global_init(CURL_GLOBAL_ALL);
            curl_initialized = true;
        }
        curl = curl_easy_init();
        //Extract fields
        std::stringstream ss(f);
        std::string item;
        while (std::getline(ss, item, ',')) {
            bool alreadyPresent = false;
            for (int i = 0; i < fieldVars.size(); i++) {
                if (item == fieldVars[i]) {
                    LOG(WARNL) << "Repeated variable name " << item << " ignored.";
                    alreadyPresent = true;
                    break;
                }
            }
            if (alreadyPresent) {
                continue;
            }
            this->fieldVars.push_back(item);
        }
	numTables++;
    }


std::string SparqlTable::generateQuery(const Literal &query) {
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
            // Also, the bracketing may depend on the type of the value (which we don't keep track of in VLog).
            // Or should we add bracketing when we put stuff in the dictionary???
            // TODO!!!
            if (val.find('<') == 0 || val.find('"') == 0) {
                binds += " BIND (" + val + " AS ?" + fieldVars[i] + ") ";
            } else {
                binds += " BIND (\"" + val + "\" AS ?" + fieldVars[i] + ") ";
            }
            select += " ?" + fieldVars[i];
            vars.push_back(-1);
        }
    }

    std::string sparqlQuery = select + " WHERE {" + binds + wb + "}";

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

    Term_t row[256];
    uint8_t *pos = query->getPosToCopy();
    const uint8_t npos = query->getNPosToCopy();
    if (posToFilter == NULL || posToFilter->size() == 0) {
        std::vector<uint8_t> sortFields;
        EDBIterator *iter = getSortedIterator(*lit, sortFields);
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

// TODO
size_t SparqlTable::estimateCardinality(const Literal &query) {
    // return getCardinality(query);
    // Just return a high number
    return 100000000;
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
        if (isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
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
    headers = curl_slist_append(headers, "User-Agent: VLog-v1.2.1");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    CURLcode resp = curl_easy_perform(curl);
    LOG(DEBUGL) << "output = " << response.substr(0, 1000) << ((response.size() > 1000) ? " ..." : "");
    json output;
    if (resp == 0) {
        try {
            output = json::parse(response);
            output = output["results"];
            output = output["bindings"];
        } catch(nlohmann::detail::parse_error x) {
            LOG(WARNL) << "Returning empty result; parse error in response";
            LOG(DEBUGL) << "Response = " << response;
        }
    } else {
        std::string em(errorBuffer);
        LOG(WARNL) << "Launching query failed: " << em;
    }
    curl_slist_free_all(headers);
    return output;
}

size_t SparqlTable::getCardinality(const Literal &query) {
    size_t sz = query.getTupleSize();

    if (sz != fieldVars.size()) {
        LOG(WARNL) << "Wrong arity in query for getCardinality";
        return 0;
    }

    std::string key = query.tostring();
    LOG(DEBUGL) << "Get cardinality for " << key;

//    std::string sparqlQuery = "SELECT (COUNT(*) AS ?cnt) WHERE { " + generateQuery(query) + " }";
//    json output = launchQuery(sparqlQuery);
//    json::iterator it = output.begin();
//    if (it != output.end()) {
//        std::string s = (*it)["cnt"]["value"];
//        LOG(DEBUGL) << "Cardinality = " << s;
//        size_t card = std::stoll(s);
//        return card;
//    }
//  This apparently does not work, sometimes causes exceptions in endpoint.

    EDBIterator *eit = getIterator(query);
    size_t count = 0;
    while (eit->hasNext()) {
        eit->next();
        count++;
    }
    delete eit;
    return count;
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

    LOG(DEBUGL) << "GetIterator, query = " << query.tostring();

    size_t sz = query.getTupleSize();

    json output;
    if (sz == fieldVars.size()) {
        std::string key = query.tostring();
        if (cachedTables.count(key) > 0) {
            output = cachedTables[key];
        } else {
            std::string sparqlQuery = generateQuery(query);
            output = launchQuery(sparqlQuery);
            cachedTables[key] = output;
        }
    }
    return new SparqlIterator(output, layer, query, fieldVars);
}

static uint64_t __getKeyFromFields(const std::vector<uint8_t> &fields) {
    assert(fields.size() <= 8);
    uint64_t key = 0;
    for(uint8_t i = 0; i < fields.size(); ++i) {
        uint8_t field = fields[i];
        key = (key << 8) + (uint64_t)(field+1);
    }
    return key;
}

EDBIterator *SparqlTable::getSortedIterator(const Literal &query,
        const std::vector<uint8_t> &fields) {

    size_t sz = query.getTupleSize();
    if (sz != fieldVars.size()) {
        json output;
        return new SparqlIterator(output, layer, query, fieldVars);
    }

    LOG(DEBUGL) << "GetSortedIterator, query = " << query.tostring();

    uint64_t key = 0;
    if (sz <= 8 && query.getNUniqueVars() == sz) {
        // See if we can find it in the cache.
        key = __getKeyFromFields(fields);
        if (cachedSegments.count(key)) {
            auto segment = cachedSegments[key];
            return new InmemoryIterator(segment, predid, fields);
        }
    }

    EDBIterator *it = getIterator(query);
    if (! it->hasNext()) {
        return it;
    }
    std::vector<ColumnWriter *> writers;
    writers.resize(sz);
    for (uint8_t i = 0; i < sz; ++i) {
        writers[i] = new ColumnWriter();
    }
    while (it->hasNext()) {
        it->next();
        for (uint8_t i = 0; i < sz; ++i) {
            writers[i]->add(it->getElementAt(i));
        }
    }
    delete it;

    std::vector<std::shared_ptr<Column>> columns;
    for (uint8_t i = 0; i < sz; ++i) {
        columns.push_back(writers[i]->getColumn());
    }

    std::shared_ptr<Segment> segment = std::shared_ptr<Segment>(new Segment(sz, columns));

    for (uint8_t i = 0; i < sz; ++i) {
        delete writers[i];
    }

    // Map fields. Note that "fields" only counts variables. We need all columns here.
    std::vector<uint8_t> offsets;
    int nConstantsSeen = 0;
    int varNo = 0;
    for (int i = 0; i < query.getTupleSize(); i++) {
        if (! query.getTermAtPos(i).isVariable()) {
            nConstantsSeen++;
        } else {
            offsets.push_back(nConstantsSeen);
        }

    }
    std::vector<uint8_t> newFields;
    for (auto f : fields) {
        newFields.push_back(offsets[f] + f);
    }


    segment = segment->sortBy(&newFields);
    if (sz <= 8 && query.getNUniqueVars() == sz) {
        // put it in the cache.
        cachedSegments[key] = segment;
    }

    return new InmemoryIterator(segment, predid, newFields);
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
    delete itr;
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
    numTables--;
    if (numTables == 0) {
	curl_initialized = false;
	curl_global_cleanup();
    }
}
