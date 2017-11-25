#include <vlog/inmemory/inmemorytable.h>

#include <kognac/utils.h>

InmemoryDict singletonDict;

void InmemoryDict::load(string pathfile) {
    if (isloaded) {
        LOG(ERRORL) << "The dictionary is already loaded!";
        throw 10;
    }
    if (!Utils::exists(pathfile)) {
        LOG(ERRORL) << "The file " << pathfile << " does not exist";
        throw 10;
    }
    ifstream ifs;
    ifs.open(pathfile);
    string line;
    while (std::getline(ifs, line)) {
        auto delim = line.find('\t');
        string number = line.substr(0, delim);
        string value = line.substr(delim + 1);
        uint64_t id = std::stol(number);
        singletonDict.add(id, value);
    }
    ifs.close();
    isloaded = true;
}

bool InmemoryDict::getID(const char *text, uint64_t sizetext, uint64_t &id) {
    string v = string(text, sizetext);
    if (invdict.count(v)) {
        id = invdict[v];
        return true;
    } else {
        return false;
    }
}

bool InmemoryDict::getText(uint64_t id, char *text) {
    if (dict.count(id)) {
        auto &str = dict[id];
        strcpy(text, str.c_str());
        return true;
    } else {
        return false;
    }
}

InmemoryTable::InmemoryTable(string repository, string tablename) {
    arity = 0;
    string schemaFile = repository + "/" + tablename + ".schema";
    ifstream ifs;
    ifs.open(schemaFile);
    string line;
    while (std::getline(ifs, line)) {
        auto delim = line.find(':');
        string varname = line.substr(0, delim);
        varnames.push_back(varname);
        arity++;
    }
    ifs.close();

    //Load the dictionary
    if (!singletonDict.isDictLoaded()) {
        singletonDict.load(repository + "/dict");
    }

    //Load the table in the database
    ifs.open(repository + "/" + tablename + ".csv");

    ifs.close();
}

void InmemoryTable::query(QSQQuery *query, TupleTable *outputTable,
        std::vector<uint8_t> *posToFilter,
        std::vector<Term_t> *valuesToFilter) {
    LOG(ERRORL) << "Not implemented yet";
    throw 10;
}

size_t InmemoryTable::getCardinality(const Literal &q) {
    LOG(ERRORL) << "Not implemented yet";
    throw 10;
}

size_t InmemoryTable::getCardinalityColumn(const Literal &q, uint8_t posColumn) {
    LOG(ERRORL) << "Not implemented yet";
    throw 10;
}

bool InmemoryTable::isEmpty(const Literal &q, std::vector<uint8_t> *posToFilter,
        std::vector<Term_t> *valuesToFilter) {
    LOG(ERRORL) << "Not implemented yet";
    throw 10;
}

EDBIterator *InmemoryTable::getIterator(const Literal &query) {
    LOG(ERRORL) << "Not implemented yet";
    throw 10;
}

EDBIterator *InmemoryTable::getSortedIterator(const Literal &query,
        const std::vector<uint8_t> &fields) {
    LOG(ERRORL) << "Not implemented yet";
    throw 10;
}

std::vector<std::shared_ptr<Column>> InmemoryTable::checkNewIn(const Literal &l1,
        std::vector<uint8_t> &posInL1,
        const Literal &l2,
        std::vector<uint8_t> &posInL2) {
    LOG(ERRORL) << "Not implemented yet";
    throw 10;
}

std::vector<std::shared_ptr<Column>> InmemoryTable::checkNewIn(
        std::vector <
        std::shared_ptr<Column >> &checkValues,
        const Literal &l2,
        std::vector<uint8_t> &posInL2) {
    LOG(ERRORL) << "Not implemented yet";
    throw 10;
}

std::shared_ptr<Column> InmemoryTable::checkIn(
        std::vector<Term_t> &values,
        const Literal &l2,
        uint8_t posInL2,
        size_t &sizeOutput) {
    LOG(ERRORL) << "Not implemented yet";
    throw 10;
}

void InmemoryTable::releaseIterator(EDBIterator *itr) {
    LOG(ERRORL) << "Not implemented yet";
    throw 10;
}

size_t InmemoryTable::estimateCardinality(const Literal &query) {
    LOG(ERRORL) << "Not implemented yet";
    throw 10;
}

bool InmemoryTable::getDictNumber(const char *text, const size_t sizeText,
        uint64_t &id) {
    return singletonDict.getID(text, sizeText, id);
}

bool InmemoryTable::getDictText(const uint64_t id, char *text) {
    return singletonDict.getText(id, text);
}

uint64_t InmemoryTable::getNTerms() {
    return singletonDict.getNTerms();
}

uint8_t InmemoryTable::getArity() const {
    return arity;
}

InmemoryTable::~InmemoryTable() {
}
