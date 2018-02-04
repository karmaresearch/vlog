#if MDLITE
#include <vlog/mdlite/mdlitetable.h>

#include <kognac/utils.h>


//MonetDB
#include "embedded.h"

#include <sstream>
#include <string>

//Shared datastructures
MDLiteCon mdconnection;

/*** Methods implementations ***/
MDLiteTable::MDLiteTable(string repository, string tablename) {
    this->tablename = tablename;
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

void MDLiteTable::query(QSQQuery *query, TupleTable *outputTable,
        std::vector<uint8_t> *posToFilter,
        std::vector<Term_t> *valuesToFilter) {
    LOG(ERRORL) << "Not implemented yet";
    throw 10;
}

size_t MDLiteTable::getCardinality(const Literal &q) {
    LOG(ERRORL) << "Not implemented yet";
    throw 10;
}

size_t MDLiteTable::getCardinalityColumn(const Literal &q, uint8_t posColumn) {
    LOG(ERRORL) << "Not implemented yet";
    throw 10;
}

bool MDLiteTable::isEmpty(const Literal &q, std::vector<uint8_t> *posToFilter,
        std::vector<Term_t> *valuesToFilter) {
    LOG(ERRORL) << "Not implemented yet";
    throw 10;
}

EDBIterator *MDLiteTable::getIterator(const Literal &query) {
    LOG(ERRORL) << "Not implemented yet";
    throw 10;
}

EDBIterator *MDLiteTable::getSortedIterator(const Literal &query,
        const std::vector<uint8_t> &fields) {
    LOG(ERRORL) << "Not implemented yet";
    throw 10;
}

bool MDLiteTable::getDictNumber(const char *text, const size_t sizeText,
        uint64_t &id) {
    return singletonDict.getID(text, sizeText, id);
}

bool MDLiteTable::getDictText(const uint64_t id, char *text) {
    return singletonDict.getText(id, text);
}

uint64_t MDLiteTable::getNTerms() {
    return singletonDict.getNTerms();
}

uint64_t MDLiteTable::getSize() {
    LOG(ERRORL) << "Not implemented yet";
    throw 10;
}

uint8_t MDLiteTable::getArity() const {
    return arity;
}

MDLiteTable::~MDLiteTable() {
}

void MDLiteCon::start() {
    if (started) {
        LOG(ERRORL) << "The database is already started";
        throw 10;
    }
    char* err = 0;
    err = monetdb_startup(NULL, 0, 0);
    if (err != 0) {
        LOG(ERRORL) << "Failed starting the database";
        throw 10;
    }

    conn = monetdb_connect();
    if (conn == NULL) {
        LOG(ERRORL) << "Failed creating the connection";
        throw 10;
    }
    started = true;
}

void *MDLiteCon::getConnection() {
    if (!started) {
        LOG(ERRORL) << "First you need to start the database!";
        throw 10;
    }
    return conn;
}
#endif
