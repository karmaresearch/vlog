#include <vlog/inmemory/inmemorytable.h>
#include <vlog/fcinttable.h>

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

InmemoryTable::InmemoryTable(string repository, string tablename,
        PredId_t predid) {
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
    string tablefile = repository + "/" + tablename + ".csv";
    if (Utils::exists(tablefile)) {
        SegmentInserter inserter(arity);
        ifs.open(tablefile);
        string line;
        std::unique_ptr<Term_t> row = std::unique_ptr<Term_t>(
                new Term_t[arity]);
        while(std::getline(ifs, line)) {
            //Parse the row
            for(uint8_t i = 0; i < arity; ++i) {
                auto delim = line.find(',');
                string sn = line.substr(0, delim);
                row.get()[i] = stol(sn);
                line = line.substr(delim + 1);
            }
            inserter.addRow(row.get());
        }
        segment = inserter.getSegment();
        ifs.close();
    } else {
        segment = NULL;
    }
}

void InmemoryTable::query(QSQQuery *query, TupleTable *outputTable,
        std::vector<uint8_t> *posToFilter,
        std::vector<Term_t> *valuesToFilter) {
    LOG(ERRORL) << "Not implemented yet";
    throw 10;
}

bool InmemoryTable::isEmpty(const Literal &q, std::vector<uint8_t> *posToFilter,
        std::vector<Term_t> *valuesToFilter) {
    if (posToFilter == NULL) {
        return segment == NULL;
    } else {
        LOG(ERRORL) << "Not implemented yet";
        throw 10;
    }
}

size_t InmemoryTable::getCardinality(const Literal &q) {
    if (q.getNUniqueVars() == q.getTupleSize()) {
        if (segment == NULL) {
            return 0;
        } else {
            if (arity == 0) {
                return 1;
            } else {
                return segment->getNRows();
            }
        }
    } else {
        LOG(ERRORL) << "Not implemented yet";
        throw 10;
    }
}

size_t InmemoryTable::getCardinalityColumn(const Literal &q, uint8_t posColumn) {
    LOG(ERRORL) << "Not implemented yet";
    throw 10;
}

void _literal2filter(const Literal &query, std::vector<uint8_t> &posVarsToCopy,
        std::vector<uint8_t> &posConstantsToFilter,
        std::vector<Term_t> &valuesConstantsToFilter,
        std::vector<std::pair<uint8_t, uint8_t>> &repeatedVars) {
    for(uint8_t i = 0; i < query.getTupleSize(); ++i) {
        auto term = query.getTermAtPos(i);
        if (term.isVariable()) {
            bool unique = true;
            for(uint8_t j = 0; j < posVarsToCopy.size(); ++j) {
                auto var = query.getTermAtPos(j);
                if (var.getId() == term.getId()) {
                    repeatedVars.push_back(std::make_pair(i, j));
                    unique = false;
                    break;
                }
            }
            if (unique)
                posVarsToCopy.push_back(i);
        } else { //constant
            posConstantsToFilter.push_back(i);
            valuesConstantsToFilter.push_back(term.getValue());
        }
    }
}

EDBIterator *InmemoryTable::getIterator(const Literal &q) {
    if (q.getNUniqueVars() == q.getTupleSize()) {
        return new InmemoryIterator(segment, predid);
    } else {
        LOG(ERRORL) << "Not implemented yet";
        throw 10;
    }
}

EDBIterator *InmemoryTable::getSortedIterator(const Literal &query,
        const std::vector<uint8_t> &fields) {
    if (query.getNUniqueVars() == query.getTupleSize()) {
        if (fields.size() >= 8) {
            auto sortedSegment = segment->sortBy(&fields);
            return new InmemoryIterator(sortedSegment, predid);
        } else {
            uint64_t sortedKey = 0;
            for(uint8_t i = 0; i < fields.size(); ++i) {
                uint8_t field = fields[i];
                sortedKey += ((uint64_t)(field+1)) << 8;
            }
            if (cachedSortedSegments.count(sortedKey)) {
                return new InmemoryIterator(cachedSortedSegments[sortedKey], predid);
            } else {
                auto sortedSegment = segment->sortBy(&fields);
                cachedSortedSegments[sortedKey] = sortedSegment;
                return new InmemoryIterator(sortedSegment, predid);
            }
        }
    } else {
        std::shared_ptr<const Segment> sortedSegment;
        if (fields.size() >=8) {
            sortedSegment = segment->sortBy(&fields);
        } else {
            //See if I have it in the cache
            uint64_t sortedKey = 0;
            for(uint8_t i = 0; i < fields.size(); ++i) {
                uint8_t field = fields[i];
                sortedKey += ((uint64_t)(field+1)) << 8;
            }
            if (cachedSortedSegments.count(sortedKey)) {
                sortedSegment = cachedSortedSegments[sortedKey];
            } else {
                sortedSegment = segment->sortBy(&fields);
                cachedSortedSegments[sortedKey] = sortedSegment;
            }
        }

        //Filter the table
        InmemoryFCInternalTable t(arity, 0, false, segment);
        std::vector<uint8_t> posVarsToCopy;
        std::vector<uint8_t> posConstantsToFilter;
        std::vector<Term_t> valuesConstantsToFilter;
        std::vector<std::pair<uint8_t, uint8_t>> repeatedVars;
        _literal2filter(query, posVarsToCopy, posConstantsToFilter,
                valuesConstantsToFilter, repeatedVars);
        auto fTable = t.filter(posVarsToCopy.size(), posVarsToCopy.data(),
                posConstantsToFilter.size(), posConstantsToFilter.data(),
                valuesConstantsToFilter.data(), repeatedVars.size(),
                repeatedVars.data(), 1); //no multithread
        auto filteredSegment = ((InmemoryFCInternalTable*)fTable.get())->getUnderlyingSegment();
        return new InmemoryIterator(filteredSegment, predid);
    }
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
    delete itr;
}

size_t InmemoryTable::estimateCardinality(const Literal &query) {
    return getCardinality(query);
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

bool InmemoryIterator::hasNext() {
    return iterator->hasNext();
}

void InmemoryIterator::next() {
    iterator->next();
}

Term_t InmemoryIterator::getElementAt(const uint8_t p) {
    return iterator->get(p);
}

PredId_t InmemoryIterator::getPredicateID() {
    return predid;
}

void InmemoryIterator::skipDuplicatedFirstColumn() {
    LOG(ERRORL) << "Not implemented yet";
    throw 10;
}

void InmemoryIterator::clear() {
}
