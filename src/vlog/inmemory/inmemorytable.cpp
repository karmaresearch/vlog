#include <vlog/inmemory/inmemorytable.h>
#include <vlog/fcinttable.h>

#include <kognac/utils.h>

InmemoryDict singletonDict;

//void InmemoryDict::load(string pathfile) {
//    if (isloaded) {
//        LOG(ERRORL) << "The dictionary is already loaded!";
//        throw 10;
//    }
//    if (!Utils::exists(pathfile)) {
//        LOG(ERRORL) << "The file " << pathfile << " does not exist";
//        throw 10;
//    }
//    ifstream ifs;
//    ifs.open(pathfile);
//    string line;
//    while (std::getline(ifs, line)) {
//        auto delim = line.find('\t');
//        string number = line.substr(0, delim);
//        string value = line.substr(delim + 1);
//        uint64_t id = std::stol(number);
//        singletonDict.add(id, value);
//    }
//    ifs.close();
//    isloaded = true;
//}
//

void dump() {
    ofstream ofs;
    ofs.open("dict", ofstream::out | ofstream::trunc);
    for (uint64_t i = 1; i <= singletonDict.getNTerms(); i++) {
	ofs << i << "\t" << singletonDict.get(i) << "\n";
    }
    ofs.close();
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
//    string schemaFile = repository + "/" + tablename + ".schema";
//    ifstream ifs;
//    ifs.open(schemaFile);
//    string line;
//    while (std::getline(ifs, line)) {
//        auto delim = line.find(':');
//        string varname = line.substr(0, delim);
//        varnames.push_back(varname);
//        arity++;
//    }
//    ifs.close();
//
    //Load the dictionary
//    if (!singletonDict.isDictLoaded()) {
//        singletonDict.load(repository + "/dict");
//    }

    //Load the table in the database
    string tablefile = repository + "/" + tablename + ".csv";
    if (Utils::exists(tablefile)) {
	ifstream ifs;
        ifs.open(tablefile);
        string line;
        std::vector<std::vector<Term_t>> vectors;
	LOG(DEBUGL) << "Reading " << tablefile;
        while(std::getline(ifs, line)) {
            //Parse the row
	    int i = 0;
	    while (line.length() > 0) {
                auto delim = line.find(',');
                string sn = line.substr(0, delim);
		if (arity == 0 && vectors.size() <= i) {
		    std::vector<Term_t> v;
		    vectors.push_back(v);
		}
                vectors[i].push_back(singletonDict.getOrAdd(sn));
		if (delim == std::string::npos) {
		    line = "";
		} else {
		    line = line.substr(delim + 1);
		}
		i++;
            }
	    if (arity == 0) {
		arity = i;
	    }
        }
        std::vector<std::shared_ptr<Column>> columns;
        for(uint8_t i = 0; i < arity; ++i) {
            columns.push_back(std::shared_ptr<Column>(new InmemoryColumn(
                            vectors[i])));
        }
        segment = std::shared_ptr<Segment>(new Segment(arity, columns));
        ifs.close();
    } else {
        segment = NULL;
    }
    // dump();
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
    if (q.getNUniqueVars() == q.getTupleSize()) {
	std::shared_ptr<Column> col = segment->getColumn(posColumn);
	return col->sort_and_unique()->size();
    }
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

std::vector<uint8_t> __mergeSortingFields(std::vector<uint8_t> v1,
        std::vector<uint8_t> v2) {
    if (!v1.empty()) {
        for(auto f : v2)
            v1.push_back(f);
        return v1;
    } else {
        return v2;
    }
}

uint64_t __getKeyFromFields(const std::vector<uint8_t> &fields) {
    assert(fields.size() <= 8);
    uint64_t key = 0;
    for(uint8_t i = 0; i < fields.size(); ++i) {
        uint8_t field = fields[i];
        key += ((uint64_t)(field+1)) << 8;
    }
    return key;
}

std::shared_ptr<const Segment> InmemoryTable::getSortedCachedSegment(
        std::shared_ptr<const Segment> segment,
        const std::vector<uint8_t> &filterBy) {
    std::shared_ptr<const Segment> sortedSegment;
    if (filterBy.size() >=8) {
        sortedSegment = segment->sortBy(&filterBy);
    } else {
        //See if I have it in the cache
        uint64_t filterByKey = __getKeyFromFields(filterBy);
        if (cachedSortedSegments.count(filterByKey)) {
            sortedSegment = cachedSortedSegments[filterByKey];
        } else {
            sortedSegment = segment->sortBy(&filterBy);
            //Rewrite columns not backed by vectors
            std::vector<std::shared_ptr<Column>> columns;
            for(uint8_t i = 0; i < arity; ++i) {
                auto column = sortedSegment->getColumn(i);
                if (!column->isBackedByVector()) {
                    auto reader = column->getReader();
                    auto vector = reader->asVector();
                    column = std::shared_ptr<Column>(new InmemoryColumn(
                                vector));
                }
                columns.push_back(column);
            }
            sortedSegment = std::shared_ptr<Segment>(new Segment(arity,
                        columns));
            cachedSortedSegments[filterByKey] = sortedSegment;
        }
    }
    return sortedSegment;
}

EDBIterator *InmemoryTable::getSortedIterator(const Literal &query,
        const std::vector<uint8_t> &fields) {
    /*** Look at the query to see if we need filtering***/
    std::vector<uint8_t> posConstants;
    std::vector<uint8_t> vars;
    bool repeatedVars = false;
    for(uint8_t i = 0; i < query.getTupleSize(); ++i) {
        if (!query.getTermAtPos(i).isVariable()) {
            posConstants.push_back(i);
        } else {
            bool found = false;
            for(auto v : vars) {
                if (v == query.getTermAtPos(i).getId()) {
                    found = true;
                    repeatedVars = true;
                }
            }
            if (!found)
                vars.push_back(query.getTermAtPos(i).getId());
        }
    }

    /*** If there are no constants, then just returned a sorted version of the
     * table ***/
    if (posConstants.empty() && !repeatedVars) {
        std::shared_ptr<const Segment> sortedSegment = getSortedCachedSegment(
                segment, fields);
        return new InmemoryIterator(sortedSegment, predid);
    } else {
        //Filter the table
        if (posConstants.size() == 1 &&
                !repeatedVars &&
                ((posConstants.size() + fields.size()) <= 8)) {
            std::vector<uint8_t> filterBy = __mergeSortingFields(posConstants,
                    fields);
            uint64_t keySortFields = __getKeyFromFields(filterBy);
            if (!cacheHashes.count(keySortFields)) { //Fill the cache
                std::shared_ptr<const Segment> sortedSegment =
                    getSortedCachedSegment(segment, filterBy);
                cacheHashes.insert(std::make_pair(keySortFields,
                            HashMapEntry(sortedSegment)));
                auto &map = cacheHashes.find(keySortFields)->second.map;
                //Add offset and length for each key
                auto column = sortedSegment->getColumn(posConstants[0]);
                auto reader = column->getReader();
                Term_t prevkey = ~0lu;
                uint64_t start = 0;
                uint64_t currentidx = 0;
                while (reader->hasNext()) {
                    Term_t t = reader->next();
                    if (t != prevkey) {
                        if (prevkey != ~0lu) {
                            map.insert(make_pair(prevkey,
                                        Coordinates(start, currentidx - start)));
                        }
                        start = currentidx;
                        prevkey = t;
                    }
                    currentidx++;
                }
                if (currentidx != 0) {
                    map.insert(std::make_pair(prevkey, Coordinates(start,
                                    currentidx - start)));
                }
            }
            //Now I have a hashmap... use it to return a projection of the segment
            auto entry = cacheHashes.find(keySortFields)->second;
            Term_t constantValue = query.getTermAtPos(posConstants[0]).getValue();
            if (entry.map.count(constantValue)) {
                //Get the start and offset
                Coordinates &coord = entry.map.find(constantValue)->second;
                //Create a segment with some subcolumns
                std::vector<std::shared_ptr<Column>> subcolumns;
                for(uint8_t i = 0; i < arity; ++i) {
                    auto column = entry.segment->getColumn(i);
                    if (column->isBackedByVector()) {
                        subcolumns.push_back(std::shared_ptr<Column>(new SubColumn(
                                        column, coord.offset, coord.len)));
                    } else {
                        std::vector<Term_t> values;
                        for(uint64_t i = coord.offset; i < coord.offset +
                                coord.len; ++i) {
                            values.push_back(column->getValue(i));
                        }
                        subcolumns.push_back(std::shared_ptr<Column>(new
                                    InmemoryColumn(values)));
                    }
                }
                std::shared_ptr<const Segment> subsegment = std::shared_ptr<
                    const Segment>(new Segment(arity, subcolumns));
                return new InmemoryIterator(subsegment, predid);
            } else {
                //Return an empty segment (i.e., where hasNext() returns false)
                return new InmemoryIterator(NULL, predid);
            }

        } else { //More sophisticated sorting procedure ...
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
            auto filteredSegment = ((InmemoryFCInternalTable*)fTable.get())->
                getUnderlyingSegment();
            return new InmemoryIterator(filteredSegment, predid);
        }
    }
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

uint64_t InmemoryTable::getSize() {
    return segment->getNRows();
}

InmemoryTable::~InmemoryTable() {
}

bool InmemoryIterator::hasNext() {
    if (hasNextChecked) {
	return hasNextValue;
    }
    if (isFirst || ! skipDuplicatedFirst) {
	hasNextValue = iterator && iterator->hasNext();
    } else {
	Term_t oldval = getElementAt(1);
	bool stop = false;
	while (! stop && iterator->hasNext()) {
	    iterator->next();
	    // This may be a problem, because now hasNext has the side effect of already shifting the
	    // iterator ...
	    if (getElementAt(1) != oldval) {
		stop = true;
	    }
	}
	hasNextValue = stop;
    }
    hasNextChecked = true;
    return hasNextValue;
}

void InmemoryIterator::next() {
    if (! hasNextChecked) {
	LOG(ERRORL) << "InmemoryIterator::next called without hasNext check";
	throw 10;
    }
    if (! hasNextValue) {
	LOG(ERRORL) << "InmemoryIterator::next called while hasNext returned false";
	throw 10;
    }
    if (isFirst || ! skipDuplicatedFirst) {
	// otherwise we already did next() on the iterator. See hasNext().
	iterator->next();
    }
    isFirst = false;
    hasNextChecked = false;
}

Term_t InmemoryIterator::getElementAt(const uint8_t p) {
    return iterator->get(p);
}

PredId_t InmemoryIterator::getPredicateID() {
    return predid;
}

void InmemoryIterator::skipDuplicatedFirstColumn() {
    skipDuplicatedFirst = true;
}

void InmemoryIterator::clear() {
}
