#include <vlog/inmemory/inmemorytable.h>
#include <vlog/fcinttable.h>
#include <vlog/support.h>

#include <kognac/utils.h>
#include <kognac/filereader.h>

#include <zstr/zstr.hpp>

void dump() {
}

std::vector<std::string> readRow(istream &ifs, char separator) {
    char buffer[65536];
    bool insideEscaped = false;
    char *p = &buffer[0];
    bool justSeenQuote = false;
    int quoteCount = 0;     // keep track of number of concecutive quotes.
    std::vector<std::string> result;
    while (true) {
        int c = ifs.get();
        bool eof = (c == EOF);
        if (eof) {
            if (p == buffer && result.size() == 0) {
                return result;
            }
            c = '\n';
        }
        if (c == '\r') {
            // ignore these?
            continue;
        }
        if (p == buffer && ! justSeenQuote) {
            // Watch out for more than one initial quote ...
            if (c == '"') {
                // Initial character is a quote.
                insideEscaped = true;
                justSeenQuote = true;
                continue;
            }
        } else if (c == '"') {
            quoteCount++;
            insideEscaped = (quoteCount & 1) == 0;
            if (insideEscaped) {
                p--;
            }
        } else {
            quoteCount = 0;
        }
        if (eof || (! insideEscaped && (c == '\n' || c == separator))) {
            if (justSeenQuote) {
                *(p-1) = '\0';
            } else {
                *p = '\0';
            }
            result.push_back(std::string(buffer));
            if (c == '\n') {
                return result;
            }
            p = buffer;
            insideEscaped = false;
        } else {
            if (p - buffer >= 65535) {
                LOG(ERRORL) << "Max field size";
                throw "Maximum field size exceeded in CSV file: 65535";
            }
            *p++ = c;
        }
        justSeenQuote = (c == '"');
    }
}

std::string convertString(const char *s, int len) {
    if (s == NULL || len == 0) {
        return "";
    }

    std::string ss = std::string(s, s+len);

    /*
    if (len > 1 && s[0] == '"' && s[len-1] == '"') {
        return (ss + "^^<http://www.w3.org/2001/XMLSchema#string>").c_str();
    }
    */
    return ss;
}

InmemoryTable::InmemoryTable(std::string repository, std::string tablename,
        PredId_t predid, EDBLayer *layer, char sep, bool loadData) {
    this->layer = layer;
    arity = 0;
    this->predid = predid;
    SegmentInserter *inserter = NULL;
    //Load the table in the database
    if (repository == "") {
        repository = ".";
    }
    std::string tablefile = repository + "/" + tablename + ".csv";
    std::string gz = tablefile + ".gz";
    istream *ifs = NULL;
    if (Utils::exists(gz)) {
        ifs = new zstr::ifstream(gz);
        if (ifs->fail()) {
            std::string e = "While importing data for predicate \"" + layer->getPredName(predid) + "\": could not open file " + gz;
            LOG(ERRORL) << e;
            throw (e);
        }
    } else if (Utils::exists(tablefile)) {
        ifs = new std::ifstream(tablefile, ios_base::in | ios_base::binary);
        if (ifs->fail()) {
            std::string e = "While importing data for predicate \"" + layer->getPredName(predid) + "\": could not open file " + tablefile;
            segment = NULL;
            LOG(ERRORL) << e;
            throw (e);
        }
    }
    if (ifs != NULL) {
        LOG(DEBUGL) << "Reading " << tablefile;
        while (! ifs->eof()) {
            std::vector<std::string> row = readRow(*ifs, sep);
            Term_t rowc[256];
            if (arity == 0) {
                arity = row.size();
            }
            if (row.size() == 0) {
                break;
            } else if (row.size() != arity) {
                LOG(ERRORL) << "Multiple arities";
                throw ("Multiple arities in file " + tablefile);
            }
            if (inserter == NULL) {
                inserter = new SegmentInserter(arity);
            }
            for (int i = 0; i < arity; i++) {
                uint64_t val;
                layer->getOrAddDictNumber(row[i].c_str(), row[i].size(), val);
                rowc[i] = val;
            }
            if (loadData)
                inserter->addRow(rowc);
            else
                break;
        }
        delete ifs;
    } else {
        tablefile = repository + "/" + tablename + ".nt";
        std::string gz = tablefile + ".gz";
        FileInfo f;
        f.start = 0;
        if (Utils::exists(gz)) {
            f.size = Utils::fileSize(gz);
            f.path = gz;
            f.splittable = false;
        } else if (Utils::exists(tablefile)) {
            f.size = Utils::fileSize(tablefile);
            f.path = tablefile;
            f.splittable = true;
        } else {
            std::string e = "While importing data for predicate \"" + layer->getPredName(predid) + "\": could not open file " + tablefile + " nor " + (repository + "/" + tablename + ".csv") + " nor gzipped versions";
            LOG(ERRORL) << e;
            segment = NULL;
            throw(e);
        }
        FileReader reader(f);
        while (reader.parseTriple()) {
            if (reader.isTripleValid()) {
                Term_t rowc[3];
                int ls, lp, lo;
                const char *s = reader.getCurrentS(ls);
                std::string ss = convertString(s, ls);
                const char *p = reader.getCurrentP(lp);
                std::string sp = convertString(p, lp);
                const char *o = reader.getCurrentO(lo);
                std::string so = convertString(o, lo);

                if (!loadData)
                    break;

                if (inserter == NULL) {
                    inserter = new SegmentInserter(3);
                }
                uint64_t val;
                layer->getOrAddDictNumber(ss.c_str(), ss.size(), val);
                rowc[0] = val;
                layer->getOrAddDictNumber(sp.c_str(), sp.size(), val);
                rowc[1] = val;
                layer->getOrAddDictNumber(so.c_str(), so.size(), val);
                rowc[2] = val;
                inserter->addRow(rowc);
            }
        }
        arity = 3;
    }
    if (inserter == NULL) {
        segment = NULL;
    } else {
        segment = inserter->getSortedAndUniqueSegment();
        delete inserter;
    }
}

InmemoryTable::InmemoryTable(PredId_t predid,
        std::vector<std::vector<std::string>> &entries,
        EDBLayer *layer) {
    arity = 0;
    this->predid = predid;
    this->layer = layer;
    //Load the table in the database
    SegmentInserter *inserter = NULL;
    for (auto &row : entries) {
        Term_t rowc[256];
        if (arity == 0) {
            arity = row.size();
        }
        if (row.size() == 0) {
            break;
        } else if (row.size() != arity) {
            throw ("Multiple arities in input");
        }
        if (inserter == NULL) {
            inserter = new SegmentInserter(arity);
        }
        for (int i = 0; i < arity; i++) {
            uint64_t val;
            layer->getOrAddDictNumber(row[i].c_str(), row[i].size(), val);
            rowc[i] = val;
        }
        inserter->addRow(rowc);
    }
    if (arity == 0) {
        segment = NULL;
    } else {
        segment = inserter->getSortedAndUniqueSegment();
        delete inserter;
    }
}

InmemoryTable::InmemoryTable(PredId_t predid,
        const Literal &query,
        // const
        EDBIterator *iter,
        EDBLayer *layer) {
    // Collect matching data. Will be stored in an InmemoryTable.
    std::vector<Term_t> term(query.getTupleSize());
    // need to store all variables, then afterwards sort by fields
    arity = query.getTupleSize();
    this->predid = predid;
    this->layer = layer;
    //Load the table in the database
    SegmentInserter *inserter = NULL;
    int count = 0;
    while (iter->hasNext()) {
        iter->next();
        for (size_t i = 0; i < term.size(); ++i) {
            term[i] = iter->getElementAt(i);
        }
        if (inserter == NULL) {
            inserter = new SegmentInserter(arity);
        }
        inserter->addRow(term.data());
        count++;
    }
    LOG(DEBUGL) << "InmemoryTable constructor: " << count;

    if (inserter == NULL) {
        segment = NULL;
    } else {
        segment = inserter->getSortedAndUniqueSegment();
        delete inserter;
    }
}

InmemoryTable::InmemoryTable(PredId_t predid,
        uint8_t arity,
        std::vector<uint64_t> &entries,
        EDBLayer *layer) {
    this->arity = arity;
    this->predid = predid;
    this->layer = layer;
    SegmentInserter *inserter =  new SegmentInserter(arity);
    for(uint64_t i = 0; i < entries.size(); i += arity) {
        Term_t rowc[256];
        for(uint8_t j = 0; j < arity; ++j) {
            rowc[j] = entries[i + j];
        }
        inserter->addRow(rowc);
    }
    if (arity == 0) {
        segment = NULL;
    } else {
        segment = inserter->getSortedAndUniqueSegment();
    }
    delete inserter;
}

struct VSorter {
    unsigned sz;

    VSorter(unsigned sz) : sz(sz) {
    }

    bool operator ()(const Term_t *t1, const Term_t *t2) const {
        if (t1 == t2) {
            return false;
        }
        for (unsigned i = 0; i < sz; i++) {
            if (t1[i] != t2[i]) {
                return t2[i] > t1[i];
            }
        }
        return false;
    }
};

void InmemoryTable::query(QSQQuery *query, TupleTable *outputTable,
        std::vector<uint8_t> *posToFilter,
        std::vector<Term_t> *valuesToFilter) {
    Term_t row[256];
    const Literal *lit = query->getLiteral();
    uint8_t *pos = query->getPosToCopy();
    const uint8_t npos = query->getNPosToCopy();
    if (posToFilter == NULL || posToFilter->size() == 0 || valuesToFilter == NULL || valuesToFilter->size() == 0) {
        EDBIterator *iter = getIterator(*lit);
        while (iter->hasNext()) {
            iter->next();
            for (int i = 0; i < npos; ++i) {
                row[i] = iter->getElementAt(pos[i]);
            }
            outputTable->addRow(row);
        }
    } else {
        EDBIterator *iter = getSortedIterator2(*lit, *posToFilter);
        std::vector<Term_t *> values(valuesToFilter->size() / posToFilter->size());
        for (size_t i = 0; i < values.size(); i++) {
            values[i] = &((*valuesToFilter)[i * posToFilter->size()]);
        }
        VSorter sorter(posToFilter->size());
        std::sort(values.begin(), values.end(), std::ref(sorter));

        size_t valIndex = 0;
        while (iter->hasNext()) {
            iter->next();
            bool match = false;
            while (valIndex < values.size()) {
                match = true;
                bool rematch = false;
                for (int i = 0; match && i < posToFilter->size(); i++) {
                    Term_t el = iter->getElementAt(posToFilter->at(i));
                    Term_t t = values[valIndex][i];
                    if (t < el) {
                        match = false;
                        valIndex++;
                        rematch = true;
                    } else if (t > el) {
                        match = false;
                    }
                }
                if (match || ! rematch) {
                    break;
                }
            }

            if (! match) {
                continue;
            }

            for (int i = 0; i < npos; ++i) {
                row[i] = iter->getElementAt(pos[i]);
            }
            outputTable->addRow(row);
        }
    }
}

bool InmemoryTable::isEmpty(const Literal &q, std::vector<uint8_t> *posToFilter,
        std::vector<Term_t> *valuesToFilter) {
    if (segment == NULL) {
        return true;
    }
    if (posToFilter == NULL || posToFilter->size() == 0) {
        return getCardinality(q) == 0;
    } else {
        VTuple v = q.getTuple();
        for (int i = 0; i < posToFilter->size(); ++i) {
            v.set(VTerm(0, valuesToFilter->at(i)), posToFilter->at(i));
        }
        Literal q1(q.getPredicate(), v);
        return getCardinality(q1) == 0;
    }
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
                auto var = query.getTermAtPos(posVarsToCopy[j]);
                if (var.getId() == term.getId()) {
                    repeatedVars.push_back(std::make_pair(i, posVarsToCopy[j]));
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

size_t InmemoryTable::getCardinality(const Literal &q) {
    size_t res;

    HiResTimer t_card("InmemoryTable::getCardinality(" + q.tostring() + ")");
    t_card.start();
    if (q.getTupleSize() != arity || segment == NULL) {
        res = 0;
    } else if (q.getNUniqueVars() == q.getTupleSize()) {
        if (arity == 0) {
            res = 1;
        } else {
            res = segment->getNRows();
        }
    } else {
        EDBIterator *iter = getIterator(q);
        size_t count = 0;
        while (iter->hasNext()) {
            iter->next();
            count++;
        }
        iter->clear();
        delete iter;
        LOG(DEBUGL) << "Cardinality of " << q.tostring(NULL, layer) << " is " << count;
        res = count;
    }
    t_card.stop();
    LOG(DEBUGL) << t_card.tostring();

    return res;
}

size_t InmemoryTable::getCardinalityColumn(const Literal &q, uint8_t posColumn) {
    if (segment == NULL) {
        return 0;
    }
    if (q.getNUniqueVars() == q.getTupleSize()) {
        std::shared_ptr<Column> col = segment->getColumn(posColumn);
        return col->sort_and_unique()->size();
    }
    int64_t oldval = -1;
    std::vector<uint8_t> fields;
    fields.push_back(posColumn);
    // probably not efficient... TODO
    EDBIterator *iter = getSortedIterator2(q, fields);
    size_t cnt = 0;
    while (iter->hasNext()) {
        iter->next();
        if (iter->getElementAt(posColumn) != oldval) {
            cnt++;
            oldval = iter->getElementAt(posColumn);
        }
    }
    iter->clear();
    delete iter;
    return cnt;
}

EDBIterator *InmemoryTable::getIterator(const Literal &q) {
    std::vector<uint8_t> sortFields;
    if (q.getTupleSize() != arity || segment == NULL) {
        return new InmemoryIterator(NULL, predid, sortFields);
    }
    if (q.getNUniqueVars() == q.getTupleSize()) {
        return new InmemoryIterator(segment, predid, sortFields);
    }
    for (int i = 0; i < q.getTupleSize(); i++) {
        if (q.getTermAtPos(i).isVariable()) {
            sortFields.push_back(i);
            break;
        }
    }
    return getSortedIterator2(q, sortFields);
}

static std::vector<uint8_t> __mergeSortingFields(std::vector<uint8_t> v1,
        std::vector<uint8_t> v2) {
    int sz = v1.size();
    if (sz != 0) {
        for(auto f : v2) {
            bool found = false;
            for (int i = 0; i < sz; i++) {
                if (v1[i] == f) {
                    found = true;
                    break;
                }
            }
            if (! found) {
                v1.push_back(f);
            }
        }
        return v1;
    } else {
        return v2;
    }
}

static uint64_t __getKeyFromFields(const std::vector<uint8_t> &fields, uint8_t sz) {
    assert(sz <= 8);
    uint64_t key = 0;
    for(uint8_t i = 0; i < sz; ++i) {
        uint8_t field = fields[i];
        key = (key << 8) + (uint64_t)(field+1);
    }
    return key;
}

std::shared_ptr<const Segment> InmemoryTable::getSortedCachedSegment(
        std::shared_ptr<const Segment> segment,
        const std::vector<uint8_t> &sortBy) {
    // The segment that we have is sorted in field order, so if that is what is requested,
    // return that.
    bool haveSorted = true;
    for (int i = 0; i < sortBy.size(); i++) {
        if (sortBy[i] != i) {
            haveSorted = false;
            break;
        }
    }
    if (haveSorted) {
        return segment;
    }
#if DEBUG
    std::string s = "";
    for (int i = 0; i < sortBy.size(); i++) {
        s += to_string(sortBy[i]) + " ";
    }
    LOG(DEBUGL) << "Sorting fields: " << s;
#endif
    std::shared_ptr<const Segment> sortedSegment;
    if (sortBy.size() >=8) {
        sortedSegment = segment->sortBy(&sortBy);
    } else {
        //See if I have it in the cache
        //if we already have one in the cache that is say, sorted on fields 1, 2, 3
        //and we now require sorted on fields 1, 2, then the one sorted on fields 1, 2, 3
        //meets the requirement.
        uint64_t filterByKey = __getKeyFromFields(sortBy, sortBy.size());
        if (cachedSortedSegments.count(filterByKey)) {
            LOG(DEBUGL) << "Found sorted segment in cache";
            sortedSegment = cachedSortedSegments[filterByKey];
        } else {
            LOG(DEBUGL) << "Did not find sorted segment in cache";
            std::vector<uint8_t> sb(sortBy);
            if (sortBy.size() < arity) {
                for (int i = 0; i < arity; i++) {
                    bool present = false;
                    for (int j = 0; j < sortBy.size(); j++) {
                        if (i == sortBy[j]) {
                            present = true;
                            break;
                        }
                    }
                    if (! present) {
                        sb.push_back(i);
                    }
                }
            }

            sortedSegment = segment->sortBy(&sb);
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
            //If we are adding one in the cache that is say, sorted on fields 1, 2, 3,
            //this one is also sorted on fields 1, 2, and also sorted on field 1.
            //So, we add those to the hashtable as well.
            for (int i = 0; i < sb.size(); i++) {
                filterByKey = __getKeyFromFields(sb, i+1);
                cachedSortedSegments[filterByKey] = sortedSegment;
            }
        }
    }
    return sortedSegment;
}


EDBIterator *InmemoryTable::getSortedIterator(const Literal &query,
        const std::vector<uint8_t> &fields) {
    LOG(DEBUGL) << "InmemoryTable::getSortedIterator (1) query " << query.tostring(NULL, layer) << " fields " << fields2str(fields);

    std::vector<uint8_t> offsets;
    int nConstantsSeen = 0;
    for (int i = 0; i < query.getTupleSize(); i++) {
        if (! query.getTermAtPos(i).isVariable()) {
            nConstantsSeen++;
        } else {
            offsets.push_back(nConstantsSeen);
        }
    }
    if (offsets.size() == 0) {
        // Apparently, getSortedIterator can be called with a query containing only constants.
        return getSortedIterator2(query, fields);
    }
    std::vector<uint8_t> newFields;
    for (auto f : fields) {
        assert(f < offsets.size());
        newFields.push_back(offsets[f] + f);
    }
    assert(newFields.size() == fields.size());
    return getSortedIterator2(query, newFields);
}

EDBIterator *InmemoryTable::getSortedIterator2(const Literal &query,
        const std::vector<uint8_t> &fields) {
    if (query.getTupleSize() != arity || segment == NULL) {
        return new InmemoryIterator(NULL, predid, fields);
    }

    LOG(DEBUGL) << "InmemoryTable::getSortedIterator, query = " << query.tostring(NULL, layer) << ", fields " << fields2str(fields);

    /*** Look at the query to see if we need filtering***/
    std::vector<uint8_t> posVarsToCopy;
    std::vector<uint8_t> posConstantsToFilter;
    std::vector<Term_t> valuesConstantsToFilter;
    std::vector<std::pair<uint8_t, uint8_t>> repeatedVars;
    _literal2filter(query, posVarsToCopy, posConstantsToFilter,
            valuesConstantsToFilter, repeatedVars);

    /*** If there are no constants, then just returned a sorted version of the
     * table ***/
    if (posConstantsToFilter.empty() && repeatedVars.empty()) {
        std::shared_ptr<const Segment> sortedSegment = getSortedCachedSegment(
                segment, fields);
        return new InmemoryIterator(sortedSegment, predid, fields);
    }

    // Now, first get a segment to filter. Several cases.
    std::shared_ptr<const Segment> segmentToFilter;
    if (posConstantsToFilter.size() == 0) {
        // No constants in the query, so we need the whole segment, sorted.
        segmentToFilter = getSortedCachedSegment(segment, fields);
    } else {
        // Constants in the query, so prepend their positions to the sorting fields.
        std::vector<uint8_t> filterBy;
        for (int i = 0; i < posConstantsToFilter.size(); i++) {
            filterBy.push_back(posConstantsToFilter[i]);
        }
        filterBy = __mergeSortingFields(filterBy, fields);
        // Now get the key of the entry we need.
        uint64_t keySortFields = __getKeyFromFields(filterBy, filterBy.size() >= 8 ? 7 : filterBy.size());
        // Fill the sort fields up with the other fields.
        if (filterBy.size() < arity) {
            for (int i = 0; i < arity; i++) {
                bool present = false;
                for (int j = 0; j < filterBy.size(); j++) {
                    if (i == filterBy[j]) {
                        present = true;
                        break;
                    }
                }
                if (! present) {
                    filterBy.push_back(i);
                }
            }
        }
        if (! cacheHashes.count(keySortFields)) {
            // Not available yet. Get the corresponding sorted segment.
            std::shared_ptr<const Segment> sortedSegment =
                getSortedCachedSegment(segment, filterBy);
            // Create a map from constant values to begin and end coordinates in this segment.
            std::shared_ptr<HashMapEntry> map = std::shared_ptr<HashMapEntry>(new HashMapEntry(sortedSegment));
            auto column = sortedSegment->getColumn(posConstantsToFilter[0]);
            auto reader = column->getReader();
            Term_t prevkey = ~0lu;
            uint64_t start = 0;
            uint64_t currentidx = 0;
            while (reader->hasNext()) {
                Term_t t = reader->next();
                if (t != prevkey) {
                    if (prevkey != ~0lu) {
                        map->map.insert(make_pair(prevkey,
                                    Coordinates(start, currentidx - start)));
                    }
                    start = currentidx;
                    prevkey = t;
                }
                currentidx++;
            }
            if (currentidx != start) {
                map->map.insert(std::make_pair(prevkey, Coordinates(start,
                                currentidx - start)));
            }
            // Now put this map in the cacheHashes map, for each size.
            for (int i = 1; i <= filterBy.size(); i++) {
                if (i >= 8) {
                    break;
                }
                keySortFields = __getKeyFromFields(filterBy, i);
                if (! cacheHashes.count(keySortFields)) {
                    cacheHashes.insert(std::make_pair(keySortFields, map));
                }
            }
        }
        // Now we hav the map available.
        auto entry = cacheHashes.find(keySortFields)->second;
        Term_t constantValue = valuesConstantsToFilter[0];
        if (entry->map.count(constantValue)) {
            //Get the start and offset
            Coordinates &coord = entry->map.find(constantValue)->second;
            //Create a segment with some subcolumns
            std::vector<std::shared_ptr<Column>> subcolumns;
            for(uint8_t i = 0; i < arity; ++i) {
                auto column = entry->segment->getColumn(i);
                if (column->isBackedByVector()) {
                    subcolumns.push_back(std::shared_ptr<Column>(new SubColumn(
                                    column, coord.offset, coord.len)));
                } else {
                    std::vector<Term_t> values;
                    for(uint64_t j = coord.offset; j < coord.offset +
                            coord.len; ++j) {
                        values.push_back(column->getValue(j));
                    }
                    subcolumns.push_back(std::shared_ptr<Column>(new
                                InmemoryColumn(values)));
                }
            }
            segmentToFilter = std::shared_ptr<const Segment>(new Segment(arity, subcolumns));
            if (filterBy.size() > 7) {
                // In this case, we don't know the exact ordering, so it needs to be sorted
                // further. Hopefully, it helps that it is already partly ordered.
                segmentToFilter = segmentToFilter->sortBy(&filterBy);
            }
        } else {
            //Return an empty segment (i.e., where hasNext() returns false)
            return new InmemoryIterator(NULL, predid, fields);
        }
    }

    if (posConstantsToFilter.size() == 1 && repeatedVars.empty()) {
        // No further filtering needed.
        return new InmemoryIterator(segmentToFilter, predid, fields);
    }

    // General filtering procedure.
    InmemoryFCInternalTable t(arity, 0, false, segmentToFilter);
    auto fTable = t.filter(posVarsToCopy.size(), posVarsToCopy.data(),
            posConstantsToFilter.size(), posConstantsToFilter.data(),
            valuesConstantsToFilter.data(), repeatedVars.size(),
            repeatedVars.data(), 1); //no multithread
    uint64_t sz = 0;
    std::vector<std::shared_ptr<Column>> subcolumns(arity);
    if (fTable == NULL || fTable->isEmpty()) {
        return new InmemoryIterator(NULL, predid, fields);
    } else if (posVarsToCopy.size() != 0) {
        // Note, the fTable now only has the variables in posVarsToCopy.
        auto filteredSegment = ((InmemoryFCInternalTable*)(fTable.get()))->
            getUnderlyingSegment();
        sz = filteredSegment->getNRows();
        for (int j = 0; j < posVarsToCopy.size(); j++) {
            subcolumns[posVarsToCopy[j]] = filteredSegment->getColumn(j);
            for (int i = 0; i < repeatedVars.size(); i++) {
                if (repeatedVars[i].second == j) {
                    subcolumns[repeatedVars[i].first] = subcolumns[posVarsToCopy[j]];
                }
            }
        }
    } else {
        sz = fTable->getNRows();
    }
    if (sz == 0) {
        return new InmemoryIterator(NULL, predid, fields);
    }
    for (int j = 0; j < posConstantsToFilter.size(); j++) {
        subcolumns[posConstantsToFilter[j]] = std::shared_ptr<Column>(new CompressedColumn(valuesConstantsToFilter[j], sz));
    }
    std::shared_ptr<const Segment> subsegment = std::shared_ptr<const Segment>(new Segment(arity, subcolumns));
    return new InmemoryIterator(subsegment, predid, fields);
}

void InmemoryTable::releaseIterator(EDBIterator *itr) {
    delete itr;
}

size_t InmemoryTable::estimateCardinality(const Literal &query) {
    return getCardinality(query);
}

bool InmemoryTable::getDictNumber(const char *text, const size_t sizeText,
        uint64_t &id) {
    return false;
}

bool InmemoryTable::getDictText(const uint64_t id, char *text) {
    return false;
}

bool InmemoryTable::getDictText(const uint64_t id, std::string &text) {
    return false;
}

uint64_t InmemoryTable::getNTerms() {
    return 0;
}

uint8_t InmemoryTable::getArity() const {
    return arity;
}

uint64_t InmemoryTable::getSize() {
    return segment == NULL ? 0 : segment->getNRows();
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
        int elNo = 0;
        if (sortFields.size() == 1) {
            elNo = sortFields[0];
        }
        if (sortFields.size() <= 1) {
            Term_t oldval = getElementAt(elNo);
            bool stop = false;
            while (! stop && iterator->hasNext()) {
                iterator->next();
                // This may be a problem, because now hasNext has the side effect of already shifting the
                // iterator ...
                if (getElementAt(elNo) != oldval) {
                    stop = true;
                }
            }
            hasNextValue = stop;
        } else {
            std::vector<Term_t> oldval;
            for (int i = 0; i < sortFields.size(); i++) {
                oldval.push_back(getElementAt(sortFields[i]));
            }
            bool stop = false;
            while (! stop && iterator->hasNext()) {
                iterator->next();
                // This may be a problem, because now hasNext has the side effect of already shifting the
                // iterator ...
                for (int i = 0; i < sortFields.size(); i++) {
                    if (oldval[i] != getElementAt(sortFields[i])) {
                        stop = true;
                        break;
                    }
                }
            }
            hasNextValue = stop;
        }
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
#if 0
        std::vector<Term_t> oldval;
        if (! isFirst) {
            for (int i = 0; i < sortFields.size(); i++) {
                oldval.push_back(getElementAt(sortFields[i]));
            }
        }
#endif
        iterator->next();
#if 0
        std::string s = "";
        for (int i = 0; i < segment->getNColumns(); i++) {
            s += to_string(getElementAt(i)) + " ";
        }
        LOG(DEBUGL) << "Iterator delivers: " + s;
        if (! isFirst) {
            for (int i = 0; i < sortFields.size(); i++) {
                if (oldval[i] < getElementAt(sortFields[i])) {
                    break;
                }
                if (oldval[i] > getElementAt(sortFields[i])) {
                    LOG(ERRORL) << "Not sorted!";
                    break;
                }
            }
        }
#endif
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
    LOG(DEBUGL) << "skipDuplicatedFirst, sortFields.size() = " << sortFields.size();
    skipDuplicatedFirst = true;
}

void InmemoryIterator::clear() {
}
