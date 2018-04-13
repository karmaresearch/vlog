#include <vlog/inmemory/inmemorytable.h>
#include <vlog/fcinttable.h>
#include <vlog/support.h>

#include <kognac/utils.h>

void dump() {
}

std::vector<std::string> readRow(istream &ifs) {
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
        if (eof || (! insideEscaped && (c == '\n' || c == ','))) {
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

InmemoryTable::InmemoryTable(string repository, string tablename,
        PredId_t predid, EDBLayer *layer) {
    this->layer = layer;
    arity = 0;
    this->predid = predid;
    //Load the table in the database
    string tablefile = repository + "/" + tablename + ".csv";
    ifstream ifs;
    ifs.open(tablefile);
    if (ifs.fail()) {
	LOG(ERRORL) << "Could not open " << tablefile;
        throw ("Could not open file " + tablefile + " for reading");
    }
    LOG(DEBUGL) << "Reading " << tablefile;
    SegmentInserter *inserter = NULL;
    while (! ifs.eof()) {
        std::vector<std::string> row = readRow(ifs);
	Term_t rowc[128];
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
	inserter->addRow(rowc);
    }
    if (inserter == NULL) {
	segment = NULL;
    } else {
	segment = inserter->getSortedAndUniqueSegment();
	delete inserter;
    }
    ifs.close();
    // dump();
}

InmemoryTable::InmemoryTable(PredId_t predid, std::vector<std::vector<std::string>> &entries,
	EDBLayer *layer) {
    arity = 0;
    this->predid = predid;
    this->layer = layer;
    //Load the table in the database
    SegmentInserter *inserter = NULL;
    for (auto &row : entries) {
	Term_t rowc[128];
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

void InmemoryTable::query(QSQQuery *query, TupleTable *outputTable,
        std::vector<uint8_t> *posToFilter,
        std::vector<Term_t> *valuesToFilter) {
    Term_t row[128];
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

    LOG(ERRORL) << "Not implemented yet";
    throw 10;
}

bool InmemoryTable::isEmpty(const Literal &q, std::vector<uint8_t> *posToFilter,
        std::vector<Term_t> *valuesToFilter) {
    if (posToFilter == NULL) {
        return segment == NULL || getCardinality(q) == 0;
    } else {
        LOG(ERRORL) << "Not implemented yet";
        throw 10;
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

size_t InmemoryTable::getCardinality(const Literal &q) {
    if (q.getTupleSize() != arity) {
        return 0;
    }
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
	std::vector<uint8_t> posVarsToCopy;
	std::vector<uint8_t> posConstantsToFilter;
	std::vector<Term_t> valuesConstantsToFilter;
	std::vector<std::pair<uint8_t, uint8_t>> repeatedVars;
	std::unique_ptr<SegmentIterator> segIter = segment->iterator();

	_literal2filter(q, posVarsToCopy, posConstantsToFilter,
		valuesConstantsToFilter, repeatedVars);

	size_t count = 0;
	while (segIter->hasNext()) {
	    segIter->next();
	    bool match = true;
	    // First filter out non-matching constants
	    for (uint8_t i = 0; i < posConstantsToFilter.size(); i++) {
		if (segIter->get(posConstantsToFilter[i]) != valuesConstantsToFilter[i]) {
		    match = false;
		    break;
		}
	    }

	    if (match && repeatedVars.size() > 0) {
		for (int i = 0; i < repeatedVars.size(); i++) {
		    if (segIter->get(repeatedVars[i].first) != segIter->get(posVarsToCopy[repeatedVars[i].second])) {
			match = false;
			break;
		    }
		}
	    }
	    if (match) {
		count++;
	    }
	}
	LOG(DEBUGL) << "Cardinality of " << q.tostring(NULL, layer) << " is " << count;
	return count;
    }
}

size_t InmemoryTable::getCardinalityColumn(const Literal &q, uint8_t posColumn) {
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
    if (q.getTupleSize() != arity) {
        return new InmemoryIterator(NULL, predid, sortFields);
    }
    if (q.getNUniqueVars() == q.getTupleSize()) {
        return new InmemoryIterator(segment, predid, sortFields);
    }

    std::vector<uint8_t> posVarsToCopy;
    std::vector<uint8_t> posConstantsToFilter;
    std::vector<Term_t> valuesConstantsToFilter;
    std::vector<std::pair<uint8_t, uint8_t>> repeatedVars;
    std::unique_ptr<SegmentIterator> segIter = segment->iterator();
    uint8_t nfields = segment->getNColumns();
    std::vector<ColumnWriter *> writers;

    _literal2filter(q, posVarsToCopy, posConstantsToFilter,
            valuesConstantsToFilter, repeatedVars);

    writers.resize(nfields);
    for (uint8_t i = 0; i < nfields; ++i) {
        writers[i] = new ColumnWriter();
    }

    while (segIter->hasNext()) {
        segIter->next();
        bool match = true;
        // First filter out non-matching constants
        for (uint8_t i = 0; i < posConstantsToFilter.size(); i++) {
            if (segIter->get(posConstantsToFilter[i]) != valuesConstantsToFilter[i]) {
                match = false;
                break;
            }
        }

        if (! match) {
            continue;
        }

        if (repeatedVars.size() > 0) {
            for (int i = 0; i < repeatedVars.size(); i++) {
                if (segIter->get(repeatedVars[i].first) != segIter->get(posVarsToCopy[repeatedVars[i].second])) {
                    match = false;
                    break;
                }
            }
            if (! match) {
                continue;
            }
        }

        for (uint8_t i = 0; i < nfields; ++i) {
            writers[i]->add(segIter->get(i));
        }
    }

    std::vector<std::shared_ptr<Column>> columns;
    for (uint8_t i = 0; i < nfields; ++i) {
        columns.push_back(writers[i]->getColumn());
    }

    std::shared_ptr<Segment> filteredSegment = std::shared_ptr<Segment>(new Segment(nfields, columns));

    for (uint8_t i = 0; i < nfields; ++i) {
        delete writers[i];
    }

    return new InmemoryIterator(filteredSegment, predid, sortFields);
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

static uint64_t __getKeyFromFields(const std::vector<uint8_t> &fields) {
    assert(fields.size() <= 8);
    uint64_t key = 0;
    for(uint8_t i = 0; i < fields.size(); ++i) {
        uint8_t field = fields[i];
        key = (key << 8) + (uint64_t)(field+1);
    }
    return key;
}

std::shared_ptr<const Segment> InmemoryTable::getSortedCachedSegment(
        std::shared_ptr<const Segment> segment,
        const std::vector<uint8_t> &sortBy) {
    std::shared_ptr<const Segment> sortedSegment;
    if (sortBy.size() >=8) {
        sortedSegment = segment->sortBy(&sortBy);
    } else {
        //See if I have it in the cache
        uint64_t filterByKey = __getKeyFromFields(sortBy);
        if (cachedSortedSegments.count(filterByKey)) {
            sortedSegment = cachedSortedSegments[filterByKey];
        } else {
            sortedSegment = segment->sortBy(&sortBy);
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
    return getSortedIterator2(query, newFields);
}

EDBIterator *InmemoryTable::getSortedIterator2(const Literal &query,
        const std::vector<uint8_t> &fields) {
    if (query.getTupleSize() != arity) {
        return new InmemoryIterator(NULL, predid, fields);
    }

    LOG(DEBUGL) << "InmemoryTable::getSortedIterator, query = " << query.tostring(NULL, layer) << ", fields.size() = " << fields.size();

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
        return new InmemoryIterator(sortedSegment, predid, fields);
    } else if ((posConstants.size() == arity || posConstants.size() == 1) &&
	    !repeatedVars &&
	    ((posConstants.size() + fields.size()) <= 8)) {
	std::vector<uint8_t> filterBy = __mergeSortingFields(posConstants,
		fields);
#if 0
	std::string s = "";
	for (auto f : filterBy) {
	    s += to_string((int) f) + " ";
	}
	LOG(DEBUGL) << "Sorting fields: " << s;
#endif
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
	    if (currentidx != start) {
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
		    for(uint64_t j = coord.offset; j < coord.offset +
			    coord.len; ++j) {
			values.push_back(column->getValue(j));
		    }
		    subcolumns.push_back(std::shared_ptr<Column>(new
				InmemoryColumn(values)));
		}
	    }
	    std::shared_ptr<const Segment> subsegment = std::shared_ptr<
		const Segment>(new Segment(arity, subcolumns));
	    if (posConstants.size() == arity) {
		InmemoryFCInternalTable t(arity, 0, false, subsegment);
		std::vector<uint8_t> posVarsToCopy;
		std::vector<Term_t> valuesConstantsToFilter;
		std::vector<std::pair<uint8_t, uint8_t>> repeatedVars;
		for (int i = 0; i < arity; i++) {
		    valuesConstantsToFilter.push_back(query.getTermAtPos(i).getValue());
		}
		auto fTable = t.filter(posVarsToCopy.size(), posVarsToCopy.data(),
		    posConstants.size(), posConstants.data(),
		    valuesConstantsToFilter.data(), repeatedVars.size(),
		    repeatedVars.data(), 1); //no multithread
		if (fTable == NULL || fTable->isEmpty()) {
		    return new InmemoryIterator(NULL, predid, fields);
		}
		LOG(DEBUGL) << query.tostring(NULL, layer) << " present!";
		for (int j = 0; j < arity; j++) {
		    subcolumns[j] = std::shared_ptr<Column>(new CompressedColumn(valuesConstantsToFilter[j], 1));
		}
		subsegment = std::shared_ptr<const Segment>(new Segment(arity, subcolumns));
		return new InmemoryIterator(subsegment, predid, fields);
	    } else {
		return new InmemoryIterator(subsegment, predid, fields);
	    }
	} else {
	    //Return an empty segment (i.e., where hasNext() returns false)
	    return new InmemoryIterator(NULL, predid, fields);
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
	std::shared_ptr<const Segment> subsegment = std::shared_ptr<
	    const Segment>(new Segment(arity, subcolumns));
	subsegment = subsegment->sortBy(&fields);
	return new InmemoryIterator(subsegment, predid, fields);
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
