#include <vlog/filterer.h>
#include <vlog/fctable.h>
#include <vlog/joinprocessor.h>
#include <vlog/concepts.h>

#include <trident/model/table.h>

FCTable::FCTable(const uint8_t sizeRow) : sizeRow(sizeRow) {
}

std::string FCTable::getSignature(const Literal &literal) {
    std::string out = "";
    std::vector<uint8_t> existingVars;
    for (uint8_t i = 0; i < literal.getTupleSize(); ++i) {
        VTerm t = literal.getTermAtPos(i);
        if (t.isVariable()) {
            int8_t idx = (int8_t) (-1 * existingVars.size() - 1);
            for (uint8_t j = 0; j < existingVars.size(); ++j) {
                if (existingVars[j] == t.getId()) {
                    idx = -1 * j - 1;
                    break;
                }
            }
            out += " " + to_string(idx) + " ";
        } else {
            out += " " + to_string(t.getValue()) + " ";
        }
    }
    return out;
}

FCIterator FCTable::read(const size_t iteration) const {
    std::vector<FCBlock>::const_iterator itr = blocks.begin();
    while (itr != blocks.end() && itr->iteration < iteration) {
        itr++;
    }
    //long t = 0;
    if (itr != blocks.end()) {
        //t = totalRows - itr->nTuplesSoFar;
        return FCIterator(itr, blocks.end());
    } else {
        return FCIterator();
    }
}

FCIterator FCTable::read(const size_t mincount, const size_t maxcount) const {
    std::vector<FCBlock>::const_iterator itr = blocks.begin();
    while (itr != blocks.end() && itr->iteration < mincount) {
        itr++;
    }
    //long t = 0;
    if (itr != blocks.end()) {
        //t = totalRows - itr->nTuplesSoFar;
        //Determine the max count
        std::vector<FCBlock>::const_iterator endrange = itr + 1;
        while (endrange != blocks.end() && endrange->iteration < maxcount) {
            endrange++;
        }
        return FCIterator(itr, endrange);
    } else {
        return FCIterator();
    }
}

FCBlock &FCTable::getLastBlock() {
    return blocks.back();
}

size_t FCTable::estimateCardinality(const Literal &literal, const size_t min, const size_t max) const {
    FCIterator itr = read(min, max);
    uint8_t nconstants = 0;
    uint8_t posConstants[SIZETUPLE];
    Term_t valueConstants[SIZETUPLE];
    for (uint8_t i = 0; i < literal.getTupleSize(); ++i) {
        VTerm t = literal.getTermAtPos(i);
        if (!t.isVariable()) {
            posConstants[nconstants] = i;
            valueConstants[nconstants] = t.getValue();
            nconstants++;
        }
    }

    size_t estimation = 0;
    while (!itr.isEmpty()) {
        //if (nconstants == 0) {
        //    estimation += itr.getCurrentTable()->getNRows();
        //} else {
        estimation += itr.getCurrentTable()->estimateNRows(
                          nconstants, posConstants,
                          valueConstants);
        //}
        itr.moveNextCount();
    }
    return estimation;
}

std::shared_ptr<const FCTable> FCTable::filter(const Literal &literal, const size_t minIteration, TableFilterer *filterer) {
    bool shouldFilter = literal.getNUniqueVars() < literal.getTupleSize();

    if (shouldFilter) {
        std::shared_ptr<FCTable> output;
        std::vector<FCBlock>::iterator itr = blocks.begin();
        std::string signature = getSignature(literal);
        BOOST_LOG_TRIVIAL(trace) << "FCTable::filter: literal = " << literal.tostring() << ", signature = " << signature;
        FCCache::iterator cacheItr = cache.find(signature);
        if (cacheItr != cache.end()) {
            BOOST_LOG_TRIVIAL(trace) << "Found in cache ...";
            output = cacheItr->second.table;

            //First update the entry if there are more entries. Otherwise return
            if (cacheItr->second.end < blocks[blocks.size() - 1].iteration) {
                BOOST_LOG_TRIVIAL(trace) << "... but needs updating";
                while (itr != blocks.end() && itr->iteration <= cacheItr->second.end) {
                    itr++;
                }
            } else {
                BOOST_LOG_TRIVIAL(trace) << "returned";
                return output;
            }
        } else {
            BOOST_LOG_TRIVIAL(trace) << "not in cache";
            output = std::shared_ptr<FCTable>(new FCTable(literal.getNVars()));
        }

        //Scan all tables to check whether there are tuples we can add in the table
        uint8_t nConstantsToFilter = 0;
        uint8_t posConstantsToFilter[SIZETUPLE];
        Term_t valuesConstantsToFilter[SIZETUPLE];

        uint8_t nRepeatedVars = 0;
        std::pair<uint8_t, uint8_t> repeatedVars[SIZETUPLE];

        uint8_t nVarsToCopy = 0;
        uint8_t posVarsToCopy[SIZETUPLE];

        for (uint8_t i = 0; i < (uint8_t) literal.getTupleSize(); ++i) {
            VTerm t = literal.getTermAtPos(i);
            if (!t.isVariable()) {
                posConstantsToFilter[nConstantsToFilter] = i;
                valuesConstantsToFilter[nConstantsToFilter++] = t.getValue();
            } else {
                //Is it repeated?
                for (uint8_t j = i + 1; j < (uint8_t) literal.getTupleSize(); ++j) {
                    if (literal.getTermAtPos(j).isVariable() &&
                            literal.getTermAtPos(j).getId() == t.getId()) {
                        repeatedVars[nRepeatedVars].first = i;
                        repeatedVars[nRepeatedVars].second = j;
                        nRepeatedVars++;
                    }
                }

                posVarsToCopy[nVarsToCopy++] = i;
            }
        }

        BOOST_LOG_TRIVIAL(trace) << "nVarsToCopy = " << (int) nVarsToCopy << ", nRepeatedVars = " << (int) nRepeatedVars;

        while (itr != blocks.end()) {
            std::shared_ptr<const FCInternalTable> currentTable = itr->table;
            //check if literal subsumes the query
#ifdef DEBUG
            boost::chrono::system_clock::time_point timeFilter = boost::chrono::system_clock::now();
            bool shouldFilter = filterer == NULL ||
                                TableFilterer::intersection(literal, *itr);

            boost::chrono::duration<double> secFilter = boost::chrono::system_clock::now() - timeFilter;
            BOOST_LOG_TRIVIAL(trace) << "Time intersection " << secFilter.count() * 1000;
#else
            bool shouldFilter = filterer == NULL ||
                                TableFilterer::intersection(literal, *itr);
#endif
            if (shouldFilter) {
                //Extract only relevant facts with a linear scan
                std::shared_ptr<const FCInternalTable> filteredTable =
                    currentTable->filter(nVarsToCopy,
                                         posVarsToCopy,
                                         nConstantsToFilter,
                                         posConstantsToFilter,
                                         valuesConstantsToFilter,
                                         nRepeatedVars,
                                         repeatedVars);

                if (filteredTable != NULL) {
                    BOOST_LOG_TRIVIAL(trace) << "Adding to output the literal " << literal.tostring() << " with iteration " << itr->iteration;
                    output->add(filteredTable, literal, itr->rule, itr->ruleExecOrder, itr->iteration, true);
                }
            }
            itr++;
        }

        //Store the table in the cache
        if (cacheItr != cache.end()) {
            //Update the end iteration
            cacheItr->second.end = blocks[blocks.size() - 1].iteration;
        } else {
            FCCacheBlock b;
            b.table = output;
            b.begin = blocks[0].iteration;
            b.end = blocks[blocks.size() - 1].iteration;
            cache.insert(std::make_pair(signature, b));
        }

        return output;
    } else {
        throw 10;
    }
}

size_t FCTable::estimateCardInRange(const size_t mincount, const size_t maxcount) const {
    size_t output = 0;
    for (std::vector<FCBlock>::const_iterator itr = blocks.begin(); itr != blocks.end(); ++itr) {
        if (itr->iteration > maxcount) {
            break;
        }
        if (itr->iteration >= mincount) {
            output += itr->table->estimateNRows();
        }
    }
    return output;
}

/*size_t FCTable::getNRows(size_t count) const {
    size_t output = 0;
    for (std::vector<FCBlock>::const_iterator itr = blocks.begin(); itr != blocks.end(); ++itr) {
        if (itr->iteration >= count) {
            output += itr->table->getNRows();
        }
    }
    return output;
}*/

bool FCTable::isEmpty() const {
    return blocks.size() == 0;
}

bool FCTable::isEmpty(size_t count) const {
    for (std::vector<FCBlock>::const_iterator itr = blocks.begin(); itr != blocks.end(); ++itr) {
        if (itr->iteration >= count) {
            if (!itr->table->isEmpty())
                return false;
        }
    }
    return true;
}

std::shared_ptr<const Segment> FCTable::retainFrom(
    std::shared_ptr<const Segment> t,
    const bool dupl) const {
    bool passed = false;

    //BOOST_LOG_TRIVIAL(debug) << "b-seg.ncolumns=" << (int)t->getNColumns();
    for (std::vector<FCBlock>::const_iterator itr = blocks.cbegin();
            itr != blocks.cend();
            ++itr) {
        t = SegmentInserter::retain(t, itr->table, dupl);
        passed = true;
    }

    if (!passed && dupl) {
        //I still need to filter the segment.
        t = SegmentInserter::retain(t, NULL, dupl);
    }
    //BOOST_LOG_TRIVIAL(debug) << "a-seg.ncolumns=" << (int)t->getNColumns();

    return t;
}

bool FCTable::add(std::shared_ptr<const FCInternalTable> t, const Literal &literal, const RuleExecutionDetails *rule,
                  const uint8_t ruleExecOrder,
                  const size_t iteration, const bool isCompleted) {
    assert(t->getRowSize() == this->getSizeRow());
    if (t->isEmpty()) {
        return false;
    }

    if (blocks.size() > 0) {
        size_t lastItr = blocks[blocks.size() - 1].iteration;
        assert(lastItr <= iteration);
        if (lastItr == iteration) {
            FCBlock *lastBlock = &blocks[blocks.size() - 1];
            lastBlock->table = lastBlock->table->merge(t);

            //Invalidate possible subtables which contain partial results
            for (FCCache::iterator itr = cache.begin(); itr != cache.end(); ++itr) {
                if (itr->second.end == lastItr) {
                    itr->second.end = std::max<Term_t>(0, lastItr - 1);
                    itr->second.table->removeBlock(lastItr);
                }
            }

            return false;
        }
    }

    //There is no tuple with the same iteration in the table. Add a new block
    if (!t->isSorted()) {
        throw 10;
    }

    FCBlock block(iteration, t, literal, rule, ruleExecOrder, isCompleted);
    blocks.push_back(block);
    return true;
}

void FCTable::addBlock(FCBlock block) {
    assert(blocks.size() == 0 || blocks.back().iteration < block.iteration);
    blocks.push_back(block);
}

void FCTable::removeBlock(const size_t iteration) {
    assert(blocks.size() == 0 || blocks.back().iteration <= iteration);
    if (blocks.size() > 0 && blocks.back().iteration == iteration) {
        blocks.pop_back();
    }
    //assert(cache.size() == 0); //If false, then I also need to remove the block from the cache elements and update the "end" count
}

size_t FCTable::getNRows(const size_t iteration) const {
    for (std::vector<FCBlock>::const_iterator itr = blocks.begin(); itr != blocks.end(); ++itr) {
        if (itr->iteration == iteration) {
            return itr->table->getNRows();
        } else if (itr->iteration > iteration) {
            return 0;
        }
    }
    return 0;
}

size_t FCTable::getNAllRows() const {
    size_t output = 0;
    for (std::vector<FCBlock>::const_iterator itr = blocks.begin(); itr != blocks.end(); ++itr) {
#if DEBUG
        BOOST_LOG_TRIVIAL(debug) << "getNAllRows: block of " << itr->table->getNRows() << ", from iteration " << itr->iteration;
#endif
        output += itr->table->getNRows();
    }
    return output;
}

FCTable::~FCTable() {
}

FCIterator::FCIterator(
    std::vector<FCBlock>::const_iterator itr,
    std::vector<FCBlock>::const_iterator end) : ntables(end - itr) {
    this->itr = itr;
    this->end = end;
}

bool FCIterator::isEmpty() const {
    return ntables == 0 || itr == end;
}

std::shared_ptr<const FCInternalTable> FCIterator::getCurrentTable() const {
    return itr->table;
}

const FCBlock *FCIterator::getCurrentBlock() const {
    return &(*itr);
}

size_t FCIterator::getCurrentIteration() const {
    return itr->iteration;
}

const RuleExecutionDetails *FCIterator::getRule() const {
    return itr->rule;
}

void FCIterator::moveNextCount() {
    itr++;
}

size_t FCIterator::getNTables() {
    return ntables;
}
