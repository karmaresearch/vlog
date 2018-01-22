#include <vlog/filterer.h>
#include <vlog/fctable.h>
#include <vlog/joinprocessor.h>
#include <vlog/concepts.h>

#include <trident/model/table.h>

// Note: When running multithreaded, mutex != NULL.

FCTable::FCTable(std::mutex *mutex, const uint8_t sizeRow) :
    sizeRow(sizeRow), mutex(mutex) {
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
    FCIterator i;

    std::vector<FCBlock>::const_iterator itr = blocks.begin();
    while (itr != blocks.end() && itr->iteration < iteration) {
        itr++;
    }
    if (itr != blocks.end()) {
        i = FCIterator(itr, blocks.end());
    } else {
        i = FCIterator();
    }

    return i;
}

FCIterator FCTable::read(const size_t mincount, const size_t maxcount) const {
    FCIterator i;
    std::vector<FCBlock>::const_iterator itr = blocks.begin();
    while (itr != blocks.end() && itr->iteration < mincount) {
        itr++;
    }
    if (itr != blocks.end()) {
        std::vector<FCBlock>::const_iterator endrange = itr + 1;
        while (endrange != blocks.end() && endrange->iteration <= maxcount) {
            endrange++;
        }
        i = FCIterator(itr, endrange);
    } else {
        i = FCIterator();
    }
    return i;
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
        estimation += itr.getCurrentTable()->estimateNRows(
                nconstants, posConstants,
                valueConstants);
        itr.moveNextCount();
    }
    return estimation;
}

std::shared_ptr<const FCTable> FCTable::filter(const Literal &literal,
        const size_t minIteration, TableFilterer *filterer, int nthreads) {
    bool shouldFilter = literal.getNUniqueVars() < literal.getTupleSize();

    if (shouldFilter) {
        if (blocks.size() == 0) {
            return std::shared_ptr<FCTable>(this);;
        }
        std::shared_ptr<FCTable> output;
        std::vector<FCBlock>::iterator itr = blocks.begin();
        std::string signature = getSignature(literal);

        if (mutex != NULL) {
            // We need a separate lock for the cache. We cannot promote the mutex to an exclusive
            // lock here, since that leads to deadlocks.
            cache_mutex.lock();
        }

        FCCache::iterator cacheItr = cache.find(signature);
        if (cacheItr != cache.end()) {
            output = cacheItr->second.table;

            //First update the entry if there are more entries. Otherwise return
            if (cacheItr->second.end < blocks[blocks.size() - 1].iteration) {
                while (itr != blocks.end() && itr->iteration <= cacheItr->second.end) {
                    itr++;
                }
            } else {
                if (mutex != NULL) {
                    cache_mutex.unlock();
                }
                return output;
            }
        } else {
            output = std::shared_ptr<FCTable>(new FCTable(mutex, literal.getNVars()));
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

        while (itr != blocks.end()) {
            std::shared_ptr<const FCInternalTable> currentTable = itr->table;
            //check if literal subsumes the query
#ifdef DEBUG
            std::chrono::system_clock::time_point timeFilter = std::chrono::system_clock::now();
            bool shouldFilter = filterer == NULL ||
                TableFilterer::intersection(literal, *itr);

            std::chrono::duration<double> secFilter = std::chrono::system_clock::now() - timeFilter;
            LOG(TRACEL) << "Time intersection " << secFilter.count() * 1000;
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
                            repeatedVars,
                            nthreads);

                if (filteredTable != NULL) {
                    output->add(filteredTable, literal, itr->posQueryInRule,
                            itr->rule, itr->ruleExecOrder, itr->iteration, true, nthreads);
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
        if (mutex != NULL) {
            cache_mutex.unlock();
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

bool FCTable::isEmpty() const {
    return blocks.size() == 0;
}

bool FCTable::isEmpty(size_t count) const {
    for (std::vector<FCBlock>::const_iterator itr = blocks.begin(); itr != blocks.end(); ++itr) {
        if (itr->iteration >= count) {
            if (!itr->table->isEmpty()) {
                return false;
            }
        }
    }
    return true;
}

std::shared_ptr<const Segment> FCTable::retainFrom(
        std::shared_ptr<const Segment> t,
        const bool dupl,
        int nthreads) const {
    bool passed = false;

    size_t sz = 0;

    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();

    for (std::vector<FCBlock>::const_iterator itr = blocks.cbegin();
            itr != blocks.cend();
            ++itr) {
        sz += itr->table->getNRows();
    }
    //    LOG(TRACEL) << "retainFrom: t.size() = " << t->getNRows() << ", blocks.size() = " << blocks.size() << ", sz = " << sz;
    for (std::vector<FCBlock>::const_iterator itr = blocks.cbegin();
            itr != blocks.cend();
            ++itr) {
        t = SegmentInserter::retain(t, itr->table, dupl, nthreads);
        //        LOG(TRACEL) << "after retain: t.size() = " << t->getNRows() << ", table size was " << itr->table->getNRows();
        passed = true;
    }

    if (!passed && dupl) {
        //I still need to filter the segment.
        t = SegmentInserter::retain(t, NULL, dupl, nthreads);
    }
    std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    LOG(TRACEL) << "Time retainFrom = " << sec.count() * 1000;

    return t;
}

bool FCTable::add(std::shared_ptr<const FCInternalTable> t,
        const Literal &literal,
        const uint8_t posLiteralInRule,
        const RuleExecutionDetails *rule,
        const uint8_t ruleExecOrder,
        const size_t iteration, const bool isCompleted,
        int nthreads) {
    assert(t->getRowSize() == this->getSizeRow());
    if (t->isEmpty()) {
        return false;
    }

    size_t sz = blocks.size();
    if (sz > 0) {
        size_t lastItr = blocks[sz - 1].iteration;
        if (mutex != NULL) {
            // Find correct block
            while (sz > 0) {
                if (lastItr != iteration) {
                    sz--;
                    if (sz > 0) {
                        lastItr = blocks[sz - 1].iteration;
                    }
                } else {
                    break;
                }
            }
        } else {
            assert(lastItr <= iteration);
        }

        if (lastItr == iteration) {
            FCBlock *lastBlock = &blocks[sz - 1];
            lastBlock->table = lastBlock->table->merge(t, nthreads);

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

    FCBlock block(iteration, t, literal, posLiteralInRule,
            rule, ruleExecOrder, isCompleted);
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
}

size_t FCTable::getNRows(const size_t iteration) const {
    size_t out = 0;
    for (std::vector<FCBlock>::const_iterator itr = blocks.begin(); itr != blocks.end(); ++itr) {
        if (itr->iteration == iteration) {
            out = itr->table->getNRows();
            break;
        } else if (mutex != NULL && itr->iteration > iteration) {
            break;
        }
    }
    return out;
}

size_t FCTable::getNAllRows() const {
    size_t output = 0;
    for (std::vector<FCBlock>::const_iterator itr = blocks.begin(); itr != blocks.end(); ++itr) {
        //LOG(TRACEL) << "getNAllRows: block of " << itr->table->getNRows() << ", from iteration " << itr->iteration;
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
