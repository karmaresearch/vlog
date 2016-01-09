/*
   Copyright (C) 2015 Jacopo Urbani.

   This file is part of Vlog.

   Vlog is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 2 of the License, or
   (at your option) any later version.

   Vlog is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with Vlog.  If not, see <http://www.gnu.org/licenses/>.
*/

#include <vlog/resultjoinproc.h>
#include <vlog/fcinttable.h>

void copyFromArray(Term_t *dest, FCInternalTableItr *source, const uint8_t n, const int *pos) {
    for (uint8_t i = 0; i < n; ++i)
        dest[i] = source->getCurrentValue((uint8_t) pos[i]);
}

void copyFromArray(Term_t *dest, const Term_t *source, const uint8_t n, const int *pos) {
    for (uint8_t i = 0; i < n; ++i)
        dest[i] = source[(uint8_t) pos[i]];
}

/*bool InterTableJoinProcessor::eq(const Term_t *a1, const uint32_t idx) {
    for (uint8_t i = 0; i < rowsize; ++i) {
        if (a1[i] != segment->at(idx, i))
            return false;
    }
    return true;
}*/

InterTableJoinProcessor::InterTableJoinProcessor(const uint8_t rowsize,
        std::vector<std::pair<uint8_t, uint8_t>> &posFromFirst,
        std::vector<std::pair<uint8_t, uint8_t>> &posFromSecond) :
    ResultJoinProcessor(rowsize, (uint8_t) posFromFirst.size(), (uint8_t) posFromSecond.size(),
                        (posFromFirst.size() > 0) ?  & (posFromFirst[0]) : NULL,
                        posFromSecond.size() > 0 ? & (posFromSecond[0]) : NULL) {
    //Create a hashtable count of 100K elements to filter out duplicates
    currentSegmentSize = MAX_NSEGMENTS;
    segments = new std::shared_ptr<SegmentInserter>[currentSegmentSize];
    for (uint32_t i = 0; i < currentSegmentSize; ++i) {
        segments[i] = std::shared_ptr<SegmentInserter>(new SegmentInserter(rowsize));

    }
    //hashcount = new std::pair<size_t, size_t>[SIZE_HASHCOUNT];
    //memset(hashcount, 0, sizeof(std::pair<size_t, size_t>)*SIZE_HASHCOUNT);
}

void InterTableJoinProcessor::processResults(const int blockid, const Term_t *first,
        FCInternalTableItr* second, const bool unique) {

    for (uint8_t i = 0; i < nCopyFromFirst; ++i) {
        row[posFromFirst[i].first] = first[posFromFirst[i].second];
    }
    for (uint8_t i = 0; i < nCopyFromSecond; ++i) {
        row[posFromSecond[i].first] = second->getCurrentValue(posFromSecond[i].second);
    }

    /* //Calculate hashcode from the two arrays.
    size_t hashcode = Hashes::hashArray(row, rowsize);
    size_t idx = hashcode % SIZE_HASHCOUNT;
    if (hashcount[idx].first == hashcode) {
        if (eq(row, hashcount[idx].second)) {
            return;
        } else {
            hashcount[idx].second = segment->getNRows();
        }
    } else {
        hashcount[idx].first = hashcode;
        hashcount[idx].second = segment->getNRows();
    }*/

    //Add the row
    processResults(blockid, unique);
}

void InterTableJoinProcessor::processResultsAtPos(const int blockid, const uint8_t pos,
        const Term_t v, const bool unique) {
    enlargeArray(blockid);
    segments[blockid]->addAt(posFromSecond[pos].first, v);
}

void InterTableJoinProcessor::processResults(const int blockid, FCInternalTableItr *first,
        FCInternalTableItr* second, const bool unique) {
    for (uint8_t i = 0; i < nCopyFromFirst; ++i) {
        row[posFromFirst[i].first] = first->getCurrentValue(posFromFirst[i].second);

    }
    for (uint8_t i = 0; i < nCopyFromSecond; ++i) {
        row[posFromSecond[i].first] = second->getCurrentValue(posFromSecond[i].second);
    }

    /*//Calculate hashcode from the two arrays.
    size_t hashcode = Hashes::hashArray(row, rowsize);
    size_t idx = hashcode % SIZE_HASHCOUNT;
    if (hashcount[idx].first == hashcode) {
        if (eq(row, hashcount[idx].second)) {
            return;
        } else {
            hashcount[idx].second = segment->getNRows();
        }
    } else {
        hashcount[idx].first = hashcode;
        hashcount[idx].second = segment->getNRows();
    }*/

    //Add the row
    processResults(blockid, unique);
}

/*void InterTableJoinProcessor::processResults(const int blockid, const Segment *first,
        const uint32_t posFirst,
        FCInternalTableItr* second, const bool unique) {
    for (uint32_t i = 0; i < nCopyFromFirst; ++i) {
        row[posFromFirst[i].first] = first->at(posFirst, posFromFirst[i].second);
    }
    for (uint32_t i = 0; i < nCopyFromSecond; ++i) {
        row[posFromSecond[i].first] = second->getCurrentValue(posFromSecond[i].second);
    }

    //Add the row
    enlargeArray(blockid);
    for (uint32_t i = 0; i < rowsize; ++i) {
        segments[blockid]->addAt(i, row[i]);
    }
}*/

void InterTableJoinProcessor::addColumns(const int blockid,
        std::vector<std::shared_ptr<Column>> &columns,
        const bool unique, const bool sorted) {
    enlargeArray(blockid);
    for (uint8_t i = 0; i < rowsize; ++i) {
        segments[blockid]->addColumn(i, columns[i], sorted);
    }
}

void InterTableJoinProcessor::addColumn(const int blockid,
                                        const uint8_t pos,
                                        std::shared_ptr<Column> column,
                                        const bool unique, const bool sorted) {
    enlargeArray(blockid);
    segments[blockid]->addColumn(pos, column, sorted);
}

bool InterTableJoinProcessor::isBlockEmpty(const int blockId, const bool unique) const {
    return segments[blockId] == NULL || segments[blockId]->isEmpty();
}

uint32_t InterTableJoinProcessor::getRowsInBlock(const int blockId, const bool unique) const {
    return segments[blockId] == NULL ? 0 : segments[blockId]->getNRows();
}

void InterTableJoinProcessor::consolidate(const bool isFinished) {
    //Add the segment to the table
    //BOOST_LOG_TRIVIAL(debug) << "InterTableJoinProcessor::consolidate: currentSegmentSize = " << currentSegmentSize;
    //BOOST_LOG_TRIVIAL(debug) << "  rowsize = " << (int)rowsize;
    for (uint32_t i = 0; i < currentSegmentSize; ++i) {
        if (segments[i] != NULL && !segments[i]->isEmpty()) {
            if (table == NULL) {
                table = std::shared_ptr<const FCInternalTable>(
                            new InmemoryFCInternalTable(rowsize, 0,
                                                        segments[i]->getNRows() < 2,
                                                        segments[i]->getSegment()));
            } else {
                table = ((InmemoryFCInternalTable*)table.get())->merge(
                            segments[i]->getSegment());
            }
            uint8_t rs = table->getRowSize();
            segments[i] = std::shared_ptr<SegmentInserter>(
                              new SegmentInserter(rs));
        }
    }
    //BOOST_LOG_TRIVIAL(debug) << "InterTableJoinProcessor::consolidate done";
}

std::shared_ptr<const FCInternalTable> InterTableJoinProcessor::getTable() {
    return table;
}

void InterTableJoinProcessor::processResults(const int blockid, const bool unique) {
#if USE_DUPLICATE_DETECTION
    if (! unique && rowsHash == NULL && rowCount == TMPT_THRESHOLD) {
	BOOST_LOG_TRIVIAL(debug) << "Threshold reached in IntertableJoinProcessor; creating rowsHash; rowCount = " << rowCount
	    << ", rowsize = " << (int) rowsize;
	consolidate(false);
	rowsHash = new HashSet(2*TMPT_THRESHOLD);
	std::vector<Term_t> null;
	for (int j = 0; j < rowsize; j++) {
	    null.push_back((Term_t) -1);
	}
	rowsHash->set_empty_key(null);
	FCInternalTableItr *itr = getTable()->getIterator();
	size_t cnt = 0;
	while (itr->hasNext()) {
	    std::vector<Term_t> hashRow(rowsize);
	    itr->next();
	    for (uint8_t i = 0; i < rowsize; i++) {
		hashRow[i] = itr->getCurrentValue(i);
	    }
	    rowsHash->insert(hashRow);
	    cnt++;
	    if (cnt % 100000 == 0) {
		BOOST_LOG_TRIVIAL(debug) << "cnt = " << cnt << ", hash size = " << rowsHash->size();
	    }
	}
	getTable()->releaseIterator(itr);
    }
    if (rowsHash != NULL) {
	std::vector<Term_t> hashRow(rowsize);
	
	for (uint8_t i = 0; i < rowsize; i++) {
	    hashRow[i] = row[i];
	}
	if (! rowsHash->insert(hashRow).second) {
	    return;
	}
	if (rowCount > (64*1000*1000) && ((float) rowsHash->size()) / rowCount > .9) {
	    delete rowsHash;
	    rowsHash = NULL;
	}
#if DEBUG
	else if (rowsHash->size() % 100000 == 0) {
	    BOOST_LOG_TRIVIAL(debug) << "rowCount = " << (rowCount + 1) << ", hash size = " << rowsHash->size();
	}
#endif /* DEBUG */
    }
    rowCount++;
#endif /* USE_DUPLICATE_DETECTION */
    enlargeArray(blockid);
    for (uint8_t i = 0; i < rowsize; ++i) {
        segments[blockid]->addAt(i, row[i]);
    }
}

InterTableJoinProcessor::~InterTableJoinProcessor() {
    delete[] segments;
}

FinalTableJoinProcessor::FinalTableJoinProcessor(
    std::vector<std::pair<uint8_t, uint8_t>> &posFromFirst,
    std::vector<std::pair<uint8_t, uint8_t>> &posFromSecond,
    std::vector<FCBlock> &listDerivations,
    FCTable *table, Literal &head,
    const RuleExecutionDetails *ruleDetails,
    const uint8_t ruleExecOrder,
    const size_t iteration) :
    ResultJoinProcessor(table->getSizeRow(),
                        (uint8_t) posFromFirst.size(),
                        (uint8_t) posFromSecond.size(),
                        posFromFirst.size() > 0 ? & (posFromFirst[0]) : NULL,
                        posFromSecond.size() > 0 ? & (posFromSecond[0]) : NULL),
    listDerivations(listDerivations),
    t(table),
    literal(head),
    ruleDetails(ruleDetails),
    ruleExecOrder(ruleExecOrder),
    iteration(iteration) {

    for (int i = 0; i < head.getTupleSize(); ++i) {
        Term t = head.getTermAtPos(i);
        if (!t.isVariable()) {
            row[i] = t.getValue();
        }
    }

#if DEBUG
    for (int i = 0; i < posFromFirst.size(); i++) {
        BOOST_LOG_TRIVIAL(debug) << "posFromFirst[" << i << "] = ("
                                 << (int) posFromFirst[i].first << ", " << (int) posFromFirst[i].second << ")";
    }
    for (int i = 0; i < posFromSecond.size(); i++) {
        BOOST_LOG_TRIVIAL(debug) << "posFromSecond[" << i << "] = ("
                                 << (int) posFromSecond[i].first << ", " << (int) posFromSecond[i].second << ")";
    }
#endif

    //Create a temporary table
    nbuffers = 0;
    tmpt = utmpt = NULL;
    tmptseg = NULL;
    enlargeBuffers(3);

}

void FinalTableJoinProcessor::enlargeBuffers(const int newsize) {
    if (nbuffers < newsize) {
        SegmentInserter** newb1 = new SegmentInserter*[newsize];
        SegmentInserter** newb2 = new SegmentInserter*[newsize];
        std::shared_ptr<const Segment> *newtmptseg = new std::shared_ptr<const Segment>[newsize];
        memset(newb1, 0, sizeof(SegmentInserter*)*newsize);
        memset(newb2, 0, sizeof(SegmentInserter*)*newsize);
        memset(newtmptseg, 0, sizeof(std::shared_ptr<const Segment>)*newsize);
        for (int i = 0; i < nbuffers; ++i) {
            newb1[i] = tmpt[i];
            newb2[i] = utmpt[i];
            newtmptseg[i] = tmptseg[i];
        }

        if (tmpt != NULL)
            delete[] tmpt;
        if (utmpt != NULL)
            delete[] utmpt;
        if (tmptseg != NULL)
            delete[] tmptseg;
        tmpt = newb1;
        utmpt = newb2;
        tmptseg = newtmptseg;
        nbuffers = newsize;
    }
}

void FinalTableJoinProcessor::processResults(const int blockid, const Term_t *first,
        FCInternalTableItr *second, const bool unique) {
    copyRawRow(first, second);
    processResults(blockid, unique);
}

void FinalTableJoinProcessor::processResults(const int blockid, FCInternalTableItr *first,
        FCInternalTableItr* second, const bool unique) {
    for (uint32_t i = 0; i < nCopyFromFirst; ++i) {
        row[posFromFirst[i].first] = first->getCurrentValue(posFromFirst[i].second);
    }
    for (uint32_t i = 0; i < nCopyFromSecond; ++i) {
        row[posFromSecond[i].first] = second->getCurrentValue(posFromSecond[i].second);
    }

    processResults(blockid, unique);
}

void FinalTableJoinProcessor::processResultsAtPos(const int blockid, const uint8_t pos,
        const Term_t v, const bool unique) {
    enlargeBuffers(blockid + 1);
    if (!unique) {
        if (tmpt[blockid] == NULL) {
            tmpt[blockid] = new SegmentInserter(rowsize);
        }
        tmpt[blockid]->addAt(posFromSecond[pos].first, v);
    } else {
        if (utmpt[blockid] == NULL) {
            utmpt[blockid] = new SegmentInserter(rowsize);
        }
        utmpt[blockid]->addAt(posFromSecond[pos].first, v);
    }
}

bool FinalTableJoinProcessor::isBlockEmpty(const int blockId, const bool unique) const {
    if (!unique) {
        return tmpt[blockId] == NULL || tmpt[blockId]->isEmpty();
    } else {
        return utmpt[blockId] == NULL || utmpt[blockId]->isEmpty();
    }
}

uint32_t FinalTableJoinProcessor::getRowsInBlock(const int blockId,
        const bool unique) const {
    if (!unique) {
        return tmpt[blockId] == NULL ? 0 : tmpt[blockId]->getNRows();
    } else {
        return utmpt[blockId] == NULL ? 0 : utmpt[blockId]->getNRows();
    }

}

void FinalTableJoinProcessor::addColumns(const int blockid,
        std::vector<std::shared_ptr<Column>> &columns,
        const bool unique, const bool sorted) {
    enlargeBuffers(blockid + 1);
    if (!unique) {
        if (tmpt[blockid] == NULL) {
            tmpt[blockid] = new SegmentInserter(rowsize);
        }
        tmpt[blockid]->addColumns(columns, sorted, false);
    } else {
        if (utmpt[blockid] == NULL) {
            utmpt[blockid] = new SegmentInserter(rowsize);
        }
        utmpt[blockid]->addColumns(columns, sorted, false);
    }
}

void FinalTableJoinProcessor::addColumns(const int blockid,
        FCInternalTableItr *itr, const bool unique,
        const bool sorted, const bool lastInsert) {
    enlargeBuffers(blockid + 1);
    if (unique) {
        if (utmpt[blockid] == NULL) {
            utmpt[blockid] = new SegmentInserter(rowsize);
        }

        uint8_t columns[128];
        for (uint32_t i = 0; i < nCopyFromSecond; ++i) {
            columns[i] = posFromSecond[i].second;
        }
        std::vector<std::shared_ptr<Column>> c =
                                              itr->getColumn(nCopyFromSecond,
                                                      columns);
        if (nCopyFromSecond > 1) {
            std::vector<std::shared_ptr<Column>> c2;
            int rowID = 0;
            int largest = -1;
            for (int i = 0; i < nCopyFromSecond; ++i) {
                //Get the row with the smallest ID
                int minID = INT_MAX;
                for (int j = 0; j <  nCopyFromSecond; ++j) {
                    if (posFromSecond[j].first > largest &&
                            posFromSecond[j].first < minID) {
                        rowID = j;
                        minID = posFromSecond[j].first;
                    }
                }
                c2.push_back(c[rowID]);
                largest = posFromSecond[rowID].first;
            }
            assert(c2.size() == c.size());
            c = c2;
        }

        if (c.size() < rowsize) {
            assert(lastInsert); //If it is not the last insert, the size
            //of the columns can change. I must address this case. For
            //now, I catch it.

            //The head contains also constants. We must add fields in the vector
            // of created columns.
            std::vector<std::shared_ptr<Column>> newc;
            int idxVar = 0;
            for (int i = 0; i < rowsize; ++i) {
                if (!literal.getTermAtPos(i).isVariable()) {
                    newc.push_back(std::shared_ptr<Column>(
                                       new CompressedColumn(row[i],
                                                            c[0]->size())));
                } else {
                    newc.push_back(c[idxVar++]);
                }
            }
            c = newc;
        }

        utmpt[blockid]->addColumns(c, sorted, lastInsert);
    } else {
        if (tmpt[blockid] == NULL) {
            tmpt[blockid] = new SegmentInserter(rowsize);
        }

        uint8_t columns[128];
        for (uint32_t i = 0; i < nCopyFromSecond; ++i) {
            columns[i] = posFromSecond[i].second;
        }
        std::vector<std::shared_ptr<Column>> c =
                                              itr->getColumn(nCopyFromSecond,
                                                      columns);
        assert(c.size() <= rowsize);

        if (nCopyFromSecond > 1) {
            std::vector<std::shared_ptr<Column>> c2;
            int rowID = 0;
            int largest = -1;
            for (int i = 0; i < nCopyFromSecond; ++i) {
                //Get the row with the smallest ID
                int minID = INT_MAX;
                for (int j = 0; j <  nCopyFromSecond; ++j) {
                    if (posFromSecond[j].first > largest &&
                            posFromSecond[j].first < minID) {
                        rowID = j;
                        minID = posFromSecond[j].first;
                    }
                }
                c2.push_back(c[rowID]);
                largest = posFromSecond[rowID].first;
            }
            assert(c2.size() == c.size());
            c = c2;
        }

        if (c.size() < rowsize) {
            //The head contains also constants. We must add fields in the vector
            // of created columns.
            std::vector<std::shared_ptr<Column>> newc;
            int idxVar = 0;
            for (int i = 0; i < rowsize; ++i) {
                if (!literal.getTermAtPos(i).isVariable()) {
                    newc.push_back(std::shared_ptr<Column>(
                                       new CompressedColumn(row[i],
                                                            c[0]->size())));
                } else {
                    newc.push_back(c[idxVar++]);
                }
            }
            c = newc;
        }

        tmpt[blockid]->addColumns(c, sorted, lastInsert);
    }
}

void FinalTableJoinProcessor::addColumn(const int blockid,
                                        const uint8_t pos, std::shared_ptr<Column> column,
                                        const bool unique, const bool sorted) {
    enlargeBuffers(blockid + 1);
    if (!unique) {
        if (tmpt[blockid] == NULL) {
            tmpt[blockid] = new SegmentInserter(rowsize);
        }
        tmpt[blockid]->addColumn(pos, column, sorted);
    } else {
        if (utmpt[blockid] == NULL) {
            utmpt[blockid] = new SegmentInserter(rowsize);
        }
        utmpt[blockid]->addColumn(pos, column, sorted);
    }
}

void FinalTableJoinProcessor::processResults(const int blockid, const bool unique) {
    enlargeBuffers(blockid + 1);

    if (!unique) {
        if (tmpt[blockid] == NULL) {
            tmpt[blockid] = new SegmentInserter(rowsize);
        }

#if USE_DUPLICATE_DETECTION
	rowCount++;
	if (rowsHash != NULL) {
	    std::vector<Term_t> hashRow(rowsize);
	    
	    for (uint8_t i = 0; i < rowsize; i++) {
		hashRow[i] = row[i];
	    }
	    if (! rowsHash->insert(hashRow).second) {
		return;
	    }
	    if (rowCount > (64*1000*1000) && ((float) rowsHash->size()) / rowCount > .9) {
		// Apparently, the hash is not effective.
		delete rowsHash;
		rowsHash = NULL;
	    }
#if DEBUG
	    else if (rowsHash->size() % 100000 == 0) {
		BOOST_LOG_TRIVIAL(debug) << "rowCount = " << rowCount << ", hash size = " << rowsHash->size();
	    }
#endif
	}
#endif

        tmpt[blockid]->addRow(row);

#if USE_DUPLICATE_DETECTION
        if (rowsHash == NULL && rowCount == TMPT_THRESHOLD) {
	    BOOST_LOG_TRIVIAL(debug)
		<< "Threshold reached in FinaltableJoinProcessor; creating rowsHash; rowCount = "
		<< rowCount << ", rowsize = " << (int) rowsize;
	    rowsHash = new HashSet(2*TMPT_THRESHOLD);
	    std::vector<Term_t> null;
	    for (int j = 0; j < rowsize; j++) {
		null.push_back((Term_t) -1);
	    }
	    rowsHash->set_empty_key(null);

            std::shared_ptr<const Segment> seg = tmpt[blockid]->getSegment();
	    std::unique_ptr<SegmentIterator> itr = seg->iterator();
	    size_t cnt = 0;
	    while (itr->hasNext()) {
		std::vector<Term_t> hashRow(rowsize);
		itr->next();
		for (uint8_t i = 0; i < rowsize; i++) {
		    hashRow[i] = itr->get(i);
		}
		rowsHash->insert(hashRow);
		cnt++;
		if (cnt % 100000 == 0) {
		    BOOST_LOG_TRIVIAL(debug) << "cnt = " << cnt << ", hash size = " << rowsHash->size();
		}
	    }
	    itr->clear();
#else
	if (tmpt[blockid]->getNRows() > TMPT_THRESHOLD) {
            if (tmptseg[blockid] != NULL && tmptseg[blockid]->getNRows() > tmpt[blockid]->getNRows()) {
                // Only start sorting and merging if it is large enough.
                return;
            }
            BOOST_LOG_TRIVIAL(debug) << "tmpt size is " << tmpt[blockid]->getNRows() << ". SortAndUnique ...";
            std::shared_ptr<const Segment> seg = tmpt[blockid]->
                                                 getSortedAndUniqueSegment();
            BOOST_LOG_TRIVIAL(debug) << "resulting segment has size " << seg->getNRows();
            if (tmptseg[blockid] == NULL) {
                tmptseg[blockid] = seg;
            } else {
                BOOST_LOG_TRIVIAL(debug) << "Merging with tmptseg, size = " << tmptseg[blockid]->getNRows();
                std::vector<std::shared_ptr<const Segment>> segmentsToMerge;
                segmentsToMerge.push_back(seg);
                segmentsToMerge.push_back(tmptseg[blockid]);
                tmptseg[blockid] = SegmentInserter::merge(segmentsToMerge);
            }
#endif
            delete tmpt[blockid];
            tmpt[blockid] = new SegmentInserter(rowsize);
#if USE_DUPLICATE_DETECTION
	    for (HashSet::iterator itr = rowsHash->begin(); itr != rowsHash->end(); itr++) {
		for (uint8_t i = 0; i < rowsize; i++) {
		    row[i] = (*itr)[i];
		}
		tmpt[blockid]->addRow(row);
	    }
#endif
        }
    } else {
        if (utmpt[blockid] == NULL) {
            utmpt[blockid] = new SegmentInserter(rowsize);
        }
        utmpt[blockid]->addRow(row);
    }
}

void FinalTableJoinProcessor::copyRawRow(const Term_t *first,
        const Term_t* second) {
    for (uint32_t i = 0; i < nCopyFromFirst; ++i) {
        row[posFromFirst[i].first] = first[posFromFirst[i].second];
    }
    for (uint32_t i = 0; i < nCopyFromSecond; ++i) {
        row[posFromSecond[i].first] = second[posFromSecond[i].second];
    }
}

void FinalTableJoinProcessor::copyRawRow(const Term_t *first,
        FCInternalTableItr* second) {
    for (uint32_t i = 0; i < nCopyFromFirst; ++i) {
        row[posFromFirst[i].first] = first[posFromFirst[i].second];
    }
    for (uint32_t i = 0; i < nCopyFromSecond; ++i) {
        row[posFromSecond[i].first] = second->getCurrentValue(posFromSecond[i].second);
    }
}

void FinalTableJoinProcessor::consolidate(const bool isFinished) {

    //If not finished, clean up the tmpbuffers

    if (utmpt != NULL) {
        for (int i = 0; i < nbuffers; ++i) {
            if (utmpt[i] != NULL && !utmpt[i]->isEmpty()) {
                if (!utmpt[i]->isSorted()) {
                    std::shared_ptr<const Segment> sortedSegment = utmpt[i]->getSegment()->sortBy(NULL);
                    std::shared_ptr<const FCInternalTable> ptrTable(new InmemoryFCInternalTable(rowsize, iteration, true, sortedSegment));
                    t->add(ptrTable, literal, ruleDetails, ruleExecOrder, iteration, isFinished);
                } else {
                    std::shared_ptr<const FCInternalTable> ptrTable(new InmemoryFCInternalTable(rowsize, iteration, true, utmpt[i]->getSegment()));
                    t->add(ptrTable, literal, ruleDetails, ruleExecOrder, iteration, isFinished);
                }

                delete utmpt[i];
                utmpt[i] = new SegmentInserter(rowsize);
            }
        }
    }

    if (tmpt != NULL) {
        for (int i = 0; i < nbuffers; ++i) {
            if (tmpt[i] != NULL && !tmpt[i]->isEmpty()) {
//        BOOST_LOG_TRIVIAL(debug) << "Rule produced " << tmpt->getNRows() << " possibly duplicated derivations";

//                boost::chrono::system_clock::time_point startS = boost::chrono::system_clock::now();
                std::shared_ptr<const Segment> seg = tmpt[i]->
                                                     getSortedAndUniqueSegment();
                if (tmptseg[i] != NULL) {
                    std::vector<std::shared_ptr<const Segment>> segmentsToMerge;
                    segmentsToMerge.push_back(seg);
                    segmentsToMerge.push_back(tmptseg[i]);
                    seg = SegmentInserter::merge(segmentsToMerge);
                }

                //This should always be sorted
//                boost::chrono::duration<double> secS = boost::chrono::system_clock::now() - startS;
//                cout << "Time retain " <<secS.count() * 1000 << endl;

                /*if (!tmpt[i]->isSorted()) {
                    seg = seg->sortBy(NULL);
                }

                bool dupl = tmpt[i]->containsDuplicates();
                if (seg->getNColumns() == 1 && dupl) {
                    seg = SegmentInserter::unique(seg);
                    dupl = false;
                }*/

                //Remove all data already existing
                seg = t->retainFrom(seg, false);

                if (!seg->isEmpty()) {
                    std::shared_ptr<const FCInternalTable> ptrTable(
                        new InmemoryFCInternalTable(rowsize,
                                                    iteration,
                                                    true,
                                                    seg));

                    t->add(ptrTable, literal, ruleDetails, ruleExecOrder,
                           iteration, isFinished);
                }

                delete tmpt[i];
                tmpt[i] = new SegmentInserter(rowsize);
            }
        }
    }

    //Add the block just created to the global list of derivations
    if (!t->isEmpty()) {
        FCBlock &block = t->getLastBlock();
        if (block.iteration == iteration) {
            if (isFinished && !block.isCompleted)
                block.isCompleted = true;

            if (listDerivations.size() == 0 ||
                    listDerivations.back().iteration != iteration) {
                listDerivations.push_back(block);
            }
        }
    }
}

FinalTableJoinProcessor::~FinalTableJoinProcessor() {
    if (utmpt != NULL) {
        for (int i = 0; i < nbuffers; ++i)
            if (utmpt[i] != NULL)
                delete utmpt[i];
        delete[] utmpt;
    }

    if (tmpt != NULL) {
        for (int i = 0; i < nbuffers; ++i) {
            if (tmpt[i] != NULL)
                delete tmpt[i];
        }
        delete[] tmpt;
    }
    if (tmptseg != NULL) {
        delete[] tmptseg;
    }
}
