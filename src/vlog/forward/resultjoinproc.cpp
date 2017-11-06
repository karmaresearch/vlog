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
        std::vector<std::pair<uint8_t, uint8_t>> &posFromSecond,
        int nthreads) :
    ResultJoinProcessor(rowsize, (uint8_t) posFromFirst.size(), (uint8_t) posFromSecond.size(),
            (posFromFirst.size() > 0) ?  & (posFromFirst[0]) : NULL,
            posFromSecond.size() > 0 ? & (posFromSecond[0]) : NULL, nthreads) {
        currentSegmentSize = MAX_NSEGMENTS;
        segments = new std::shared_ptr<SegmentInserter>[currentSegmentSize];
        for (uint32_t i = 0; i < currentSegmentSize; ++i) {
            segments[i] = std::shared_ptr<SegmentInserter>(new SegmentInserter(rowsize));

        }
    }

void InterTableJoinProcessor::processResults(const int blockid, const Term_t *first,
        FCInternalTableItr* second, const bool unique) {

    for (uint8_t i = 0; i < nCopyFromFirst; ++i) {
        row[posFromFirst[i].first] = first[posFromFirst[i].second];
    }
    for (uint8_t i = 0; i < nCopyFromSecond; ++i) {
        row[posFromSecond[i].first] = second->getCurrentValue(posFromSecond[i].second);
    }
    //Add the row
    processResults(blockid, unique, NULL);
}

void InterTableJoinProcessor::processResults(std::vector<int> &blockid, Term_t *p,
        std::vector<bool> &unique, std::mutex *m) {

    int newBufsize = currentSegmentSize;
    for (int j = 0; j < blockid.size(); j++) {
        if (blockid[j] > newBufsize) {
            newBufsize = blockid[j];
        }
    }
    enlargeArray(newBufsize);
    for (int j = 0; j < blockid.size(); j++) {
        for (uint8_t i = 0; i < nCopyFromFirst; ++i) {
            row[posFromFirst[i].first] = *p;
            p++;
        }
        for (uint8_t i = 0; i < nCopyFromSecond; ++i) {
            row[posFromSecond[i].first] = *p;
            p++;
        }

        //Add the row
#if USE_DUPLICATE_DETECTION
        processResults(blockid[j], unique[j], m);
#else
        segments[blockid[j]]->addRow(row, rowsize);
#endif
    }
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

    //Add the row
    processResults(blockid, unique, NULL);
}

void InterTableJoinProcessor::processResults(const int blockid,
        const std::vector<const std::vector<Term_t> *> &vectors1, size_t i1,
        const std::vector<const std::vector<Term_t> *> &vectors2, size_t i2,
        const bool unique) {
    for (int i = 0; i < nCopyFromFirst; i++) {
        row[posFromFirst[i].first] = (*vectors1[posFromFirst[i].second])[i1];
    }
    for (int i = 0; i < nCopyFromSecond; i++) {
        row[posFromSecond[i].first] = (*vectors2[posFromSecond[i].second])[i2];
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
    processResults(blockid, unique, NULL);
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
segments[blockid]->addRow(row, rowsize);
}*/

void InterTableJoinProcessor::addColumns(const int blockid,
        std::vector<std::shared_ptr<Column>> &columns,
        const bool unique, const bool sorted) {
    enlargeArray(blockid);
    for (uint8_t i = 0; i < rowsize; ++i) {
        segments[blockid]->addColumn(i, columns[i], sorted);
    }
#if DEBUG
    checkSizes();
#endif
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

bool InterTableJoinProcessor::isEmpty() const {
    for (int i = 0; i < currentSegmentSize; ++i) {
        if (segments[i] != NULL && !segments[i]->isEmpty())
            return false;
    }
    return true;
}

uint32_t InterTableJoinProcessor::getRowsInBlock(const int blockId, const bool unique) const {
    return segments[blockId] == NULL ? 0 : segments[blockId]->getNRows();
}

void InterTableJoinProcessor::consolidate(const bool isFinished) {
    //Add the segment to the table
    //LOG(DEBUGL) << "InterTableJoinProcessor::consolidate: currentSegmentSize = " << currentSegmentSize;
    //LOG(DEBUGL) << "  rowsize = " << (int)rowsize;
    for (uint32_t i = 0; i < currentSegmentSize; ++i) {
        if (segments[i] != NULL && !segments[i]->isEmpty()) {
            if (table == NULL) {
                table = std::shared_ptr<const FCInternalTable>(
                        new InmemoryFCInternalTable(rowsize, 0,
                            segments[i]->getNRows() < 2,
                            segments[i]->getSegment()));
            } else {
                table = ((InmemoryFCInternalTable*)table.get())->merge(
                        segments[i]->getSegment(), nthreads);
            }
            uint8_t rs = table->getRowSize();
            assert(rs == rowsize);
            segments[i] = std::shared_ptr<SegmentInserter>(
                    new SegmentInserter(rs));
        }
    }
    //LOG(DEBUGL) << "InterTableJoinProcessor::consolidate done";
}

std::shared_ptr<const FCInternalTable> InterTableJoinProcessor::getTable() {
    return table;
}

#if USE_DUPLICATE_DETECTION
void InterTableJoinProcessor::processResults(const int blockid, const bool unique, std::mutex *m) {
    if (! unique && rowsHash == NULL && rowCount == TMPT_THRESHOLD) {
        LOG(DEBUGL) << "Threshold reached in IntertableJoinProcessor; creating rowsHash; rowCount = " << rowCount
            << ", rowsize = " << (int) rowsize;
        consolidate(false);
        rowsHash = new HashSet(2 * TMPT_THRESHOLD);
        std::vector<Term_t> null;
        for (int j = 0; j < rowsize; j++) {
            null.push_back((Term_t) - 1);
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
                LOG(DEBUGL) << "cnt = " << cnt << ", hash size = " << rowsHash->size();
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
        if (rowCount > (64 * 1000 * 1000) && ((float) rowsHash->size()) / rowCount > .9) {
            delete rowsHash;
            rowsHash = NULL;
        } else if (rowsHash->size() % 100000 == 0) {
            LOG(DEBUGL) << "rowCount = " << (rowCount + 1) << ", hash size = " << rowsHash->size();
        }
    }
    rowCount++;
    enlargeArray(blockid);
    segments[blockid]->addRow(row, rowsize);
}
#endif /* USE_DUPLICATE_DETECTION */

InterTableJoinProcessor::~InterTableJoinProcessor() {
    delete[] segments;
}

void FinalRuleProcessor::processResults(const int blockid, const bool unique,
        std::mutex *m) {
    enlargeBuffers(blockid + 1);
    if (!unique) {
        if (tmpt[blockid] == NULL) {
            tmpt[blockid] = new SegmentInserter(rowsize);
        }

        tmpt[blockid]->addRow(row);
        if (tmpt[blockid]->getNRows() > TMPT_THRESHOLD) {
            mergeTmpt(blockid, unique, m);
        }
    } else {
        if (utmpt[blockid] == NULL) {
            utmpt[blockid] = new SegmentInserter(rowsize);
        }
        utmpt[blockid]->addRow(row);
    }
}

void FinalRuleProcessor::processResults(std::vector<int> &blockid, Term_t *p,
        std::vector<bool> &unique, std::mutex *m) {

    for (int j = 0; j < blockid.size(); j++) {
        for (uint8_t i = 0; i < nCopyFromFirst; ++i) {
            row[posFromFirst[i].first] = *p;
            p++;
        }
        for (uint8_t i = 0; i < nCopyFromSecond; ++i) {
            row[posFromSecond[i].first] = *p;
            p++;
        }

        //Add the row
        processResults(blockid[j], unique[j], m);
    }
}

FinalRuleProcessor::FinalRuleProcessor(
        std::vector<std::pair<uint8_t, uint8_t>> &posFromFirst,
        std::vector<std::pair<uint8_t, uint8_t>> &posFromSecond,
        std::vector<FCBlock> &listDerivations,
        FCTable *table, Literal &head,
        const RuleExecutionDetails *ruleDetails,
        const uint8_t ruleExecOrder,
        const size_t iteration,
        const bool addToEndTable,
        const int nthreads) :
    ResultJoinProcessor(table->getSizeRow(),
            (uint8_t) posFromFirst.size(),
            (uint8_t) posFromSecond.size(),
            posFromFirst.size() > 0 ? & (posFromFirst[0]) : NULL,
            posFromSecond.size() > 0 ? & (posFromSecond[0]) : NULL, nthreads),
    listDerivations(listDerivations),
    t(table),
    ruleExecOrder(ruleExecOrder),
    iteration(iteration),
    addToEndTable(addToEndTable),
    newDerivation(false),
    ruleDetails(ruleDetails),
    literal(head) {

        for (int i = 0; i < head.getTupleSize(); ++i) {
            VTerm t = head.getTermAtPos(i);
            if (!t.isVariable()) {
                row[i] = t.getValue();
            }
        }

#if DEBUG
        for (int i = 0; i < posFromFirst.size(); i++) {
            LOG(DEBUGL) << "posFromFirst[" << i << "] = ("
                << (int) posFromFirst[i].first << ", " << (int) posFromFirst[i].second << ")";
        }
        for (int i = 0; i < posFromSecond.size(); i++) {
            LOG(DEBUGL) << "posFromSecond[" << i << "] = ("
                << (int) posFromSecond[i].first << ", " << (int) posFromSecond[i].second << ")";
        }
#endif

        //Create a temporary table
        nbuffers = 0;
        tmpt = utmpt = NULL;
        tmptseg = NULL;
        enlargeBuffers(3);

    }

void FinalRuleProcessor::enlargeBuffers(const int newsize) {
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

void FinalRuleProcessor::processResults(const int blockid, const Term_t *first,
        FCInternalTableItr *second, const bool unique) {
    copyRawRow(first, second);
    processResults(blockid, unique, NULL);
}

void FinalRuleProcessor::processResults(const int blockid, FCInternalTableItr *first,
        FCInternalTableItr* second, const bool unique) {
    for (uint32_t i = 0; i < nCopyFromFirst; ++i) {
        row[posFromFirst[i].first] = first->getCurrentValue(posFromFirst[i].second);
    }
    for (uint32_t i = 0; i < nCopyFromSecond; ++i) {
        row[posFromSecond[i].first] = second->getCurrentValue(posFromSecond[i].second);
    }

    processResults(blockid, unique, NULL);
}

void FinalRuleProcessor::processResults(const int blockid,
        const std::vector<const std::vector<Term_t> *> &vectors1, size_t i1,
        const std::vector<const std::vector<Term_t> *> &vectors2, size_t i2,
        const bool unique) {
    for (int i = 0; i < nCopyFromFirst; i++) {
        row[posFromFirst[i].first] = (*vectors1[posFromFirst[i].second])[i1];
    }
    for (int i = 0; i < nCopyFromSecond; i++) {
        row[posFromSecond[i].first] = (*vectors2[posFromSecond[i].second])[i2];
    }
    processResults(blockid, unique, NULL);
}

void FinalRuleProcessor::processResultsAtPos(const int blockid, const uint8_t pos,
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

bool FinalRuleProcessor::isBlockEmpty(const int blockId, const bool unique) const {
    if (!unique) {
        return tmpt[blockId] == NULL || tmpt[blockId]->isEmpty();
    } else {
        return utmpt[blockId] == NULL || utmpt[blockId]->isEmpty();
    }
}

bool FinalRuleProcessor::isEmpty() const {
    for (int i = 0; i < nbuffers; ++i) {
        if (utmpt[i] != NULL && !utmpt[i]->isEmpty())
            return false;
        if (tmpt[i] != NULL && !tmpt[i]->isEmpty())
            return false;
    }
    return true;
}

uint32_t FinalRuleProcessor::getRowsInBlock(const int blockId,
        const bool unique) const {
    if (!unique) {
        return tmpt[blockId] == NULL ? 0 : tmpt[blockId]->getNRows();
    } else {
        return utmpt[blockId] == NULL ? 0 : utmpt[blockId]->getNRows();
    }

}

void FinalRuleProcessor::addColumns(const int blockid,
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
#if DEBUG
    checkSizes();
#endif
}

void FinalRuleProcessor::addColumns(const int blockid,
        FCInternalTableItr *itr, const bool unique,
        const bool sorted, const bool lastInsert) {
    enlargeBuffers(blockid + 1);
    if (unique) { //I am sure that the results are unique...
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
        //There might be duplicates...

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
#if DEBUG
    checkSizes();
#endif
}

void FinalRuleProcessor::addColumn(const int blockid,
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

#if USE_DUPLICATE_DETECTION
void FinalRuleProcessor::processResults(const int blockid, const bool unique, std::mutex *m) {
    enlargeBuffers(blockid + 1);

    if (!unique) {
        if (tmpt[blockid] == NULL) {
            tmpt[blockid] = new SegmentInserter(rowsize);
        }

        rowCount++;
        if (rowsHash != NULL) {
            std::vector<Term_t> hashRow(rowsize);

            for (uint8_t i = 0; i < rowsize; i++) {
                hashRow[i] = row[i];
            }
            if (! rowsHash->insert(hashRow).second) {
                return;
            }
            if (rowCount > (64 * 1000 * 1000) && ((float) rowsHash->size()) / rowCount > .9) {
                // Apparently, the hash is not effective.
                delete rowsHash;
                rowsHash = NULL;
            }
#if DEBUG
            else if (rowsHash->size() % 100000 == 0) {
                LOG(DEBUGL) << "rowCount = " << rowCount << ", hash size = " << rowsHash->size();
            }
#endif
        }

        tmpt[blockid]->addRow(row);

        if (rowsHash == NULL && rowCount == TMPT_THRESHOLD) {
            LOG(DEBUGL)
                << "Threshold reached in FinaltableJoinProcessor; creating rowsHash; rowCount = "
                << rowCount << ", rowsize = " << (int) rowsize;
            rowsHash = new HashSet(2 * TMPT_THRESHOLD);
            std::vector<Term_t> null;
            for (int j = 0; j < rowsize; j++) {
                null.push_back((Term_t) - 1);
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
                    LOG(DEBUGL) << "cnt = " << cnt << ", hash size = " << rowsHash->size();
                }
            }
            itr->clear();
            delete tmpt[blockid];
            tmpt[blockid] = new SegmentInserter(rowsize);
            for (HashSet::iterator itr = rowsHash->begin(); itr != rowsHash->end(); itr++) {
                for (uint8_t i = 0; i < rowsize; i++) {
                    row[i] = (*itr)[i];
                }
                tmpt[blockid]->addRow(row);
            }
        }
    } else {
        if (utmpt[blockid] == NULL) {
            utmpt[blockid] = new SegmentInserter(rowsize);
        }
        utmpt[blockid]->addRow(row);
    }
}

#else
void FinalRuleProcessor::mergeTmpt(const int blockid, const bool unique, std::mutex *m) {
    if (tmptseg[blockid] != NULL && tmptseg[blockid]->getNRows() > tmpt[blockid]->getNRows()) {
        // Only start sorting and merging if it is large enough.
        return;
    }
    LOG(DEBUGL) << "tmpt size is " << tmpt[blockid]->getNRows() << ". SortAndUnique ...";
    SegmentInserter *toSort = tmpt[blockid];
    tmpt[blockid] = new SegmentInserter(rowsize);
    std::shared_ptr<const Segment> seg;
    if (m != NULL) {
        m->unlock();
        seg = toSort->getSortedAndUniqueSegment();
        m->lock();
    } else {
        seg = toSort->getSortedAndUniqueSegment(nthreads);
    }
    LOG(DEBUGL) << "resulting segment has size " << seg->getNRows();
    if (tmptseg[blockid] == NULL) {
        tmptseg[blockid] = seg;
    } else {
        LOG(DEBUGL) << "Merging with tmptseg, size = " << tmptseg[blockid]->getNRows();
        std::vector<std::shared_ptr<const Segment>> segmentsToMerge;
        segmentsToMerge.push_back(seg);
        segmentsToMerge.push_back(tmptseg[blockid]);
        tmptseg[blockid] = SegmentInserter::merge(segmentsToMerge);
    }
    delete toSort;
}
#endif

void FinalRuleProcessor::copyRawRow(const Term_t *first,
        const Term_t* second) {
    for (uint32_t i = 0; i < nCopyFromFirst; ++i) {
        row[posFromFirst[i].first] = first[posFromFirst[i].second];
    }
    for (uint32_t i = 0; i < nCopyFromSecond; ++i) {
        row[posFromSecond[i].first] = second[posFromSecond[i].second];
    }
}

void FinalRuleProcessor::copyRawRow(const Term_t *first,
        FCInternalTableItr* second) {
    for (uint32_t i = 0; i < nCopyFromFirst; ++i) {
        row[posFromFirst[i].first] = first[posFromFirst[i].second];
    }
    for (uint32_t i = 0; i < nCopyFromSecond; ++i) {
        row[posFromSecond[i].first] = second->getCurrentValue(posFromSecond[i].second);
    }
}

void FinalRuleProcessor::consolidateSegment(std::shared_ptr<const Segment> seg) {
    std::shared_ptr<const FCInternalTable> ptrTable(
            new InmemoryFCInternalTable(rowsize,
                iteration,
                true,
                seg));
    t->add(ptrTable, literal, ruleDetails, ruleExecOrder,
            iteration, true, nthreads);
}

void FinalRuleProcessor::consolidate(const bool isFinished,
        const bool forceCheck) {
    if (!addToEndTable) {
        //do nothing. We'll consolidate later on.
        return;
    }

    if (utmpt != NULL) {
        for (int i = 0; i < nbuffers; ++i) {
            if (utmpt[i] != NULL && !utmpt[i]->isEmpty()) {
                if (forceCheck) {
                    //Even though this data should be original, I still check to make sure there are no duplicates
                    std::shared_ptr<const Segment> seg;
                    if (!utmpt[i]->isSorted()) {
                        seg = utmpt[i]->getSegment()->sortBy(NULL);
                    } else {
                        seg = utmpt[i]->getSegment();
                    }
                    seg = t->retainFrom(seg, false, nthreads);
                    if (!seg->isEmpty()) {
                        std::shared_ptr<const FCInternalTable> ptrTable(
                                new InmemoryFCInternalTable(rowsize,
                                    iteration,
                                    true,
                                    seg));
                        t->add(ptrTable, literal, ruleDetails, ruleExecOrder,
                                iteration, isFinished, nthreads);
#if 0
                        char buffer[16384];
                        FCInternalTableItr *test = ptrTable->getIterator();
                        int ncols = test->getNColumns();
                        while (test->hasNext()) {
                            test->next();
                            std::string s = "";
                            for (int i = 0; i < ncols; i++) {
                                Term_t t = test->getCurrentValue(i);
                                if (i > 0) {
                                    s += ", ";
                                }
                                s += std::to_string(t);
                            }
                            LOG(DEBUGL) << "Tuple: <" << s << ">";
                        }
                        ptrTable->releaseIterator(test);
#endif
                    }

                } else {
                    if (!utmpt[i]->isSorted()) {
                        std::shared_ptr<const Segment> sortedSegment;
                        if (nthreads == -1)
                            sortedSegment = utmpt[i]->getSegment()->sortBy(NULL);
                        else
                            sortedSegment = utmpt[i]->getSegment()->sortBy(NULL, nthreads, false);

                        std::shared_ptr<const FCInternalTable> ptrTable(new InmemoryFCInternalTable(rowsize, iteration, true, sortedSegment));
#if 0
                        char buffer[16384];
                        FCInternalTableItr *test = ptrTable->getIterator();
                        int ncols = test->getNColumns();
                        while (test->hasNext()) {
                            test->next();
                            std::string s = "";
                            for (int i = 0; i < ncols; i++) {
                                Term_t t = test->getCurrentValue(i);
                                if (i > 0) {
                                    s += ", ";
                                }
                                s += std::to_string(t);
                            }
                            LOG(DEBUGL) << "Tuple: <" << s << ">";
                        }
                        ptrTable->releaseIterator(test);
#endif
                        t->add(ptrTable, literal, ruleDetails, ruleExecOrder, iteration, isFinished, nthreads);
                    } else {
                        std::shared_ptr<const FCInternalTable> ptrTable(new InmemoryFCInternalTable(rowsize, iteration, true, utmpt[i]->getSegment()));
#if 0
                        char buffer[16384];
                        FCInternalTableItr *test = ptrTable->getIterator();
                        int ncols = test->getNColumns();
                        while (test->hasNext()) {
                            test->next();
                            std::string s = "";
                            for (int i = 0; i < ncols; i++) {
                                Term_t t = test->getCurrentValue(i);
                                if (i > 0) {
                                    s += ", ";
                                }
                                s += std::to_string(t);
                            }
                            LOG(DEBUGL) << "Tuple: <" << s << ">";
                        }
                        ptrTable->releaseIterator(test);
#endif
                        t->add(ptrTable, literal, ruleDetails, ruleExecOrder, iteration, isFinished, nthreads);
                    }
                }

                delete utmpt[i];
                utmpt[i] = new SegmentInserter(rowsize);
            }
        }
    }

    if (tmpt != NULL) {
        for (int i = 0; i < nbuffers; ++i) {
            if (tmpt[i] != NULL && !tmpt[i]->isEmpty()) {
                std::shared_ptr<const Segment> seg;
                LOG(DEBUGL) << "getSortedAndUnique ...";
                seg = tmpt[i]->getSortedAndUniqueSegment(nthreads);
                LOG(DEBUGL) << "getSortedAndUnique done";
                if (tmptseg[i] != NULL) {
                    std::vector<std::shared_ptr<const Segment>> segmentsToMerge;
                    segmentsToMerge.push_back(seg);
                    segmentsToMerge.push_back(tmptseg[i]);
                    seg = SegmentInserter::merge(segmentsToMerge);
                }

                //Remove all data already existing
                seg = t->retainFrom(seg, false, nthreads);

                if (!seg->isEmpty()) {
                    std::shared_ptr<const FCInternalTable> ptrTable(
                            new InmemoryFCInternalTable(rowsize,
                                iteration,
                                true,
                                seg));


#if 0
                    char buffer[16384];
                    FCInternalTableItr *test = ptrTable->getIterator();
                    int ncols = test->getNColumns();
                    while (test->hasNext()) {
                        test->next();
                        std::string s = "";
                        for (int i = 0; i < ncols; i++) {
                            Term_t t = test->getCurrentValue(i);
                            if (i > 0) {
                                s += ", ";
                            }
                            s += std::to_string(t);
                        }
                        LOG(DEBUGL) << "Tuple: <" << s << ">";
                    }
                    ptrTable->releaseIterator(test);
#endif
                    t->add(ptrTable, literal, ruleDetails, ruleExecOrder,
                            iteration, isFinished, nthreads);

                }

                delete tmpt[i];
                tmpt[i] = new SegmentInserter(rowsize);
            }
        }
    }

    if (!t->isEmpty(iteration))
        newDerivation = true;

#ifdef WEBINTERFACE
    //Add the block just created to the global list of derivations
    if (!t->isEmpty()) {
        //This method is not existing...
        //FCBlock &block = t->getLastBlock_nolock();
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
#endif
}

std::vector<std::shared_ptr<const Segment>> FinalRuleProcessor::getAllSegments() {
    std::vector<std::shared_ptr<const Segment>> out;
    if (tmpt != NULL) {
        for (int i = 0; i < nbuffers; ++i) {
            if (tmpt[i] != NULL && !tmpt[i]->isEmpty())
                out.push_back(tmpt[i]->getSortedAndUniqueSegment(nthreads));
        }
    }
    //std::chrono::system_clock::time_point startS = std::chrono::system_clock::now();
    if (utmpt != NULL) {
        for (int i = 0; i < nbuffers; ++i) {
            if (utmpt[i] != NULL && !utmpt[i]->isEmpty())
                out.push_back(utmpt[i]->getSegment()->sortBy(NULL, nthreads, false));
        }
    }
    if (tmptseg != NULL) {
        for (int i = 0; i < nbuffers; ++i) {
            if (tmptseg[i] != NULL && !tmptseg[i]->isEmpty())
                out.push_back(tmptseg[i]->sortBy(NULL, nthreads, false));
        }
    }
    //std::chrono::duration<double> sec = std::chrono::system_clock::now() - startS;
    //LOG(WARNL) << "---Time other " << sec.count() * 1000;
    return out;
}

FinalRuleProcessor::~FinalRuleProcessor() {
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
