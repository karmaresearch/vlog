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

void ResultJoinProcessor::copyRawRow(const Term_t *first,
        const Term_t* second) {
    for (uint32_t i = 0; i < nCopyFromFirst; ++i) {
        row[posFromFirst[i].first] = first[posFromFirst[i].second];
    }
    for (uint32_t i = 0; i < nCopyFromSecond; ++i) {
        row[posFromSecond[i].first] = second[posFromSecond[i].second];
    }
}

void ResultJoinProcessor::copyRawRow(const Term_t *first,
        FCInternalTableItr* second) {
    for (uint32_t i = 0; i < nCopyFromFirst; ++i) {
        row[posFromFirst[i].first] = first[posFromFirst[i].second];
    }
    for (uint32_t i = 0; i < nCopyFromSecond; ++i) {
        row[posFromSecond[i].first] = second->getCurrentValue(posFromSecond[i].second);
    }
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

/*uint32_t InterTableJoinProcessor::getRowsInBlock(const int blockId, const bool unique) const {
    return segments[blockId] == NULL ? 0 : segments[blockId]->getNRows();
}*/

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
