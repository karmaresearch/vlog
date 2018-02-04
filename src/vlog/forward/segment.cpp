#include <vlog/segment.h>
#include <vlog/segment_support.h>
#include <vlog/support.h>
#include <vlog/fcinttable.h>

//#include <tbb/parallel_for.h>

#include <random>
#include <memory>

Segment::Segment(const uint8_t nfields) : nfields(nfields) {
    columns = new std::shared_ptr<Column>[nfields];
    memset(columns, 0, sizeof(Column*)*nfields);
}

Segment::Segment(const uint8_t nfields, std::shared_ptr<Column> *c) : Segment(nfields) {
    for (uint8_t i = 0; i < nfields; ++i)
        this->columns[i] = c[i];
#if DEBUG
    checkSizes();
#endif
}

Segment::Segment(const uint8_t nfields, std::vector<std::shared_ptr<Column>> &c) : Segment(nfields) {
    assert(c.size() == nfields);
    for (uint8_t i = 0; i < nfields; ++i)
        this->columns[i] = c[i];
#if DEBUG
    checkSizes();
#endif
}

#if DEBUG
void Segment::checkSizes() const {
    if (nfields > 0) {
        size_t sz = 0;
        if (columns[0] != NULL) {
            sz = columns[0]->size();
        }
        for (uint8_t i = 1; i < nfields; ++i) {
            if (columns[i] == NULL) {
                assert(sz == 0);
            } else {
                assert(sz == columns[i]->size());
            }
        }
    }

}
#endif

Segment& Segment::operator =(const std::shared_ptr<Column> *v) {
    for (uint8_t i = 0; i < nfields; ++i) {
        columns[i] = v[i];
    }
    return *this;
}

size_t Segment::estimate(const uint8_t nconstantsToFilter,
        const uint8_t *posConstantsToFilter,
        const Term_t *valuesConstantsToFilter,
        const uint8_t nfields) const {

    if (nconstantsToFilter == 0) {
        return columns[0]->estimateSize();
    }

    size_t estimate = 0;
    std::vector<std::shared_ptr<ColumnReader>> readers;
    for (int i = 0; i < nconstantsToFilter; ++i) {
        readers.push_back(columns[posConstantsToFilter[i]]->getReader());
    }

    //if (nrows < 1000) {
    //linear scan

    size_t processedRows = 0;
    while (processedRows < 100) {
        bool cond = true;
        for (int i = 0; i < nconstantsToFilter && cond; ++i) {
            // cond = cond && readers[i]->hasNext();
            cond = readers[i]->hasNext();
        }
        if (!cond)
            break;

        bool ok = true;
        for (uint8_t j = 0; j < nconstantsToFilter; ++j) {
            if (readers[j]->next() != valuesConstantsToFilter[j]) {
                ok = false;
                break;
            }
        }
        if (ok)
            estimate++;
        processedRows++;
    }
    for (int i = 0; i < nconstantsToFilter; i++) {
        readers[i]->clear();
    }

    if (processedRows < 100) {
        return estimate;
    } else {
        size_t minSize = 1;
        size_t completeSize = columns[0]->estimateSize();
        size_t ratio = std::max(minSize, estimate /
                processedRows * completeSize);
        return std::min(completeSize, ratio);
    }

    /*} else {
    //sample 10% up to 1000 elements
    std::default_random_engine generator;
    std::uniform_int_distribution<size_t> distribution(0, nrows - 1);
    for (int i = 0; i < 1000; ++i) {
    size_t idx = distribution(generator) * nfields;
    bool ok = true;
    for (uint8_t j = 0; j < nconstantsToFilter; ++j) {
    if (readers[j]->get(idx) != valuesConstantsToFilter[j]) {
    ok = false;
    break;
    }
    }
    if (ok)
    estimate++;
    }
    estimate = ((double) estimate * nrows) / 1000;
    }*/
    return estimate;
    //return std::min(nrows, estimate);
}

bool Segment::areAllColumnsPartOftheSameQuery(EDBLayer **edb, const Literal **l,
        std::vector<uint8_t> *outputpos) const {
    bool sameLiteral = true;
    const Literal *lit;
    EDBLayer *layer;
    std::vector<uint8_t> posInLiteral;
    for (int i = 0; i < nfields; ++i) {
        if (columns[i]->isEDB()) {
            EDBColumn *edbC = (EDBColumn*) columns[i].get();
            if (i == 0) {
                lit = &(edbC->getLiteral());
                posInLiteral.push_back(edbC->posColumnInLiteral());
                layer = &(edbC->getEDBLayer());
            } else {
                //Check the literal is the same
                const Literal *newlit = &(edbC->getLiteral());
                Substitution subs[SIZETUPLE];
                if (Literal::subsumes(subs, *newlit, *lit) != -1
                        && Literal::subsumes(subs, *lit, *newlit) != -1) {
                    posInLiteral.push_back(edbC->posColumnInLiteral());
                } else {
                    sameLiteral = false;
                    break;
                }
            }
        } else {
            sameLiteral = false;
            break;
        }
    }

    if (sameLiteral && lit->getNVars() <= posInLiteral.size()) {
        //I check also that every posInLiteral is unique because the
        //procedure below does not work for repeated fields
        std::vector<uint8_t> test = posInLiteral;
        std::sort(test.begin(), test.end());
        std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
        auto newlim = std::unique(test.begin(), test.end());
        std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
        LOG(TRACEL) << "Time std::unique = " << sec.count() * 1000;
        // assert(newlim == test.end());
        if (newlim != test.end()) {
            return false;
        }

        if (edb != NULL) {
            *edb = layer;
            *l = lit;
            *outputpos = posInLiteral;
        }
        return true;
    }
    return false;
}

std::shared_ptr<Segment> Segment::sortBy(const std::vector<uint8_t> *fields) const {
    return sortBy(fields, 1, false);
}

std::shared_ptr<Segment> Segment::sortBy(const std::vector<uint8_t> *fields,
        const int nthreads,
        const bool filterDupls) const {
    //Special case: the fields are all EDBs and part of the same literal
    if (fields == NULL && nfields > 1) {
        //Check they are all EDB and part of the same literal
        EDBLayer *layer;
        const Literal *lit;
        std::vector<uint8_t> posInLiteral;
        bool sameLiteral = areAllColumnsPartOftheSameQuery(&layer, &lit, &posInLiteral);

        if (sameLiteral) {
            //I can sort the segment by re-creating the columns according
            //the same ordering.

            std::vector<std::shared_ptr<Column>> columns;
            columns.resize(nfields);
            std::vector<uint8_t> posPresorting;
            for (int i = 0; i < nfields; ++i) {
                //The flag unique is set to false because we have multiple fields
                //and therefore the field itself cannot be unique
                columns[i] = std::shared_ptr<Column>(
                        new EDBColumn(*layer, *lit, posInLiteral[i],
                            posPresorting,
                            false));
                posPresorting.push_back(posInLiteral[i]);
            }

            std::shared_ptr<Segment> newSeg(new Segment(nfields,
                        columns));
            return newSeg;
        }
    } //End special case
    if (nthreads == 1) {
        assert(filterDupls == false);
        return intsort(fields);
    } else {
        return intsort(fields, nthreads, filterDupls);
    }
}

std::shared_ptr<Segment> Segment::intsort(
        const std::vector<uint8_t> *fields) const {
    if (!isEmpty()) {
        //Get all fields that are not constant
        std::vector<std::shared_ptr<Column>> varColumns;
        std::vector<std::unique_ptr<ColumnReader>> varColumnsR;
        std::vector<uint8_t> idxVarColumns;
        for (uint8_t i = 0; i < nfields; ++i) {
            if (!columns[i]->isConstant()) {
                varColumns.push_back(columns[i]);
                varColumnsR.push_back(columns[i]->getReader());
                idxVarColumns.push_back(i);
            }
        }
        std::vector<std::shared_ptr<Column>> sortedColumns;

        if (varColumns.size() == 0) {
            return std::shared_ptr<Segment>(new Segment(nfields, columns));
        } else if (varColumns.size() == 1) {
            sortedColumns.push_back(varColumns[0]->sort());
        } else {
            if (fields != NULL) {
                //Rearrange the fields
                std::vector<std::shared_ptr<Column>> newVarColumns;
                std::vector<uint8_t> newIdxVarColumns;
                for (uint8_t j = 0; j < fields->size(); ++j) {
                    for (uint8_t m = 0; m < idxVarColumns.size(); ++m) {
                        if (idxVarColumns[m] == fields->at(j)) {
                            newVarColumns.push_back(varColumns[m]);
                            newIdxVarColumns.push_back(idxVarColumns[m]);
                        }
                    }
                }

                for (uint8_t m = 0; m < idxVarColumns.size(); ++m) {
                    bool found = false;
                    for (uint8_t n = 0; n < newIdxVarColumns.size() && !found; ++n) {
                        if (newIdxVarColumns[n] == idxVarColumns[m]) {
                            found = true;
                        }
                    }
                    if (!found) {
                        newVarColumns.push_back(varColumns[m]);
                        newIdxVarColumns.push_back(idxVarColumns[m]);
                    }
                }
                assert(varColumns.size() == newVarColumns.size());
                varColumns = newVarColumns;
                idxVarColumns = newIdxVarColumns;
            }

            if (varColumns.size() == 2) {
                //Populate the array
                std::vector<const std::vector<Term_t> *> vectors = getAllVectors(varColumns);
                std::vector<std::pair<Term_t, Term_t>> values;
                size_t sz1 = vectors[0]->size();
                size_t sz2 = vectors[1]->size();
                const size_t allRows = sz1 < sz2 ? sz1 : sz2;
                values.reserve(allRows);
                for (size_t i = 0; i < allRows; ++i) {
                    const Term_t v1 = (*vectors[0])[i];
                    const Term_t v2 = (*vectors[1])[i];
                    values.push_back(make_pair(v1, v2));
                }
                deleteAllVectors(varColumns, vectors);
                std::sort(values.begin(), values.end());
                ColumnWriter sortedColumnsInserters[2];
                for (const auto v : values) {
                    sortedColumnsInserters[0].add(v.first);
                    sortedColumnsInserters[1].add(v.second);
                }
                sortedColumns.push_back(sortedColumnsInserters[0].getColumn());
                sortedColumns.push_back(sortedColumnsInserters[1].getColumn());
            } else {
                //Sort function
                std::vector<const std::vector<Term_t> *> vectors = getAllVectors(varColumns);
                SegmentSorter sorter(vectors);

                const size_t allRows = vectors[0]->size();
                std::vector<size_t> rows;
                rows.reserve(allRows);
                for (size_t i = 0; i < allRows; ++i) {
                    rows.push_back(i);
                }

                std::sort(rows.begin(), rows.end(), std::ref(sorter));
                // LOG(TRACEL) << "Sort done.";

                //Reconstruct the fields
                const uint8_t nvarColumns = (uint8_t) varColumns.size();
                std::vector<ColumnWriter> sortedColumnsInserters;
                sortedColumnsInserters.resize(nvarColumns);

                for (std::vector<size_t>::iterator itr = rows.begin();
                        itr != rows.end(); ++itr) {
                    for (uint8_t i = 0; i < nvarColumns; ++i) {
                        sortedColumnsInserters[i].add((*vectors[i])[*itr]);
                    }
                }
                //Populate sortedColumns
                assert(sortedColumns.size() == 0);
                for (auto &writer : sortedColumnsInserters)
                    sortedColumns.push_back(writer.getColumn());
                deleteAllVectors(varColumns, vectors);
            }
        }

        //Reconstruct all the fields
        std::vector<std::shared_ptr<Column>> allSortedColumns;
        for (uint8_t i = 0; i < nfields; ++i) {
            bool isVar = false;
            for (uint8_t j = 0; j < varColumns.size() && !isVar; ++j) {
                if (idxVarColumns[j] == i) {
                    allSortedColumns.push_back(sortedColumns[j]);
                    isVar = true;
                }
            }
            if (!isVar) {
                allSortedColumns.push_back(columns[i]);
            }
        }
        return std::shared_ptr<Segment>(new Segment(nfields, allSortedColumns));
    } else {
        return std::shared_ptr<Segment>(new Segment(nfields));
    }
}

std::shared_ptr<Segment> Segment::intsort(
        const std::vector<uint8_t> *fields,
        const int nthreads,
        const bool filterDupl) const {
    assert(nthreads > 1);

    if (!isEmpty()) {
        //std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
        //Get all fields that are not constant
        std::vector<std::shared_ptr<Column>> varColumns;
        std::vector<std::unique_ptr<ColumnReader>> varColumnsR;
        std::vector<uint8_t> idxVarColumns;
        for (uint8_t i = 0; i < nfields; ++i) {
            if (!columns[i]->isConstant()) {
                varColumns.push_back(columns[i]);
                varColumnsR.push_back(columns[i]->getReader());
                idxVarColumns.push_back(i);
            }
        }
        std::vector<std::shared_ptr<Column>> sortedColumns;
        //std::chrono::duration<double> sec1 = std::chrono::system_clock::now() - start;
        //LOG(WARNL) << "---- get readers =" << sec1.count() * 1000;

        if (varColumns.size() == 0) {
            if (filterDupl) {
                std::vector<std::shared_ptr<Column>> newColumns;
                for(int i = 0; i < nfields; ++i) {
                    newColumns.push_back(columns[i]->unique());
                }
                return std::shared_ptr<Segment>(new Segment(nfields, newColumns));
            } else {
                return std::shared_ptr<Segment>(new Segment(nfields, columns));
            }
        } else if (varColumns.size() == 1) {
            //std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
            if (filterDupl) {
                sortedColumns.push_back(varColumns[0]->sort_and_unique(nthreads));
            } else {
                sortedColumns.push_back(varColumns[0]->sort(nthreads));
            }
            //std::chrono::duration<double> sec1 = std::chrono::system_clock::now() - start;
            //LOG(WARNL) << "----sort one column=" << sec1.count() * 1000;
        } else {
            if (fields != NULL) {
                //Rearrange the fields
                std::vector<std::shared_ptr<Column>> newVarColumns;
                std::vector<uint8_t> newIdxVarColumns;
                for (uint8_t j = 0; j < fields->size(); ++j) {
                    for (uint8_t m = 0; m < idxVarColumns.size(); ++m) {
                        if (idxVarColumns[m] == fields->at(j)) {
                            newVarColumns.push_back(varColumns[m]);
                            newIdxVarColumns.push_back(idxVarColumns[m]);
                        }
                    }
                }

                for (uint8_t m = 0; m < idxVarColumns.size(); ++m) {
                    bool found = false;
                    for (uint8_t n = 0; n < newIdxVarColumns.size() && !found; ++n) {
                        if (newIdxVarColumns[n] == idxVarColumns[m]) {
                            found = true;
                        }
                    }
                    if (!found) {
                        newVarColumns.push_back(varColumns[m]);
                        newIdxVarColumns.push_back(idxVarColumns[m]);
                    }
                }
                assert(varColumns.size() == newVarColumns.size());
                varColumns = newVarColumns;
                idxVarColumns = newIdxVarColumns;
            }

            std::vector<const std::vector<Term_t> *> vectors = getAllVectors(varColumns, nthreads);

            size_t sz = varColumns[0]->size();
            std::vector<size_t> idxs;
            idxs.reserve(sz);

            size_t chunks = (sz + nthreads - 1) / nthreads;

            /*
               if (idxs.size() >= 1000000) {
               tbb::parallel_for(tbb::blocked_range<size_t>(0, idxs.size(), chunks),
               InitArray(idxs));
               } else
               */
            {
                for (size_t i = 0; i < sz; i++) {
                    idxs.push_back(i);
                }
            }

            if (varColumns.size() == 2) {
                //Populate array
                //std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
                const Term_t *rawv1 = &(*(vectors[0]))[0];
                const Term_t *rawv2 = &(*(vectors[1]))[0];

                //std::chrono::duration<double> sec1 = std::chrono::system_clock::now() - start;
                //LOG(WARNL) << "---- populate pairs vector =" << sec1.count() * 1000 << " " << varColumns[0]->size();

                //Sort
                //start = std::chrono::system_clock::now();
                PairComparator pc(rawv1, rawv2);

                if (idxs.size() > 1000) {
                    ParallelTasks::sort_int(idxs.begin(), idxs.end(), pc, nthreads);
                } else {
                    std::sort(idxs.begin(), idxs.end(), pc);
                }
                //sec1 = std::chrono::system_clock::now() - start;
                //LOG(WARNL) << "---- parallel sort =" << sec1.count() * 1000 << " " << nthreads;

                //Copy back sorted columns
                //start = std::chrono::system_clock::now();
                if (!filterDupl) {
                    std::vector<Term_t> out1;
                    std::vector<Term_t> out2;
                    // Not sure if it makes sense to do this in parallel at all
                    if (chunks < 10000) {
                        // Sequential version
                        out1.reserve(sz);
                        out2.reserve(sz);
                        for (size_t i = 0; i < sz; i++) {
                            out1.push_back(rawv1[idxs[i]]);
                            out2.push_back( rawv2[idxs[i]]);
                        }
                    } else {
                        // Parallel version
                        out1.resize(sz);
                        out2.resize(sz);
                        //tbb::parallel_for(tbb::blocked_range<size_t>(0, idxs.size(), chunks),
                        //        CreateColumns2(idxs, rawv1, rawv2, out1, out2));
                        ParallelTasks::parallel_for(0, idxs.size(), chunks,
                                CreateColumns2(idxs, rawv1, rawv2, out1, out2));
                    }
                    sortedColumns.push_back(ColumnWriter::getColumn(out1, true));
                    sortedColumns.push_back(ColumnWriter::getColumn(out2, false));
                } else {
                    // Not sure if it makes sense to do this in parallel at all
                    if (chunks < 10000) {
                        // Sequential version
                        Term_t prev1 = (Term_t) - 1;
                        Term_t prev2 = (Term_t) - 1;
                        std::vector<Term_t> out1;
                        std::vector<Term_t> out2;
                        for (size_t i = 0; i < idxs.size(); i++) {
                            const Term_t value1 =  rawv1[idxs[i]];
                            const Term_t value2 =  rawv2[idxs[i]];
                            if (value1 != prev1 || value2 != prev2) {
                                out1.push_back(value1);
                                out2.push_back(value2);
                            }
                            prev1 = value1;
                            prev2 = value2;
                        }
                        sortedColumns.push_back(ColumnWriter::getColumn(out1, true));
                        sortedColumns.push_back(ColumnWriter::getColumn(out2, false));
                    } else {
                        // Parallel version
                        std::vector<std::pair<size_t, std::vector<Term_t>>> ranges1;
                        std::vector<std::pair<size_t, std::vector<Term_t>>> ranges2;
                        std::mutex m;
                        //tbb::parallel_for(tbb::blocked_range<size_t>(0, idxs.size(), chunks),
                        //        CreateColumnsNoDupl2(idxs, rawv1, rawv2,
                        //            ranges1, ranges2, m));
                        ParallelTasks::parallel_for(0, idxs.size(), chunks,
                                CreateColumnsNoDupl2(idxs, rawv1, rawv2,
                                    ranges1, ranges2, m));

                        size_t totalSize = 0;
                        for (size_t i = 0; i < ranges1.size(); ++i) {
                            totalSize += ranges1[i].second.size();
                        }
                        std::vector<Term_t> out1;
                        std::vector<Term_t> out2;
                        out1.reserve(totalSize);
                        out2.reserve(totalSize);

                        //Sort the ranges
                        std::vector<int> idxRanges;
                        for (int i = 0; i < ranges1.size(); ++i) {
                            idxRanges.push_back(i);
                        }
                        std::sort(idxRanges.begin(), idxRanges.end(),
                                CompareRanges(ranges1));
                        for (int i = 0; i < ranges1.size(); ++i) {
                            size_t idx = idxRanges[i];
                            std::vector<Term_t> &vals1 = ranges1[idx].second;
                            std::vector<Term_t> &vals2 = ranges2[idx].second;
                            assert(ranges1[idx].first == ranges2[idx].first);
                            assert(vals1.size() == vals2.size());
                            for (size_t idxtocopy = 0; idxtocopy < vals1.size();
                                    idxtocopy++) {
                                out1.push_back(vals1[idxtocopy]);
                                out2.push_back(vals2[idxtocopy]);
                            }
                        }
                        sortedColumns.push_back(ColumnWriter::getColumn(out1, true));
                        sortedColumns.push_back(ColumnWriter::getColumn(out2, false));
                    }
                }
                //sec1 = std::chrono::system_clock::now() - start;
                //LOG(WARNL) << "---- copy back =" << sec1.count() * 1000 << " " << nthreads;
            } else {
                //Sort function
                SegmentSorter sorter(vectors);

                if (nthreads > 1 && idxs.size() > 1000) {
                    ParallelTasks::sort_int(idxs.begin(), idxs.end(), std::ref(sorter), nthreads);
                } else {
                    std::sort(idxs.begin(), idxs.end(), std::ref(sorter));
                }
                // LOG(TRACEL) << "Sort done.";
                //
                if (! filterDupl) {
                    std::vector<std::vector<Term_t>> out(varColumns.size());
                    // Not sure if it makes sense to do this in parallel at all
                    if (nthreads <= 1 || chunks < 10000) {
                        // Sequential version
                        for (size_t i = 0; i < idxs.size(); i++) {
                            for (int j = 0; j < out.size(); j++) {
                                out[j].push_back((*vectors[j])[idxs[i]]);
                            }
                        }
                    } else {
                        // Parallel version
                        for (int i = 0; i < out.size(); i++) {
                            out[i].resize(idxs.size());
                        }
                        //tbb::parallel_for(tbb::blocked_range<size_t>(0, idxs.size(), chunks),
                        //        CreateColumns(idxs, vectors, out));
                        ParallelTasks::parallel_for(0, idxs.size(), chunks,
                                CreateColumns(idxs, vectors, out));
                    }
                    sortedColumns.push_back(ColumnWriter::getColumn(out[0], true));
                    for (int i = 1; i < out.size(); i++) {
                        sortedColumns.push_back(ColumnWriter::getColumn(out[i], false));
                    }
                } else {
                    // TODO!
                    throw 10;
                }
            }
            deleteAllVectors(varColumns, vectors);
        }

        //Reconstruct all the fields
        std::vector<std::shared_ptr<Column>> allSortedColumns;

        assert(varColumns.size() > 0);
        size_t newsize = varColumns[0]->size();

        for (uint8_t i = 0; i < nfields; ++i) {
            bool isVar = false;
            for (uint8_t j = 0; j < varColumns.size() && !isVar; ++j) {
                if (idxVarColumns[j] == i) {
                    allSortedColumns.push_back(sortedColumns[j]);
                    isVar = true;
                }
            }
            if (!isVar) {
                //Size must be adapted to the one of var columns in case dupl is true
                if (filterDupl && columns[i]->size() != newsize) {
                    Term_t v = columns[i]->getValue(0);
                    allSortedColumns.push_back(std::shared_ptr<Column>(
                                new CompressedColumn(v, newsize)));
                } else {
                    allSortedColumns.push_back(columns[i]);
                }
            }
        }
        return std::shared_ptr<Segment>(new Segment(nfields, allSortedColumns));
    } else {
        return std::shared_ptr<Segment>(new Segment(nfields));
    }
}

#if DEBUG
void SegmentInserter::checkSizes() const {
    if (nfields > 0) {
        size_t sz = columns[0].size();
        for (uint8_t i = 1; i < nfields; ++i) {
            assert(sz == columns[i].size());
        }
    }
}
#endif

void SegmentInserter::addRow(const Term_t *row, const uint8_t *posToCopy) {
    if (segmentSorted) {
        assert(nfields > 0);
        if (!columns[0].isEmpty()) {
            for (uint8_t i = 0; i < nfields; ++i) {
                if (row[posToCopy[i]] < columns[posToCopy[i]].lastValue()) {
                    segmentSorted = false;
                    break;
                } else if (row[posToCopy[i]] > columns[posToCopy[i]].lastValue()) {
                    break;
                }
            }
        }
    }

    for (int i = 0; i < nfields; ++i) {
        columns[i].add(row[posToCopy[i]]);
    }
}

/*void SegmentInserter::addRow(const Segment *seg, const uint32_t rowid) {
  if (segmentSorted) {
  assert(nfields > 0);
  if (!columns[0].isEmpty()) {
  for (uint8_t i = 0; i < nfields; ++i) {
  if (seg->at(rowid, i) < columns[i].lastValue()) {
  segmentSorted = false;
  break;
  } else if (seg->at(rowid, i) > columns[i].lastValue()) {
  break;
  }
  }
  }
  }

  for (int i = 0; i < nfields; ++i) {
  columns[i].add(seg->at(rowid, i));
  }
  }*/

void SegmentInserter::addRow(SegmentIterator &itr) {
    if (segmentSorted) {
        assert(nfields > 0);
        if (!columns[0].isEmpty()) {
            for (uint8_t i = 0; i < nfields; ++i) {
                if (itr.get(i) < columns[i].lastValue()) {
                    segmentSorted = false;
                    break;
                } else if (itr.get(i) > columns[i].lastValue()) {
                    break;
                }
            }
        }
    }

    for (uint8_t i = 0; i < nfields; ++i) {
        columns[i].add(itr.get(i));
    }
}

void SegmentInserter::addRow(FCInternalTableItr *itr, const uint8_t *posToCopy) {
    if (segmentSorted) {
        assert(nfields > 0);
        if (!columns[0].isEmpty()) {
            for (uint8_t i = 0; i < nfields; ++i) {
                if (itr->getCurrentValue(posToCopy[i]) < columns[posToCopy[i]].lastValue()) {
                    segmentSorted = false;
                    break;
                } else if (itr->getCurrentValue(posToCopy[i]) > columns[posToCopy[i]].lastValue()) {
                    break;
                }
            }
        }
    }

    for (int i = 0; i < nfields; ++i) {
        columns[i].add(itr->getCurrentValue(posToCopy[i]));
    }
}

void SegmentInserter::addRow(const Term_t *row) {
    if (segmentSorted) {
        assert(nfields > 0);
        if (!columns[0].isEmpty()) {
            for (uint8_t i = 0; i < nfields; ++i) {
                if (row[i] < columns[i].lastValue()) {
                    segmentSorted = false;
                    break;
                } else if (row[i] > columns[i].lastValue()) {
                    break;
                }
            }
        }
    }

    for (size_t i = 0; i < nfields; ++i) {
        columns[i].add(row[i]);
    }
}

bool SegmentInserter::isEmpty() const {
    assert(nfields > 0);
    if (copyColumns[0] != std::shared_ptr<Column>()) {
        if (!copyColumns[0]->isEmpty())
            return false;
    }
    return columns[0].isEmpty();
}

void SegmentInserter::addColumns(std::vector<std::shared_ptr<Column>> &c,
        const bool sorted, const bool lastInsert) {
    assert(c.size() == nfields);
    segmentSorted = isEmpty() && sorted;
    if (nfields == 1 && isEmpty() &&
            !c[0]->containsDuplicates()) {
        duplicates = false;
    } else {
        duplicates = true;
    }

    if (/*lastInsert && */isEmpty()) {
        for (uint8_t i = 0; i < c.size(); ++i) {
            copyColumns[i] = c[i];
        }
    } else {
        for (uint8_t i = 0; i < c.size(); ++i) {
            /*if (copyColumns[i] != NULL) {
            //Concatenate first the column
            //columns[i].concatenate(copyColumns[i].get());
            //copyColumns[i] = std::shared_ptr<Column>();
            }*/

            columns[i].concatenate(c[i].get());
        }
    }
}

void SegmentInserter::addColumn(const uint8_t pos,
        std::shared_ptr<Column> column,
        const bool sorted) {
    segmentSorted = isEmpty() && sorted;
    duplicates = !(segmentSorted && nfields == 1);

    if (isEmpty() && nfields == 1) {
        copyColumns[pos] = column;
    } else {
        /*if (copyColumns[pos] != std::shared_ptr<Column>()) {
          columns[pos].concatenate(copyColumns[pos].get());
          copyColumns[pos] = std::shared_ptr<Column>();
          }*/
        columns[pos].concatenate(column.get());
    }
}

std::shared_ptr<const Segment> SegmentInserter::getSegment() {
    std::vector<std::shared_ptr<Column>> c;
    for (int i = 0; i < nfields; ++i) {
        if (copyColumns[i] != std::shared_ptr<Column>()) {
            if (!columns[i].isEmpty()) {
                columns[i].concatenate(copyColumns[i].get());
                c.push_back(columns[i].getColumn());
            } else {
                c.push_back(copyColumns[i]);
            }
        } else {
            c.push_back(columns[i].getColumn());
        }
    }
    return std::shared_ptr<const Segment>(new Segment(nfields, c));
}

std::shared_ptr<const Segment> SegmentInserter::getSegment(const int nthreads) {
    if (nthreads <= 1 || nfields <= 1) {
        return getSegment();
    }
    int count = 0;
    for (int i = 0; i < nfields; ++i) {
        if (copyColumns[i] != std::shared_ptr<Column>()) {
            if (!columns[i].isEmpty()) {
                count++;
            }
        }
    }
    if (count <= 1) {
        return getSegment();
    }

    std::vector<std::shared_ptr<Column>> c(nfields);

    //tbb::parallel_for(tbb::blocked_range<int>(0, nfields, 1),
    //        Concat(c, this));
    ParallelTasks::parallel_for(0, nfields, 1, Concat(c, this));
    return std::shared_ptr<const Segment>(new Segment(nfields, c));
}

std::shared_ptr<const Segment> SegmentInserter::getSortedAndUniqueSegment(const int nthreads) {
    if (nthreads <= 1) {
        return getSortedAndUniqueSegment();
    }

    if (segmentSorted && !duplicates) {
        return getSegment(nthreads);
    }

    //std::chrono::system_clock::time_point start = std::chrono::system_clock::now();

    //I assume nthreads > 1
    assert(nthreads > 1);
    std::shared_ptr<const Segment> seg = getSegment(nthreads);

    //std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    //LOG(WARNL) << "---Time getSegment " << sec.count() * 1000;
    //start = std::chrono::system_clock::now();

    // if (seg->getNRows() > 1) {
    /*if (!segmentSorted) {
      }*/
    seg = seg->sortBy(NULL, nthreads, duplicates);

    //sec = std::chrono::system_clock::now() - start;
    //LOG(WARNL) << "---Time sort " << sec.count() * 1000;
    //start = std::chrono::system_clock::now();

    //if (duplicates) {
    //    seg = SegmentInserter::unique(seg, nthreads);
    //}

    //sec = std::chrono::system_clock::now() - start;
    //LOG(WARNL) << "---Time unique " << sec.count() * 1000;

    // }
    return seg;
}

std::shared_ptr<const Segment> SegmentInserter::getSortedAndUniqueSegment() {
#if DEBUG
    LOG(TRACEL) << "getSortedAndUniqueSegment: segmentSorted = " << segmentSorted << ", duplicates = " << duplicates;
#endif
    if (segmentSorted && !duplicates) {
        return getSegment();
    }  else if (copyColumns[0] != NULL && !columns[0].isEmpty()) {
        //First sort the first segment
        std::vector<std::shared_ptr<Column>> c;
        for (int i = 0; i < nfields; ++i)
            c.push_back(copyColumns[i]);
        std::shared_ptr<const Segment> seg1(new Segment(nfields, c));
        if (seg1->getNRows() > 1) {
            seg1 = seg1->sortBy(NULL);
            if (duplicates) {
                seg1 = SegmentInserter::unique(seg1);
            }
        }

        //Now sort the second segment
        std::vector<std::shared_ptr<Column>> c2;
        for (int i = 0; i < nfields; ++i)
            c2.push_back(columns[i].getColumn());
        std::shared_ptr<const Segment> seg2(new Segment(nfields, c2));
        if (seg2->getNRows() > 1) {
            seg2 = seg2->sortBy(NULL);
            if (duplicates) {
                seg2 = SegmentInserter::unique(seg2);
            }
        }

        //Now merge them
        std::vector<std::shared_ptr<const Segment>> segments;
        segments.push_back(seg1);
        segments.push_back(seg2);
        std::shared_ptr<const Segment> finalSeg = SegmentInserter::merge(
                segments);
        return finalSeg;

    } else {
        std::shared_ptr<const Segment> seg = getSegment();
        if (seg->getNRows() > 1) {
            if (!segmentSorted) {
                seg = seg->sortBy(NULL);
            }
            if (duplicates) {
                seg = SegmentInserter::unique(seg);
            }
        }
        return seg;
    }
}

size_t SegmentInserter::getNRows() const {
    assert(nfields > 0);
    size_t s = 0;
    if (copyColumns[0] != std::shared_ptr<Column>()) {
        if (!copyColumns[0]->isEmpty())
            s = copyColumns[0]->size();
    }
    s += columns[0].size();
    return s;
}

void SegmentInserter::addRow(FCInternalTableItr *itr) {
    if (segmentSorted) {
        if (!isEmpty()) {
            for (uint8_t i = 0; i < nfields; ++i) {
                if (itr->getCurrentValue(i) < columns[i].lastValue()) {
                    segmentSorted = false;
                    break;
                } else if (itr->getCurrentValue(i) > columns[i].lastValue()) {
                    break;
                }
            }
        }
    }

    for (uint8_t i = 0; i < nfields; ++i) {
        columns[i].add(itr->getCurrentValue(i));
    }
}

void SegmentInserter::addAt(const uint8_t p, const Term_t v) {
    //assert(copyColumns[p] == NULL);
    segmentSorted = false;
    columns[p].add(v);
}

/*void SegmentInserter::moveFrom(std::shared_ptr<Segment> seg, const bool sorted) {
  segment = seg;
  this->segmentSorted = sorted;
  }*/

void SegmentInserter::copyArray(SegmentIterator &source) {
    for (uint8_t i = 0; i < nfields; ++i) {
        addAt(i, source.get(i));
    }
}

/*std::shared_ptr<const Segment> SegmentInserter::unique(
  std::shared_ptr<const Segment> &segment) {
  retain(segment, NULL);
  }*/

/*void SegmentInserter::sortSegment() {
  if (!isSorted()) {
  segment = segment->sortBy(NULL);
  segmentSorted = true;
  }
  }*/

std::shared_ptr<const Segment> SegmentInserter::retainMemMem(Column *c1,
        Column *c2) {
    ColumnWriter co;
    Term_t prevv1 = (Term_t) - 1;
    Term_t v1 = (Term_t) - 1;
    Term_t v2 = (Term_t) - 1;

    if (c1->isBackedByVector() && c2->isBackedByVector()) {
        const vector<Term_t> &vc1 = c1->getVectorRef();
        const vector<Term_t> &vc2 = c2->getVectorRef();
        int v1Index = 0;
        int v2Index = 0;

        v1 = vc1[v1Index++];
        v2 = vc2[v2Index++];
        if (v1 > vc2[vc2.size()-1] || vc1[vc1.size()-1] < v2) {
            co.add(v1);
            prevv1 = v1;
            for (int i = v1Index; i < vc1.size(); i++) {
                v1 = vc1[i];
                if (v1 != prevv1) {
                    co.add(v1);
                    prevv1 = v1;
                }
            }
        } else {
            while(true) {
                if (v1 <= v2) {
		    if (v1 < v2 && v1 != prevv1) {
                        co.add(v1);
                        prevv1 = v1;
                    }
                    if (v1Index < vc1.size()) {
                        v1 = vc1[v1Index++];
                    } else {
                        v1 = (Term_t) -1;
                        break;
                    }
                } else {
                    if (v2Index < vc2.size()) {
                        v2 = vc2[v2Index++];
                    } else {
                        v2 = (Term_t) -1;
                        break;
                    }
                }
            }
            if (v1 != (Term_t) -1) {
                if (v1 != prevv1) {
                    co.add(v1);
                    prevv1 = v1;
                }
                for (int i = v1Index; i < vc1.size(); i++) {
                    v1 = vc1[i];
                    if (v1 != prevv1) {
                        co.add(v1);
                        prevv1 = v1;
                    }
                }
            }
        }
    } else {

        //size_t idx1 = 0;
        //size_t idx2 = 0;
        std::unique_ptr<ColumnReader> c1R = c1->getReader();
        std::unique_ptr<ColumnReader> c2R = c2->getReader();

        if (!c1R->hasNext())
            throw 10;
        else
            v1 = c1R->next();
        if (!c2R->hasNext())
            throw 10;
        else
            v2 = c2R->next();

        while (true) {
            if (v1 <= v2) {
                if (v1 < v2 && v1 != prevv1) {
                    co.add(v1);
                    prevv1 = v1;
                }
                if (c1R->hasNext()) {
                    v1 = c1R->next();
                } else {
                    v1 = (Term_t) - 1;
                    break;
                }
            }
	    else {
                if (c2R->hasNext()) {
                    v2 = c2R->next();
                } else {
                    v2 = (Term_t) - 1;
                    break;
                }
            }
        }

        if (v1 != (Term_t) - 1) {
            if (v1 != prevv1) {
                co.add(v1);
                prevv1 = v1;
            }
            while (c1R->hasNext()) {
                v1 = c1R->next();
                if (v1 != prevv1) {
                    co.add(v1);
                    prevv1 = v1;
                }
            }
        }
    }

    std::vector<std::shared_ptr<Column>> columns;
    columns.push_back(co.getColumn());
    return std::shared_ptr<Segment>(new Segment(1, columns));
}

std::shared_ptr<const Segment> SegmentInserter::retainMemEDB(
        std::shared_ptr<const Segment> seg,
        std::shared_ptr<const FCInternalTable > existingValues,
        uint8_t nfields) {

    std::shared_ptr<const Column> edbCol = existingValues->getColumn(0);
    const Literal l = ((EDBColumn*) edbCol.get())->getLiteral();
    EDBLayer &layer = ((EDBColumn*)edbCol.get())->getEDBLayer();

    uint8_t pos1 = ((EDBColumn*)edbCol.get())->posColumnInLiteral();
    std::vector<uint8_t> posColumns;
    posColumns.push_back(pos1);

    std::vector<std::shared_ptr<Column>> valuesToCheck;
    valuesToCheck.push_back(seg->getColumn(0));

    if (nfields == 2) {
        //Get also the second column
        std::shared_ptr<const Column> edbCol2 = existingValues->getColumn(1);
        const Literal l2 = ((EDBColumn*) edbCol2.get())->getLiteral();
        uint8_t pos2 = ((EDBColumn*)edbCol2.get())->posColumnInLiteral();
        //TODO: check is the same literal
        posColumns.push_back(pos2);
        valuesToCheck.push_back(seg->getColumn(1));
    }

    //Do an anti join on the EDB layer
    std::vector<std::shared_ptr<Column>> output = layer.checkNewIn(
            valuesToCheck,
            l, posColumns);
    return std::shared_ptr<Segment>(new Segment(nfields, output));
}

std::shared_ptr<const Segment> SegmentInserter::retainEDB(
        std::shared_ptr<const Segment> seg,
        std::shared_ptr<const FCInternalTable> existingValues,
        uint8_t nfields) {
    //Column is an EDB column
    std::shared_ptr<Column> column = seg->getColumn(0);
    EDBLayer &layer = ((EDBColumn*)column.get())->getEDBLayer();
    const Literal &l1 = ((EDBColumn*)column.get())->getLiteral();
    uint8_t pos1 = ((EDBColumn*)column.get())
        ->posColumnInLiteral();
    std::vector<uint8_t> posColumns1;
    posColumns1.push_back(pos1);

    //Get literal and pos join
    std::shared_ptr<const Column> firstCol = existingValues->getColumn(0);
    const Literal l2 = ((EDBColumn*)firstCol.get())->getLiteral();
    uint8_t pos2 = ((EDBColumn*)firstCol.get())
        ->posColumnInLiteral();
    std::vector<uint8_t> posColumns2;
    posColumns2.push_back(pos2);

    assert(nfields > 0 && nfields < 3);
    if (nfields == 2) {
        std::shared_ptr<Column> column12 = seg->getColumn(1);
        const Literal &l12 = ((EDBColumn*)column12.get())->getLiteral();
        uint8_t pos12 = ((EDBColumn*)column12.get())
            ->posColumnInLiteral();
        //Chech the two literals are equivalent and that the rel. are
        //different positions. ??? What if the head is, say pred(?A,?A)? --Ceriel
        Substitution subs[SIZETUPLE];
        if (!l1.sameVarSequenceAs(l12) || l1.subsumes(subs, l1, l12) == -1
                // || pos1 == pos12	// Commented out --Ceriel
		) {
            //The columns come from different literals. This is not yet
            //supported
            throw 10;
        }
        posColumns1.push_back(pos12);

        //Repeat the same process for the second field
        std::shared_ptr<const Column> column22 = existingValues->getColumn(1);
        const Literal &l22 = ((EDBColumn*)column22.get())->getLiteral();
        uint8_t pos22 = ((EDBColumn*)column22.get())
            ->posColumnInLiteral();
        if (!l2.sameVarSequenceAs(l22) || l1.subsumes(subs, l2, l22) == -1
                // || pos2 == pos22	// Commented out --Ceriel
		) {
            //The columns come from different literals. This is not yet
            //supported
            throw 10;
        }
        posColumns2.push_back(pos22);
    }

    std::vector<std::shared_ptr<Column>> columns = layer.
        checkNewIn(l1, posColumns1, l2,
                posColumns2);
    return std::shared_ptr<Segment>(new Segment(nfields, columns));
}

std::shared_ptr<const Segment> SegmentInserter::retain(
        std::shared_ptr<const Segment> &segment,
        std::shared_ptr<const FCInternalTable> existingValues,
        const bool duplicates,
        const int nthreads) {

    if (segment->isEmpty())
        return segment;

    //Special cases: one of the two sides have one column each and are EDB views
    if ((segment->getNColumns() == 1 || segment->getNColumns() == 2)
            && existingValues != NULL && existingValues->isEDB()) {
        if (segment->isEDB()) {
            segment = retainEDB(segment, existingValues,
                    segment->getNColumns());
        } else  {
            segment = retainMemEDB(segment, existingValues,
                    segment->getNColumns());
        }
        return segment;
    }

    std::vector<uint8_t> posConstants;
    std::vector<uint8_t> posToCopy;
    std::vector<Term_t> valueConstants;
    if (!segment->isEmpty()) {
        for (uint8_t i = 0; i < segment->getNColumns(); ++i) {
            if (segment->isConstantField(i)) {
                posConstants.push_back(i);
                valueConstants.push_back(segment->firstInColumn(i));
            } else {
                posToCopy.push_back(i);
            }
        }
    }

    bool match = true;
    bool superset = false;
    if (existingValues != NULL && !existingValues->isEmpty()
            && posConstants.size() > 0) {
        //Is existingValues a superset of the current values?
        for (uint8_t i = 0; i < posConstants.size() && match; ++i) {
            if (existingValues->isColumnConstant(posConstants[i])) {
                if (valueConstants[i] !=
                        existingValues->
                        getValueConstantColumn(posConstants[i])) {
                    match = false;
                }
            } else {
                superset = true;
            }
        }
    }

    if (superset) {
        existingValues = existingValues->filter((uint8_t) posToCopy.size(),
                posToCopy.size() > 0 ?
                & (posToCopy[0]) : NULL,
                (uint8_t) posConstants.size(),
                &(posConstants[0]),
                &(valueConstants[0]),
                0, NULL, nthreads);
    } else if (existingValues != NULL &&
            posToCopy.size() < existingValues->getRowSize()) {
        existingValues = existingValues->filter((uint8_t) posToCopy.size(),
                &(posToCopy[0]), 0, NULL,
                NULL, 0, NULL, nthreads);
    }

    const size_t nPosToCompare = posToCopy.size();
    const uint8_t *posToCompare = posToCopy.size() > 0 ? &(posToCopy[0]) : NULL;

    /*if (duplicates && nPosToCompare == 1) {
    //Clean the duplicated lines
    std::shared_ptr<Column> col = segment->getColumn(posToCompare[0])->sort();
    uint32_t newsize = col->size();
    //Update all other columns with the new cardinality
    for (uint8_t i = 0; i < posConstants.size(); ++i) {
    Column *c = segment->getColumn(posConstants[i]).get();
    assert(c->isConstant());
    segment->replaceColumn(posConstants[i],
    std::shared_ptr<Column>(new
    ColumnImpl(c->getReader()->first(),
    newsize)));
    }
    //duplicates = false;
    }*/

    FCInternalTableItr *itr2 = existingValues != NULL &&
        match ? existingValues->getSortedIterator(nthreads) : NULL;

    //uint32_t idx1 = 0;
    bool toRead1 = false;
    bool toRead2 = true;
    bool active1 = true;
    bool active2 = match && itr2 != NULL && itr2->hasNext();

    if (!active2 && !duplicates) {
        //Don't need to do anything else. The values are already unique
        if (itr2 != NULL) {
            existingValues->releaseIterator(itr2);
        }
        return segment;
    }

    if (nPosToCompare == 1 && segment->getNColumns() == 1 && itr2 != NULL) {
        //It is column vs. column. Launch a faster algo than the one below
        std::shared_ptr<Column> c1 = segment->getColumn(posToCompare[0]);
        std::vector<uint8_t> fields;
        fields.push_back((uint8_t)0);
        std::vector<std::shared_ptr<Column>> c2 = itr2->getColumn(1, &(fields[0]));
        segment = retainMemMem(c1.get(), c2[0].get());
        existingValues->releaseIterator(itr2);
        return segment;
    }

    const uint8_t nfields = segment->getNColumns();
    std::unique_ptr<SegmentIterator> segmentIterator = segment->iterator();
    active1 = segmentIterator->hasNext();
    assert(active1);
    segmentIterator->next();

    Term_t *prevrow1 = new Term_t[nfields];
    bool prevrow1valid = false;
    SegmentInserter retainedValues(nfields);

    while (active1 && active2) {
        if (toRead1) {
            segmentIterator->next();

            if (duplicates) {
                if (prevrow1valid) {
                    bool same = true;
                    for (uint8_t i = 0; i < nfields && same; ++i)
                        if (prevrow1[i] != segmentIterator->get(i))
                            same = false;
                    if (same) {
                        active1 = segmentIterator->hasNext();
                        continue;
                    }
                }

                for (uint8_t i = 0; i < nfields; ++i) {
                    prevrow1[i] = segmentIterator->get(i);
                }
                prevrow1valid = true;
            }
            toRead1 = false;
        }

        if (toRead2) {
            itr2->next(); //I assume all values are unique
            toRead2 = false;
        }

        long res = 0;
        for (uint8_t i = 0; i < nPosToCompare; ++i) {
            if (segmentIterator->get(posToCompare[i]) != itr2->getCurrentValue(i)) {
                res = segmentIterator->get(posToCompare[i]) - itr2->getCurrentValue(i);
                break;
            }
        }
        if (res == 0) {
            toRead1 = true;
            active1 = segmentIterator->hasNext();
        } else if (res < 0) {
            retainedValues.copyArray(*segmentIterator.get());
            toRead1 = true;
            active1 = segmentIterator->hasNext();
        } else {
            toRead2 = true;
            active2 = itr2->hasNext();
        }
    }

    //Copy the remaining
    if (!toRead1) {
        retainedValues.copyArray(*segmentIterator.get());
        toRead1 = true;
        if (!prevrow1valid) {
            for (uint8_t i = 0; i < nfields; ++i) {
                prevrow1[i] = segmentIterator->get(i);
            }
            prevrow1valid = true;
        }
    }

    if (duplicates) {
        //for (; idx1 < segment->getNRows(); ++idx1) {
        while (segmentIterator->hasNext()) {
            segmentIterator->next();
            if (prevrow1valid) {
                bool same = true;
                for (uint8_t i = 0; i < nfields && same; ++i)
                    if (prevrow1[i] != segmentIterator->get(i))
                        same = false;
                if (same) {
                    continue;
                }
            }

            for (uint8_t i = 0; i < nfields; ++i) {
                prevrow1[i] = segmentIterator->get(i);
            }
            prevrow1valid = true;
            retainedValues.copyArray(*segmentIterator.get());
        }
    } else {
        //std::chrono::system_clock::time_point startFiltering = std::chrono::system_clock::now();

        //for (; idx1 < segment->getNRows(); ++idx1) {
        //    retainedValues.copyArray(segmentIterator);
        //}
        //
        while (segmentIterator->hasNext()) {
            segmentIterator->next();
            retainedValues.copyArray(*segmentIterator.get());
        }

        //std::chrono::duration<double> secFiltering = std::chrono::system_clock::now() - startFiltering;
        //cout << "Time filtering " << secFiltering.count() * 1000 << endl;
    }

    segmentIterator->clear();

    delete[] prevrow1;

    if (itr2 != NULL) {
        existingValues->releaseIterator(itr2);
    }

    return retainedValues.getSegment();
    }

    bool SegmentInserter::isSorted() const {
        return segmentSorted;
    }

    bool SegmentInserter::containsDuplicates() const {
        return this->duplicates;
    }

    std::shared_ptr<const Segment> SegmentInserter::concatenate(
            std::vector<std::shared_ptr<const Segment>> &segments) {
        const uint8_t nfields = segments[0]->getNColumns();
        //Check all segments have the same size
        for (int i = 1; i < segments.size(); ++i)
            if (segments[i]->getNColumns() != nfields)
                throw 10; //not possible

        SegmentInserter inserter(nfields);

        for (auto &segment : segments) {
            for (uint8_t i = 0; i < nfields; ++i) {
                inserter.addColumn(i, segment->getColumn(i), false);
            }
        }
        return inserter.getSegment();
    }

    std::shared_ptr<const Segment> SegmentInserter::concatenate(
            std::vector<std::shared_ptr<const Segment>> &segments, const int nthreads) {
        const uint8_t nfields = segments[0]->getNColumns();
        //Check all segments have the same size
        for (int i = 1; i < segments.size(); ++i)
            if (segments[i]->getNColumns() != nfields)
                throw 10; //not possible

        SegmentInserter inserter(nfields);

        for (auto &segment : segments) {
            for (uint8_t i = 0; i < nfields; ++i) {
                inserter.addColumn(i, segment->getColumn(i), false);
            }
        }
        return inserter.getSegment(nthreads);
    }

    std::shared_ptr<const Segment> SegmentInserter::unique(
            std::shared_ptr<const Segment> seg) {
        std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
        if (seg->getNColumns() == 1) {
            std::shared_ptr<Column> c = seg->getColumn(0);
            //I assume c is sorted
            auto c2 = c->unique();
            std::vector<std::shared_ptr<Column>> fields;
            fields.push_back(c2);
            std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
            LOG(TRACEL) << "Time SegmentInserter::unique = " << sec.count() * 1000;
            return std::shared_ptr<const Segment>(new Segment(1, fields));
        } else {

            bool sameLiteral = seg->areAllColumnsPartOftheSameQuery(NULL, NULL,
                    NULL);
            if (sameLiteral) {
                std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
                LOG(TRACEL) << "Time SegmentInserter::unique = " << sec.count() * 1000;
                return seg;
            }

            std::unique_ptr<SegmentIterator> itr = seg->iterator();
            const uint8_t ncolumns = seg->getNColumns();
            std::unique_ptr<Term_t[]> fields(new Term_t[ncolumns]);
            for (int i = 0; i < ncolumns; ++i)
                fields[i] = (Term_t) - 1;
            std::vector<ColumnWriter> writers;
            writers.resize(ncolumns);
            while (itr->hasNext()) {
                itr->next();
                bool unq = false;
                for (uint8_t i = 0; i < ncolumns; ++i)
                    if (itr->get(i) != fields[i]) {
                        unq = true;
                        break;
                    }

                if (unq) {
                    for (uint8_t i = 0; i < ncolumns; ++i) {
                        writers[i].add(itr->get(i));
                        fields[i] = itr->get(i);
                    }
                }
            }
            itr->clear();

            std::vector<std::shared_ptr<Column>> outputColumns;
            for (int i = 0; i < ncolumns; ++i)
                outputColumns.push_back(writers[i].getColumn());
            std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
            LOG(TRACEL) << "Time SegmentInserter::unique = " << sec.count() * 1000;
            return std::shared_ptr<const Segment>(new Segment(ncolumns,
                        outputColumns));
        }
    }

    bool varsHasNext(std::unique_ptr<ColumnReader> *p, const uint8_t s) {
        for (int i = 0; i < s; ++i) {
            if (! p[i]->hasNext()) {
                return false;
            }
        }
        return true;
    }

    void varsNext(Term_t *values, std::unique_ptr<ColumnReader> *p,
            const uint8_t s) {
        for (int i = 0; i < s; ++i)
            values[i] = p[i]->next();
    }

    std::shared_ptr<const Segment> SegmentInserter::merge(
            std::vector<std::shared_ptr<const Segment>> &segments) {

        LOG(TRACEL) << "SegmentInserter::merge";
        //Check all segments have the same size
        const uint8_t nfields = segments[0]->getNColumns();
        for (int i = 1; i < segments.size(); ++i)
            if (segments[i]->getNColumns() != segments[i - 1]->getNColumns())
                throw 10; //not possible

        //Should I compare all fields or only a few of them?
        std::vector<uint8_t> fieldsToCompare;
        for (uint8_t i = 0; i < nfields; ++i) {
            bool identical = true;
            for (int j = 1; j < segments.size(); ++j) {
                if (!segments[j - 1]->getColumn(i)->isConstant() ||
                        !segments[j]->getColumn(i)->isConstant()) {
                    identical = false;
                } else {
                    std::unique_ptr<ColumnReader> r1 = segments[j - 1]->getColumn(i)->getReader();
                    std::unique_ptr<ColumnReader> r2 = segments[j]->getColumn(i)->getReader();

                    if (r1->first() != r2->first()) {
                        identical = false;
                    }
                    r1->clear();
                    r2->clear();
                }
            }
            if (!identical) {
                fieldsToCompare.push_back(i);
            }
        }

        if (fieldsToCompare.size() == 0) {
            //Nothing to merge. The columns are equal to the existing one
            return segments[0];
        }

        std::shared_ptr<const Segment> lastSegment = segments[0];
        for (int idxCurSegment = 1;
                idxCurSegment < segments.size();
                ++idxCurSegment) {
            std::shared_ptr<const Segment> curSegment = segments[idxCurSegment];

            std::vector<std::unique_ptr<ColumnReader>> vars1;
            std::vector<std::unique_ptr<ColumnReader>> vars2;
            for (auto pos : fieldsToCompare) {
                vars1.push_back(lastSegment->getColumn(pos)->getReader());
                vars2.push_back(curSegment->getColumn(pos)->getReader());
            }

            SegmentInserter out((uint8_t) fieldsToCompare.size());
            /*uint32_t s1 = 0;
              uint32_t s2 = 0;
              const uint32_t e1 = lastSegment->getNRows();
              const uint32_t e2 = curSegment->getNRows();*/
            const uint8_t nvars = (uint8_t) fieldsToCompare.size();
            //LOG(TRACEL) << "Segment::merge, nvars = " << (int) nvars
            //                         << ", e1 = " << e1 << ", e2 = " << e2;

            bool lastValueEOF = false;
            Term_t lastValues[SIZETUPLE];
            bool curValueEOF = false;
            Term_t curValues[SIZETUPLE];

            long count1 = 1;
            long count2 = 1;

            std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
            if (varsHasNext(&vars1[0], nvars)) {
                varsNext(lastValues, &vars1[0], nvars);
                //lastValue = lastSegment->next();

                if (varsHasNext(&vars2[0], nvars)) {
                    varsNext(curValues, &vars2[0], nvars);
                    //curValue = curSegment->next();

                    while (!lastValueEOF && !curValueEOF) {
                        //Compare the columns
                        int res = 0;
                        for (uint8_t i = 0; i < nvars && res == 0; ++i) {
                            Term_t v1 = lastValues[i];
                            Term_t v2 = curValues[i];

                            if (v2 > v1) {
                                res = -1;
                                for (uint8_t j = 0; j < nvars; ++j)
                                    out.addAt(j, lastValues[j]);
                                if (varsHasNext(&vars1[0], nvars)) {
                                    varsNext(lastValues, &vars1[0], nvars);
                                    count1++;
                                } else {
                                    lastValueEOF = true;
                                    break;
                                }
                            } else if (v2 < v1) {
                                res = 1;
                                for (uint8_t j = 0; j < nvars; ++j)
                                    out.addAt(j, curValues[j]);
                                if (varsHasNext(&vars2[0], nvars)) {
                                    varsNext(curValues, &vars2[0], nvars);
                                    count2++;
                                } else {
                                    curValueEOF = true;
                                    break;
                                }
                            }
                        }

                        if (res == 0) {
                            //Add only one and increment both
                            for (uint8_t j = 0; j < nvars; ++j)
                                out.addAt(j, lastValues[j]);
                            if (varsHasNext(&vars1[0], nvars)) {
                                varsNext(lastValues, &vars1[0], nvars);
                                count1++;
                            } else {
                                lastValueEOF = true;
                                // break; No break here, you still need to increment vars2! --Ceriel
                            }
                            if (varsHasNext(&vars2[0], nvars)) {
                                varsNext(curValues, &vars2[0], nvars);
                                count2++;
                            } else {
                                curValueEOF = true;
                                break;
                            }
                        }
                    }
                } else {
                    curValueEOF = true;
                }
            } else {
                lastValueEOF = true;
            }

            //Add remaining
            while (!lastValueEOF) {
                for (uint8_t j = 0; j < nvars; ++j)
                    out.addAt(j, lastValues[j]);
                lastValueEOF = !varsHasNext(&vars1[0], nvars);
                if (!lastValueEOF) {
                    varsNext(lastValues, &vars1[0], nvars);
                    count1++;
                }
            }

            while (!curValueEOF) {
                for (uint8_t j = 0; j < nvars; ++j)
                    out.addAt(j, curValues[j]);
                curValueEOF = !varsHasNext(&vars2[0], nvars);
                if (!curValueEOF) {
                    varsNext(curValues, &vars2[0], nvars);
                    count2++;
                }
            }
            std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
            LOG(TRACEL) << "Time merge = " << sec.count() * 1000 << ", merged segments of " << count1 << " and "
                << count2 << " elements";

            //copy remaining fields
            uint8_t nv = 0;
            size_t nsize = out.getNRows();
            std::vector<std::shared_ptr<Column>> newcolumns;
            std::shared_ptr<const Segment> outSegment = out.getSegment();

            newcolumns.resize(nfields);
            for (uint8_t i = 0; i < nfields; ++i) {
                bool found = false;
                for (uint8_t j = 0; j < nvars && !found; ++j) {
                    if (fieldsToCompare[j] == i) {
                        found = true;
                    }
                }

                if (found) {
                    //Replace it with the merged one.
                    newcolumns[i] = outSegment->getColumn(nv++);
                } else {
                    newcolumns[i] = std::shared_ptr<Column>(
                            new CompressedColumn(
                                lastSegment->firstInColumn(i), nsize));
                }
            }

            lastSegment = std::shared_ptr<const Segment>(
                    new Segment(nfields, newcolumns));

            LOG(TRACEL) << "Segment::merge done";
            for (int i = 0; i < vars1.size(); i++) {
                if (vars1[i] != NULL) {
                    vars1[i]->clear();
                }
            }
            for (int i = 0; i < vars2.size(); i++) {
                if (vars2[i] != NULL) {
                    vars2[i]->clear();
                }
            }
        }

        return lastSegment;
    }

    std::unique_ptr<SegmentIterator> Segment::iterator() const {
        std::vector<const std::vector<Term_t> *> vectors;
        bool vectorSupported = true;
        for (int i = 0; i < nfields; i++) {
            if (! columns[i]->isBackedByVector()) {
                vectorSupported = false;
                break;
            } else {
                vectors.push_back(&columns[i]->getVectorRef());
            }
        }
        if (vectorSupported) {
            return std::unique_ptr<VectorSegmentIterator>(new VectorSegmentIterator(vectors, 0, vectors[0]->size(), NULL));
        }

        return std::unique_ptr<SegmentIterator>(
                new SegmentIterator(nfields, columns));
    }

    std::unique_ptr<VectorSegmentIterator> Segment::vectorIterator() const {
        std::vector<const std::vector<Term_t> *> vectors;
        std::vector<bool> allocated;
        for (int i = 0; i < nfields; i++) {
            if (! columns[i]->isBackedByVector()) {
                std::vector<Term_t> *v = new std::vector<Term_t>();
                *v = columns[i]->getReader()->asVector();
                allocated.push_back(true);
                vectors.push_back(v);
            } else {
                vectors.push_back(&columns[i]->getVectorRef());
                allocated.push_back(false);
            }
        }
        return std::unique_ptr<VectorSegmentIterator>(new VectorSegmentIterator(vectors, 0, vectors[0]->size(), new std::vector<bool>(allocated)));
    }
