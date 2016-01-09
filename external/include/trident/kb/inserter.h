/*
   Copyright (C) 2015 Jacopo Urbani.

   This file is part of Trident.

   Trident is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 2 of the License, or
   (at your option) any later version.

   Trident is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with Trident.  If not, see <http://www.gnu.org/licenses/>.
*/

#ifndef INSERTER_H_
#define INSERTER_H_

#include <trident/kb/dictmgmt.h>

#include <trident/tree/root.h>
#include <trident/kb/consts.h>
#include <trident/storage/storagestrat.h>

#include <tridentcompr/utils/factory.h>
#include <tridentcompr/utils/triple.h>

#include <vector>
#include <iostream>

#define POSAGGRBYTE 0x1000000000000l

class TreeInserter {

public:
    virtual void addEntry(nTerm key, long nElements, short file, int pos,
                          char stategy) = 0;

    virtual ~TreeInserter() {}
};

class Inserter {
private:
    static char STRATEGY_FOR_POS;

    Root* tree;
    TableStorage **files;
    const long nTerms;

    const bool useFixedStrategy;
    const char fixedStrategy;

    const size_t thresholdForColumnStorage;

    long currentT1[N_PARTITIONS];
    long currentT2[N_PARTITIONS];
    long nElements[N_PARTITIONS];

    long *values1[N_PARTITIONS];
    long *values2[N_PARTITIONS];

    short fileIdx[N_PARTITIONS];
    int startPositions[N_PARTITIONS];
    char strategies[N_PARTITIONS];
    bool onlyReferences[N_PARTITIONS];

    //Used for the aggregated indices
    long lastFirstTerm[N_PARTITIONS];
    long countLastFirstTerm[N_PARTITIONS];
    long coordinatesLastFirstTerm[N_PARTITIONS];
    long posElements[N_PARTITIONS];

    Statistics stats[N_PARTITIONS];

    StorageStrat storageStrategy[N_PARTITIONS];
    Factory<ListPairHandler> listFactory[N_PARTITIONS];
    Factory<GroupPairHandler> comprFactory[N_PARTITIONS];
    Factory<SimplifiedGroupPairHandler> list2Factory[N_PARTITIONS];
    PairHandler *currentPairHandler[N_PARTITIONS];

    boost::mutex mutex;

    long getCoordinatesForPOS(const int p);
    void writeCurrentEntryIntoTree(int permutation, TripleWriter *posArray,
                                   TreeInserter *treeInserter, const bool aggregated);
    void storeInmemoryValuesIntoFiles(int permutation, long* v1, long* v2,
                                      int n, TripleWriter *posArray, const bool aggregated);


public:
    Inserter(Root *tree, TableStorage **files, long nTerms,
             bool useFixedStrategy, char fixedStrategy) : nTerms(nTerms),
        useFixedStrategy(useFixedStrategy), fixedStrategy(fixedStrategy),
        thresholdForColumnStorage(StorageStrat::getBinaryBreakingPoint()) {
        this->tree = tree;
        this->files = files;

        for (int i = 0; i < N_PARTITIONS; ++i) {
            currentT1[i] = -1;
            values1[i] = new long[THRESHOLD_KEEP_MEMORY + 1];
            values2[i] = new long[THRESHOLD_KEEP_MEMORY + 1];
            storageStrategy[i].init(&listFactory[i], &comprFactory[i], &list2Factory[i]);
            currentPairHandler[i] = NULL;

            lastFirstTerm[i] = -1;
            countLastFirstTerm[i] = 0;
            posElements[i] = 0;
            coordinatesLastFirstTerm[i] = 0;
        }
        BOOST_LOG_TRIVIAL(debug) << "Threshold for column layout is " << thresholdForColumnStorage;
    }

    bool insert(const int permutation, const long t1, const long t2, const long t3, const long count,
                TripleWriter *posArray, TreeInserter *treeInserter, const bool aggregated);

    void insert(nTerm key, TermCoordinates *value);

    std::string getPathPermutationStorage(const int perm);

    void flush(int permutation, TripleWriter *posArray, TreeInserter *treeInserter,
               const bool aggregated);

    Statistics *getStats(int permutation) {
        return &stats[permutation];
    }

    ~Inserter() {
        for (int i = 0; i < N_PARTITIONS; ++i) {
            delete[] values1[i];
            delete[] values2[i];
        }
    }
};

#endif /* INSERTER_H_ */
