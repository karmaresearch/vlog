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

#include <trident/kb/inserter.h>
#include <trident/storage/storagestrat.h>
#include <trident/storage/pairstorage.h>
#include <trident/storage/pairhandler.h>

#include <boost/chrono.hpp>
#include <boost/log/trivial.hpp>

char Inserter::STRATEGY_FOR_POS =
    StorageStrat::getStrategy(ROW_LAYOUT, NO_DIFFERENCE, NO_COMPR, NO_COMPR, true);

namespace timens = boost::chrono;

bool Inserter::insert(const int permutation, const long t1, const long t2, const long t3, const long count,
                      TripleWriter *posArray, TreeInserter *treeInserter, const bool aggregated) {

    bool ret = false;
    if (t1 != currentT1[permutation]) {
        if (currentT1[permutation] != -1) {
            writeCurrentEntryIntoTree(permutation, posArray, treeInserter, aggregated);
        }
        currentT1[permutation] = t1;
        currentT2[permutation] = t2;
        nElements[permutation] = 0;

        if (posArray != NULL) {
            lastFirstTerm[permutation] = -1;
            countLastFirstTerm[permutation] = 0;
            coordinatesLastFirstTerm[permutation] = 0;
        }
    }

    long n = nElements[permutation]++;
    if (aggregated) {
        posElements[permutation] += count;
    }
    if (n < THRESHOLD_KEEP_MEMORY) {
        // Copy the values in an internal buffer
        values1[permutation][(int) n] = t2;
        values2[permutation][(int) n] = t3;
    } else {
        if (n == THRESHOLD_KEEP_MEMORY) {
            storeInmemoryValuesIntoFiles(permutation, values1[permutation],
                                         values2[permutation], (int) n, posArray,
                                         aggregated);
        }

        if (posArray != NULL) {
            if (t2 != lastFirstTerm[permutation]) { // store element into the list
                if (lastFirstTerm[permutation] != -1) {
                    posArray->write(lastFirstTerm[permutation], currentT1[permutation]
                                    , coordinatesLastFirstTerm[permutation],
                                    countLastFirstTerm[permutation]);
                }
                coordinatesLastFirstTerm[permutation] = getCoordinatesForPOS(permutation);
                lastFirstTerm[permutation] = t2;
                countLastFirstTerm[permutation] = 0;
            }
            posArray->write(t2, t1, t3 | POSAGGRBYTE, 1);
            countLastFirstTerm[permutation]++;
        }

        if (aggregated) {
            if (onlyReferences[permutation]) {
                if ((t3 & POSAGGRBYTE) == 0) {
                    files[permutation]->append(t2, t3);
                }
            } else {
                if (t3 & POSAGGRBYTE) {
                    files[permutation]->append(t2, t3 & ~POSAGGRBYTE);
                }
            }
        } else {
            files[permutation]->append(t2, t3);
        }

    }
    return ret;
}

std::string Inserter::getPathPermutationStorage(const int perm) {
    return files[perm]->getPath();
}

long Inserter::getCoordinatesForPOS(const int p) {
    long coordinates = ((long) (strategies[p] & 0xFF) << 48) + ((long) fileIdx[p] << 32)
                       + startPositions[p];
    return coordinates;
}

void Inserter::insert(nTerm key, TermCoordinates *value) {
    tree->put(key, value);
}

void Inserter::writeCurrentEntryIntoTree(int permutation,
        TripleWriter *posArray, TreeInserter *treeInserter, const bool aggregated) {

    if (nElements[permutation] <= THRESHOLD_KEEP_MEMORY) {
        storeInmemoryValuesIntoFiles(permutation, values1[permutation],
                                     values2[permutation], (int) nElements[permutation],
                                     posArray, aggregated);
    }
    if (posArray != NULL && lastFirstTerm[permutation] != -1) {
        posArray->write(lastFirstTerm[permutation], currentT1[permutation],
                        coordinatesLastFirstTerm[permutation], countLastFirstTerm[permutation]);
    }

    //Stop append
    files[permutation]->stopAppend();

    //Release PairHandler
    switch (currentPairHandler[permutation]->getType()) {
    case ROW_LAYOUT:
        listFactory[permutation].release(
            (ListPairHandler *) (currentPairHandler[permutation]));
        break;
    case CLUSTER_LAYOUT:
        comprFactory[permutation].release(
            (GroupPairHandler *) (currentPairHandler[permutation]));
        break;
    case COLUMN_LAYOUT:
        list2Factory[permutation].release(
            (SimplifiedGroupPairHandler*) (currentPairHandler[permutation]));
    }

    long nels;
    if (aggregated) {
        nels = posElements[permutation];
        nels /= 2; //I divide it by two because each table receives twice number of counts. One of each rows, and one for the aggregated ones.
        posElements[permutation] = 0;
    } else {
        nels = nElements[permutation];
    }

    treeInserter->addEntry(currentT1[permutation], nels,
                           fileIdx[permutation], startPositions[permutation],
                           strategies[permutation]);
}

void Inserter::storeInmemoryValuesIntoFiles(int permutation, long* v1, long* v2,
        int n, TripleWriter *posArray, const bool aggregated) {
    //Determine the storage strategy to use and register the coordinates
    char strat;
    if (!aggregated) {
        if (useFixedStrategy) {
            strat = fixedStrategy;
        } else { //Dynamic strategy
            strat = StorageStrat::determineStrategy(v1, v2, n, nTerms,
                                                    thresholdForColumnStorage,
                                                    stats[permutation]);
        }
    } else {
        //Should I store only the reference or the elements directly?
        onlyReferences[permutation] = StorageStrat::determineAggregatedStrategy(v1, v2, n, nTerms, stats[permutation]);
        if (onlyReferences[permutation]) {
            strat = STRATEGY_FOR_POS;
        } else {
            strat = StorageStrat::determineStrategy(v1, v2, n, nTerms,
                                                    thresholdForColumnStorage,
                                                    stats[permutation]);
        }
    }

    currentPairHandler[permutation] =
        storageStrategy[permutation].getPairHandler(strat);
    startPositions[permutation] = files[permutation]->startAppend(
                                      currentPairHandler[permutation]);
    fileIdx[permutation] = files[permutation]->getLastCreatedFile();
    strategies[permutation] = strat;

    // Copy the values in-memory to disk
    for (int i = 0; i < n; ++i) {
        if (posArray != NULL) {
            if (v1[i] != lastFirstTerm[permutation]) {
                // store element into the list
                if (lastFirstTerm[permutation] != -1) {
                    posArray->write(lastFirstTerm[permutation], currentT1[permutation],
                                    coordinatesLastFirstTerm[permutation],
                                    countLastFirstTerm[permutation]);
                }
                lastFirstTerm[permutation] = v1[i];
                countLastFirstTerm[permutation] = 0;
                coordinatesLastFirstTerm[permutation] = getCoordinatesForPOS(permutation);
            }
            posArray->write(v1[i], currentT1[permutation], v2[i] | POSAGGRBYTE, 1);
            countLastFirstTerm[permutation]++;
        }
        if (aggregated) {
            if (onlyReferences[permutation]) {
                if ((v2[i] & POSAGGRBYTE) == 0) {
                    files[permutation]->append(v1[i], v2[i]);
                }
            } else {
                if (v2[i] & POSAGGRBYTE) {
                    files[permutation]->append(v1[i], v2[i] & ~POSAGGRBYTE);
                }
            }
        } else {
            files[permutation]->append(v1[i], v2[i]);
        }
    }
}

void Inserter::flush(int permutation, TripleWriter *posArray,
                     TreeInserter *treeInserter, const bool aggregated) {
    if (currentT1[permutation] != -1) {
        writeCurrentEntryIntoTree(permutation, posArray, treeInserter, aggregated);
    }
    currentT1[permutation] = -1;
}
