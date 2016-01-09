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

#include <trident/storage/storagestrat.h>

#include <boost/chrono.hpp>

unsigned StorageStrat::getStrat1() {
    unsigned output = 0;
    output = StorageStrat::setStorageType(output, CLUSTER_LAYOUT);
    output = StorageStrat::setCompr1(output, COMPR_2);
    output = StorageStrat::setCompr2(output, COMPR_2);
    output = StorageStrat::setDiff1(output, NO_DIFFERENCE);
    return output;
}

unsigned StorageStrat::getStrat2() {
    unsigned output = 0;
    output = StorageStrat::setStorageType(output, ROW_LAYOUT);
    output = StorageStrat::setCompr1(output, COMPR_2);
    output = StorageStrat::setCompr2(output, COMPR_2);
    output = StorageStrat::setDiff1(output, NO_DIFFERENCE);
    return output;
}

unsigned StorageStrat::getStrat3() {
    unsigned output = 0;
    output = StorageStrat::setStorageType(output, COLUMN_LAYOUT);
    output = StorageStrat::setCompr1(output, COMPR_2);
    output = StorageStrat::setCompr2(output, COMPR_2);
    output = StorageStrat::setDiff1(output, NO_DIFFERENCE);
    return output;
}

const unsigned StorageStrat::FIXEDSTRAT1 = getStrat1();
const unsigned StorageStrat::FIXEDSTRAT2 = getStrat2();
const unsigned StorageStrat::FIXEDSTRAT3 = getStrat3();

PairHandler *StorageStrat::getPairHandler(const char signature) {
    int storageType = getStorageType(signature);
    if (storageType == ROW_LAYOUT) {
        statsRow++;
        ListPairHandler *ph = f1->get();
        ph->setCompressionMode(getCompr1(signature), getCompr2(signature));
        ph->setDifferenceMode(getDiff1(signature));
        return ph;
    } else if (storageType == CLUSTER_LAYOUT)  {
        statsCluster++;
        GroupPairHandler *ph = f2->get();
        ph->mode_compression(getCompr1(signature), getCompr2(signature));
        ph->mode_difference(getDiff1(signature));
        return ph;
    } else {
        statsColumn++;
        SimplifiedGroupPairHandler *ph = f3->get();
        ph->setCompressionMode(getCompr1(signature), getCompr2(signature));
        return ph;
    }
}

char StorageStrat::getStrategy(int typeStorage, int diff, int compr1,
                               int compr2, bool aggre) {
    unsigned strat = 0;
    strat = setStorageType(strat, typeStorage);
    strat = setDiff1(strat, diff);
    strat = setCompr1(strat, compr1);
    strat = setCompr2(strat, compr2);
    strat = setAggregated(strat, aggre);
    return (char) strat;
}

long StorageStrat::minsum(const long counters1[2][2], const long counters2[2], int &oc1, int &oc2, int &od) {
    long minSum = LONG_MAX;
    for (int c1 = 0; c1 < 2; c1++) {
        for (int delta = 0; delta < 2; ++delta) {
            for (int c2 = 0; c2 < 2; c2++) {
                long sum = counters1[c1][delta] + counters2[c2];
                if (sum < minSum) {
                    minSum = sum;
                    oc1 = c1;
                    oc2 = c2;
                    od = delta;
                }
            }
        }
    }
    return minSum;
}

size_t StorageStrat::getBinaryBreakingPoint() {
    //Used only to ensure the compiler is not optimizing the code out
    long fakeSum = 0;

    //Anything below 8 can be search faster with a linear search
    int i = 8;
    for (; i < 4096; (i > 32) ? i += 32 : i *= 2) {
        std::vector<long> vector1;

        //Fill the table
        for (int j = 0; j < i; ++j) {
            vector1.push_back(rand());
        }

        //Sort the array
        std::sort(vector1.begin(), vector1.end());

        /*const long* values = &vector1[0];
        //const int* values2 = &vector2[0];
        timens::system_clock::time_point start = timens::system_clock::now();
        for (int lookups = 0; lookups < 100000; ++lookups) {
            const int idx = rand() % i;
            fakeSum += values[idx];
            //fakeSum += values2[idx];
        }
        boost::chrono::duration<double> sec = boost::chrono::system_clock::now() - start;*/

        std::vector<size_t> idsToSearch;
        for (int j = 0; j < 1000; ++j) {
            idsToSearch.push_back(rand() % i);
        }

        boost::chrono::duration<double> linearSec[31];
        for (int m = 0; m < 31; ++m) {
            //Search linearly
            boost::chrono::system_clock::time_point start = boost::chrono::system_clock::now();
            for (std::vector<size_t>::const_iterator itr = idsToSearch.begin();
                    itr != idsToSearch.end(); ++itr) {
                std::vector<long>::const_iterator itrP = vector1.begin();
                while (itrP != vector1.end()) {
                    if (*itrP >= vector1[*itr]) {
                        break;
                    }
                    itrP++;
                }
                fakeSum += m + ((itrP != vector1.end() && *itrP == vector1[*itr]) ? 1 : 0);
            }
            linearSec[m] = boost::chrono::system_clock::now() - start;
        }

        //Search with binary search
        boost::chrono::duration<double> binarySec[31];
        for (int m = 0; m < 31; ++m) {
            boost::chrono::system_clock::time_point start = boost::chrono::system_clock::now();
            for (std::vector<size_t>::const_iterator itr = idsToSearch.begin();
                    itr != idsToSearch.end(); ++itr) {
                fakeSum += m + ((std::binary_search(vector1.begin(), vector1.end(),
                                                    vector1[*itr])) ? 1 : 0);
            }
            binarySec[m] = boost::chrono::system_clock::now() - start;
        }

        int linearWins = 0;
        for (int m = 0; m < 31; ++m) {
            if (linearSec[m].count() <= binarySec[m].count())
                linearWins++;
            //cout << "i=" << i << " wins=" << linearWins << " linear " << linearSec[m].count() << " binary " << binarySec[m].count() << endl;
        }

        if (linearWins < 16) {
            return i > 32 ? i - 32 : i / 2;
        }

        //This is a simple check to ensure the compiler is not deleting the sum
        if (fakeSum == 0)
            throw 10;
    }
    return 4096;
}

bool StorageStrat::determineAggregatedStrategy(long *v1, long *v2, const int size,
        const long nTerms, Statistics &stats) {
    //return true if v1 contains many duplicates

    //For every different v1, there are at least two different pairs, one with the value and one with the coordinates.
    long unique = 1;
    for (size_t i = 1; i < size; ++i) {
        if (v1[i - 1] != v1[i]) {
            unique++;
        }
    }
    if (unique <= size / 10) {
        stats.aggregated++;
        return true; //aggregate
    } else {
        stats.notAggregated++;
        return false; //do not aggregate
    }
}

void StorageStrat::createAllCombinations(std::vector<Combinations> &output,
        long *groupCounters1Compr2, long *listCounters1Compr2,
        long groupCounters2Compr2, long listCounters2Compr2,
        long *groupCounters1Compr1, long *listCounters1Compr1,
        long groupCounters2Compr1, long listCounters2Compr1) {

    for (int diff = 0; diff < 2; ++diff) {
        //group
        Combinations c;
        c.diffMode = diff;
        c.type = CLUSTER_LAYOUT;
        c.compr1Mode = COMPR_2;
        c.compr2Mode = COMPR_2;
        c.sum = groupCounters1Compr2[diff] + groupCounters2Compr2;
        output.push_back(c);
        c.compr1Mode = COMPR_2;
        c.compr2Mode = COMPR_1;
        c.sum = groupCounters1Compr2[diff] + groupCounters2Compr1;
        output.push_back(c);
        c.compr1Mode = COMPR_1;
        c.compr2Mode = COMPR_2;
        c.sum = groupCounters1Compr1[diff] + groupCounters2Compr2;
        output.push_back(c);
        c.compr1Mode = COMPR_1;
        c.compr2Mode = COMPR_1;
        c.sum = groupCounters1Compr1[diff] + groupCounters2Compr1;
        output.push_back(c);

        //row
        c.type = ROW_LAYOUT;
        c.compr1Mode = COMPR_2;
        c.compr2Mode = COMPR_2;
        c.sum = listCounters1Compr2[diff] + listCounters2Compr2;
        output.push_back(c);
        c.compr1Mode = COMPR_2;
        c.compr2Mode = COMPR_1;
        c.sum = listCounters1Compr2[diff] + listCounters2Compr1;
        output.push_back(c);
        c.compr1Mode = COMPR_1;
        c.compr2Mode = COMPR_2;
        c.sum = listCounters1Compr1[diff] + listCounters2Compr2;
        output.push_back(c);
        c.compr1Mode = COMPR_1;
        c.compr2Mode = COMPR_1;
        c.sum = listCounters1Compr1[diff] + listCounters2Compr1;
        output.push_back(c);
    }
}

bool combinationSorter(const StorageStrat::Combinations &c1, const StorageStrat::Combinations &c2) {
    if (c1.sum == c2.sum) {
        if (c1.diffMode == NO_DIFFERENCE && c2.diffMode == DIFFERENCE) {
            return true;
        } else if (c1.diffMode == DIFFERENCE && c2.diffMode == NO_DIFFERENCE) {
            return false;
        }

        //compr2 goes first
        if (c1.compr1Mode == COMPR_2 && c2.compr1Mode == COMPR_1) {
            return true;
        } else if (c1.compr1Mode == COMPR_1 && c2.compr1Mode == COMPR_2) {
            return false;
        }

        if (c1.compr2Mode == COMPR_2 && c2.compr2Mode == COMPR_1) {
            return true;
        } else if (c1.compr2Mode == COMPR_1 && c2.compr2Mode == COMPR_2) {
            return false;
        }

    }
    return c1.sum < c2.sum;
}

char StorageStrat::determineStrategy(long *v1, long *v2, const int size,
                                     const long nTermsInInput,
                                     const size_t nTermsClusterColumn,
                                     Statistics &stats) {
    unsigned strat = 0;

    /***** DETERMINE TYPE STORAGE *****/
    int typeStorage = CLUSTER_LAYOUT;
    //These counters assume compr2
    long listCounters1[2];
    long groupCounters1[2];
    long bytesSecondGroup = 0;
    listCounters1[0] = groupCounters1[0] = 0;
    listCounters1[1] = groupCounters1[1] = 0;
    long listCounters2 = 0, groupCounters2 = 0;

    //These counters assume compr1
    long listCounters1_2[2];
    long groupCounters1_2[2];
    long bytesSecondGroup_2 = 0;
    listCounters1_2[0] = groupCounters1_2[0] = 0;
    listCounters1_2[1] = groupCounters1_2[1] = 0;
    long listCounters2_2 = 0, groupCounters2_2 = 0;

    if (size < THRESHOLD_KEEP_MEMORY) {
        //What is the layout that compresses the table the most?
        size_t nUniqueFirstTerms = 0;

        //Calculate all options, and pick the best:
        long prevFirstValue = -1;
        long prevSecondValue = -1;

        for (int i = 0; i < size; i++) {
            for (int delta = 0; delta < 2; delta++) {
                //Size first term
                long value;
                if (delta == DIFFERENCE && prevFirstValue != -1) {
                    value = v1[i] - prevFirstValue;
                } else {
                    value = v1[i];
                }
                int bs = Utils::numBytes2(value);
                int bs2 = Utils::numBytes(value);

                listCounters1[delta] += bs;
                listCounters1_2[delta] += bs2;
                if (v1[i] != prevFirstValue) {
                    groupCounters1[delta] += bs + 1;
                    groupCounters1_2[delta] += bs2 + 1;

                    //Pointer is 4 bytes, and not one.
                    if (bytesSecondGroup > 255) {
                        groupCounters1[delta] += 3;
                    }
                    if (bytesSecondGroup_2 > 255) {
                        groupCounters1_2[delta] += 3;
                    }
                }
            }

            if (v1[i] != prevFirstValue) {
                prevFirstValue = v1[i];
                prevSecondValue = -1;
                nUniqueFirstTerms++;
                bytesSecondGroup = 0;
                bytesSecondGroup_2 = 0;
            }

            //Size second term: -- List
            long value = v2[i];
            int bs = Utils::numBytes2(value);
            int bs2 = Utils::numBytes(value);
            listCounters2 += bs;
            listCounters2_2 += bs2;

            //-- Group
            if (prevSecondValue != -1) {
                value = v2[i] - prevSecondValue;
                bs = Utils::numBytes2(value);
                bs2 = Utils::numBytes(value);
            }
            groupCounters2 += bs;
            bytesSecondGroup += bs;
            groupCounters2_2 += bs2;
            bytesSecondGroup_2 += bs2;
            prevSecondValue = v2[i];

            if (nUniqueFirstTerms > nTermsClusterColumn) {
                //Linear search is not possible on this set. Switch to column store
                typeStorage = COLUMN_LAYOUT;
                stats.nList2Strategies++;
                strat = setCompr1(strat, COMPR_2);
                strat = setCompr2(strat, COMPR_2);
                stats.nFirstCompr2++;
                stats.nSecondCompr2++;
                strat = setStorageType(strat, typeStorage);
                return (char) strat;
            }
        }

        //I might still have to add some bytes in case the last group was larger
        for (int delta = 0; delta < 2; delta++) {
            if (bytesSecondGroup > 255) {
                groupCounters1[delta] += 3;
            }
            if (bytesSecondGroup_2 > 255) {
                groupCounters1_2[delta] += 3;
            }
        }

        std::vector<Combinations> allCombinations;
        createAllCombinations(allCombinations, groupCounters1, listCounters1,
                              groupCounters2, listCounters2, groupCounters1_2,
                              listCounters1_2, groupCounters2_2, listCounters2_2);
        std::sort(allCombinations.begin(), allCombinations.end(), combinationSorter);

        //Smallest comnination
        Combinations c = allCombinations.front();
        if (c.type == CLUSTER_LAYOUT) {
            typeStorage = CLUSTER_LAYOUT;
            stats.nGroupStrategies++;
        } else {
            typeStorage = ROW_LAYOUT;
            stats.nListStrategies++;
        }

        if (c.compr1Mode == COMPR_2) {
            strat = setCompr1(strat, COMPR_2);
            stats.nFirstCompr2++;
        } else {
            strat = setCompr1(strat, COMPR_1);
            stats.nFirstCompr1++;
        }

        if (c.compr2Mode == COMPR_2) {
            strat = setCompr2(strat, COMPR_2);
            stats.nSecondCompr2++;
        } else {
            strat = setCompr2(strat, COMPR_1);
            stats.nSecondCompr1++;
        }

        if (c.diffMode == DIFFERENCE) {
            strat = setDiff1(strat, DIFFERENCE);
            stats.diff++;
        } else {
            strat = setDiff1(strat, NO_DIFFERENCE);
            stats.nodiff++;
        }

        stats.exact++;
    } else {
        typeStorage = COLUMN_LAYOUT;
        stats.nList2Strategies++;
        stats.approximate++;
        strat = setCompr1(strat, COMPR_2);
        strat = setCompr2(strat, COMPR_2);
        stats.nFirstCompr2++;
        stats.nSecondCompr2++;
    }

    strat = setStorageType(strat, typeStorage);
    return (char) strat;
}

char StorageStrat::determineStrategyold(long *v1, long *v2, const int size,
                                        const long nTerms, Statistics &stats) {
    unsigned strat = 0;

    long listCounters1[2][2];
    long groupCounters1[2][2];
    long listCounters2[2];
    long groupCounters2[2];
    for (int i = 0; i < 2; ++i) {
        for (int j = 0; j < 2; ++j) {
            listCounters1[i][j] = 0;
            groupCounters1[i][j] = 0;
        }
        listCounters2[i] = 0;
        groupCounters2[i] = 0;
    }

    long countUniqueValues = 0;
    long prevFirstValue = -1;
    long prevSecondValue = -1;
    for (int i = 0; i < size; i++) {
        for (int compr1 = 0; compr1 < 2; compr1++) {
            for (int delta = 0; delta < 2; delta++) {
                long value;
                if (delta == DIFFERENCE && prevFirstValue != -1) {
                    value = v1[i] - prevFirstValue;
                } else {
                    value = v1[i];
                }

                int bs;
                if (compr1 == COMPR_1) {
                    bs = Utils::numBytes(value);
                } else {
                    bs = Utils::numBytes2(value);
                }

                listCounters1[compr1][delta] += bs;
                if (v1[i] != prevFirstValue) {
                    groupCounters1[compr1][delta] += bs + 1;
                }
            }
        }

        if (v1[i] != prevFirstValue) {
            countUniqueValues++;
            prevFirstValue = v1[i];
            prevSecondValue = -1;
        }

        for (int compr2 = 0; compr2 < 2; compr2++) {
            long value = v2[i];
            int bs;
            if (compr2 == COMPR_1) {
                bs = Utils::numBytes(value);
            } else {
                bs = Utils::numBytes2(value);
            }
            listCounters2[compr2] += bs;

            if (prevSecondValue != -1) {
                value = v2[i] - prevSecondValue;
                if (compr2 == COMPR_1) {
                    bs = Utils::numBytes(value);
                } else {
                    bs = Utils::numBytes2(value);
                }
            }
            groupCounters2[compr2] += bs;
        }
        prevSecondValue = v2[i];
    }

    /***** DETERMINE TYPE STORAGE *****/
    bool list = false;
    bool calculateCompression = true;
    if (size < THRESHOLD_KEEP_MEMORY) {
        int gc1, gc2, gd;
        long minG = minsum(groupCounters1, groupCounters2, gc1, gc2, gd);
        int lc1, lc2, ld;
        long minL = minsum(listCounters1, listCounters2, lc1, lc2, ld);
        int c1, c2, d;
        if (minL < minG) {
            list = true;
            c1 = lc1;
            c2 = lc2;
            d = ld;
        } else {
            c1 = gc1;
            c2 = gc2;
            d = gd;
        }

        //Set compressions and deltas
        if (c1 == COMPR_1) {
            strat = setCompr1(strat, COMPR_1);
            stats.nFirstCompr1++;
        } else {
            strat = setCompr1(strat, COMPR_2);
            stats.nFirstCompr2++;
        }

        if (c2 == COMPR_1) {
            strat = setCompr2(strat, COMPR_1);
            stats.nSecondCompr1++;
        } else {
            strat = setCompr2(strat, COMPR_2);
            stats.nSecondCompr2++;
        }

        if (d == DIFFERENCE) {
            strat = setDiff1(strat, DIFFERENCE);
            stats.diff++;
        } else {
            strat = setDiff1(strat, NO_DIFFERENCE);
            stats.nodiff++;
        }

        stats.exact++;
        calculateCompression = false;
    } else {
        //Do estimate
        double rate = (double) size / countUniqueValues;
        list = rate <= RATE_LIST;
        stats.approximate++;
    }

    if (list) {
        strat = setStorageType(strat, ROW_LAYOUT);
        stats.nListStrategies++;
    } else {
        strat = setStorageType(strat, CLUSTER_LAYOUT);
        stats.nGroupStrategies++;
    }

    if (calculateCompression) {
        /***** DETERMINE DATA COMPRESSION *****/

        //The first term is always a p, which is small because p are popular. Always choose compr2
        strat = setCompr1(strat, COMPR_2);
        stats.nFirstCompr2++;

        if (list) {
            if (nTerms < 0x10000000) {
                strat = setCompr2(strat, COMPR_2);
                stats.nSecondCompr2++;
            } else {
                strat = setCompr2(strat, COMPR_1);
                stats.nSecondCompr1++;
            }
        } else {
            strat = setCompr2(strat, COMPR_2);
            stats.nSecondCompr2++;
        }

        /***** DETERMINE DIFFERENCE ON FIRST ELEMENTS *****/
        if (!list) {
            bool delta = false;
            prevFirstValue = -1;
            for (int i  = 0; i < size; ++i) {
                if (v1[i] != prevFirstValue) {
                    countUniqueValues++;
                    prevFirstValue = v1[i];
                    if (countUniqueValues == 10) {
                        delta = true;
                        break;
                    }
                }
            }
            if (!delta) {
                strat = setDiff1(strat, NO_DIFFERENCE);
                stats.nodiff++;
            } else {
                strat = setDiff1(strat, DIFFERENCE);
                stats.diff++;
            }
        } else {
            if (size >= 10) {
                strat = setDiff1(strat, NO_DIFFERENCE);
                stats.nodiff++;
            } else {
                strat = setDiff1(strat, DIFFERENCE);
                stats.diff++;

            }
        }
    }
    return (char) strat;
}
