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

#ifndef STORAGESTRAT_H_
#define STORAGESTRAT_H_

#include <trident/storage/pairhandler.h>
#include <trident/kb/consts.h>

#include <tridentcompr/utils/factory.h>

/*
 * Current format: 2bits <storage format> -- 1bit <delta on the first term> -- 2 bit <compr on firs el> -- 2 bit <compr on second el> -- 1 bit <aggregated>
 */


#define RATE_LIST 1.05

struct Statistics {
    long nListStrategies;
    long nList2Strategies;
    long nGroupStrategies;

    long nFirstCompr1;
    long nFirstCompr2;
    long nSecondCompr1;
    long nSecondCompr2;

    long diff;
    long nodiff;

    long exact;
    long approximate;

    long aggregated, notAggregated;

    Statistics() {
        nList2Strategies = nListStrategies = nGroupStrategies = 0;
        nFirstCompr1 = nFirstCompr2 = nSecondCompr1 = nSecondCompr2 = 0;
        exact = approximate = 0;
        diff = nodiff  = 0;
        aggregated = 0;
        notAggregated = 0;
    }
};

class StorageStrat {
public:
    struct Combinations {
        int type;
        int compr1Mode;
        int compr2Mode;
        int diffMode;
        long sum;
    };

protected:

    static unsigned getStrat1();
    static unsigned getStrat2();
    static unsigned getStrat3();


    static void createAllCombinations(std::vector<Combinations> &output,
                                      long *groupCounters1Compr2, long *listCounters1Compr2,
                                      long groupCounters2Compr2, long listCounters2Compr2,
                                      long *groupCounters1Compr1, long *listCounters1Compr1,
                                      long groupCounters2Compr1, long listCounters2Compr1);

private:
    long static minsum(const long counters1[2][2], const long counters2[2], int &c1, int &c2, int &d);

    unsigned static setStorageType(const unsigned signature, int type) {
        return signature | type << 6;
    }

    unsigned static getStorageType(const unsigned signature) {
        return signature >> 6 & 3;
    }

    unsigned static setCompr1(const unsigned signature, unsigned compr) {
        return (compr == NO_COMPR ?
                signature | 0x10 :
                (compr == COMPR_2 ? (signature | 0x8) & 0xEF : signature & 0xE7));
    }

    unsigned static getCompr1(const unsigned signature) {
        return ((signature & 0x10) != 0) ? NO_COMPR :
               ((signature & 0x8) != 0) ? COMPR_2 : COMPR_1;
    }

    unsigned static setCompr2(const unsigned signature, int compr) {
        return (compr == NO_COMPR ?
                signature | 0x4 :
                (compr == COMPR_2 ? (signature | 0x2) & 0xFB : signature & 0xF9));
    }

    unsigned static getCompr2(const unsigned signature) {
        return ((signature & 0x4) != 0) ? NO_COMPR :
               ((signature & 0x2) != 0) ? COMPR_2 : COMPR_1;
    }

    unsigned static setDiff1(const unsigned signature, int diff) {
        return (diff == NO_DIFFERENCE ? signature | 0x20 : signature & 0xDF);
    }

    unsigned static getDiff1(const unsigned signature) {
        return ((signature & 0x20) != 0) ? NO_DIFFERENCE : DIFFERENCE;
    }

    unsigned static setAggregated(const unsigned signature, const bool aggregate) {
        if (aggregate) {
            return signature | 1;
        } else {
            return signature & 0xFE;
        }
    }


    Factory<ListPairHandler> *f1;
    Factory<GroupPairHandler> *f2;
    Factory<SimplifiedGroupPairHandler> *f3;

public:
    static const unsigned FIXEDSTRAT1;
    static const unsigned FIXEDSTRAT2;
    static const unsigned FIXEDSTRAT3;

    static size_t getBinaryBreakingPoint();

    static bool determineAggregatedStrategy(long *v1, long *v2, const int size,
                                            const long nTerms, Statistics &stats);

    static char determineStrategy(long *v1, long *v2, const int size,
                                  const long nTerms,
                                  const size_t nTermsClusterColumn,
                                  Statistics &stats);

    static char determineStrategyold(long *v1, long *v2, const int size,
                                     const long nTerms,
                                     Statistics &stats);

    static char getStrategy(int typeStorage, int diff, int compr1, int compr2,
                            bool aggregated);

    int static isAggregated(const char signature) {
        return signature & 1;
    }

    StorageStrat() {
        f1 = NULL;
        f2 = NULL;
        f3 = NULL;
        statsCluster = statsRow = statsColumn = 0;
    }

    void resetCounters() {
        statsCluster = statsRow = statsColumn = 0;
    }

    void init(Factory<ListPairHandler> *listFactory,
              Factory<GroupPairHandler> *comprFactory, Factory<SimplifiedGroupPairHandler> *list2Factory) {
        this->f1 = listFactory;
        this->f2 = comprFactory;
        this->f3 = list2Factory;
    }

    PairHandler *getPairHandler(const char signature);

    long statsCluster, statsRow, statsColumn;
};

#endif /* STORAGESTRAT_H_ */
