/*
 * memoryopt.h
 *
 *  Created on: Jan 31, 2014
 *      Author: jacopo
 */

#ifndef MEMORYOPT_H_
#define MEMORYOPT_H_

#include <trident/kb/kbconfig.h>

class MemoryOptimizer {

private:

    static int calculateBytesPerDictPair(long nTerms, int leafSize);

public:
    static void optimizeForWriting(long inputTriples, KBConfig &config);

    static void optimizeForReading(int ndicts, KBConfig &config);

    static void optimizeForReasoning(int ndicts, KBConfig &config);

};

#endif /* MEMORYOPT_H_ */
