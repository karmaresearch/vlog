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
