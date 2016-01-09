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

#ifndef KBB_H_
#define KBB_H_

#include <trident/kb/consts.h>
#include <trident/kb/statistics.h>
#include <trident/kb/dictmgmt.h>
#include <trident/kb/kbconfig.h>
#include <trident/kb/cacheidx.h>
#include <trident/memory/memorymgr.h>

#include <boost/thread.hpp>

#include <string>

class Leaf;
class Querier;
class Inserter;
class TableStorage;
class Root;
class StringBuffer;
struct FileSegment;

using namespace std;

class KB {
private:
    const string path;

    const bool readOnly;

    Root *tree;

    Stats stats;
    Stats *statsStrings;

    StringBuffer **stringbuffers;
    //text->number
    Root **dictionaries;
    //number->text
    Root **invDictionaries;
    int dictPartitions;
    bool dictHash;

    long totalNumberTriples;
    long totalNumberTerms;

    int nindices;
    bool aggrIndices;
    bool incompleteIndices;

    bool useFixedStrategy;
    char storageFixedStrategy;

    bool dictEnabled;

    double sampleRate;

    //boost::thread *dictLookupThread;
    DictMgmt *dictManager;

    TableStorage *files[N_PARTITIONS];
    MemoryManager<FileSegment> *bytesTracker[N_PARTITIONS];

    CacheIdx *pso;
    CacheIdx *osp;

    KB *sampleKB;

    void loadDict(int id, KBConfig *config);

public:
    KB(const char *path, bool readOnly, bool reasoning, bool dictEnabled, KBConfig &config);

    Querier *query();

    Inserter *insert();

    DictMgmt *getDictMgmt() {
        return dictManager;
    }

    void closeDict();

    void closeInvDict();

    void closeStringBuffers();

    Stats getStats();

    Stats *getStatsDict();

    string getDictPath(int i);

    string getPath() {
        return path;
    }

    int getNIndices() const {
        return nindices;
    }

    uint64_t getNTerms() const {
        return totalNumberTerms;
    }

    int getNDictionaries() {
        return dictPartitions;
    }

    double getSampleRate() {
        return sampleRate;
    }

    long getSize() {
        return totalNumberTriples;
    }

    TreeItr *getItrTerms();

    ~KB();
};

#endif /* KBB_H_ */
