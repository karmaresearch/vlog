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

#include <trident/kb/memoryopt.h>
#include <trident/kb/kb.h>
#include <trident/kb/querier.h>
#include <trident/kb/inserter.h>
#include <trident/kb/consts.h>
#include <trident/kb/kbconfig.h>
#include <trident/tree/root.h>
#include <trident/tree/stringbuffer.h>
#include <trident/storage/pairstorage.h>

#include <boost/log/trivial.hpp>

#include <string>
#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <cmath>

using namespace std;

KB::KB(const char *path, bool readOnly, bool reasoning, bool dictEnabled, KBConfig &config) :
    path(path), readOnly(readOnly), dictEnabled(dictEnabled) {
    timens::system_clock::time_point start = timens::system_clock::now();

    //Get some statistics
    string fileConf = path + string("/kbstats");
    if (fs::exists(fs::path(fileConf))) {
        std::ifstream fis;
        fis.open(fileConf);
        char data[8];
        fis.read(data, 8);
        dictPartitions = (int)Utils::decode_long(data, 0);
        fis.read(data, 8);
        totalNumberTerms = Utils::decode_long(data, 0);
        fis.read(data, 8);
        totalNumberTriples = Utils::decode_long(data, 0);

        fis.read(data, 4);
        nindices = Utils::decode_int(data, 0);
        fis.read(data, 1);
        if (data[0]) {
            aggrIndices = true;
        } else {
            aggrIndices = false;
        }
        incompleteIndices = fis.get() != 0;
        dictHash = fis.get() != 0;
        fis.close();
    } else {
        dictPartitions = config.getParamInt(DICTPARTITIONS);
        dictHash = config.getParamBool(DICTHASH);
        totalNumberTerms = 0;
        totalNumberTriples = 0;
        nindices = config.getParamInt(NINDICES);
        aggrIndices = config.getParamBool(AGGRINDICES);
        incompleteIndices = config.getParamBool(INCOMPLINDICES);
        useFixedStrategy = config.getParamBool(USEFIXEDSTRAT);
        storageFixedStrategy = (char) config.getParamInt(FIXEDSTRAT);
    }

    //Optimize the memory management
    if (reasoning) {
        MemoryOptimizer::optimizeForReasoning(dictPartitions, config);
    } else if (readOnly) {
        MemoryOptimizer::optimizeForReading(dictPartitions, config);
    }

    //Initialize the tree
    string fileTree = path + string("/tree/");
    PropertyMap map;
    map.setBool(TEXT_KEYS, false);
    map.setBool(TEXT_VALUES, false);
    map.setBool(COMPRESSED_NODES, false);
    map.setInt(LEAF_SIZE_PREALL_FACTORY,
               config.getParamInt(TREE_MAXPREALLLEAVESCACHE));
    map.setInt(LEAF_SIZE_FACTORY, config.getParamInt(TREE_MAXLEAVESCACHE));
    map.setInt(MAX_NODES_IN_CACHE, config.getParamInt(TREE_MAXNODESINCACHE));
    map.setInt(NODE_MIN_BYTES, config.getParamInt(TREE_NODEMINBYTES));
    map.setLong(CACHE_MAX_SIZE, config.getParamLong(TREE_MAXSIZECACHETREE));
    map.setInt(FILE_MAX_SIZE, config.getParamInt(TREE_MAXFILESIZE));
    map.setInt(MAX_N_OPENED_FILES, config.getParamInt(TREE_MAXNFILES));
    map.setInt(MAX_EL_PER_NODE, config.getParamInt(TREE_MAXELEMENTSNODE));

    map.setInt(LEAF_MAX_PREALL_INTERNAL_LINES,
               config.getParamInt(TREE_MAXPREALLINTERNALLINES));
    map.setInt(LEAF_MAX_INTERNAL_LINES,
               config.getParamInt(TREE_MAXINTERNALLINES));
    map.setInt(LEAF_ARRAYS_FACTORY_SIZE, config.getParamInt(TREE_FACTORYSIZE));
    map.setInt(LEAF_ARRAYS_PREALL_FACTORY_SIZE,
               config.getParamInt(TREE_ALLOCATEDELEMENTS));

    map.setInt(NODE_KEYS_FACTORY_SIZE,
               config.getParamInt(TREE_NODE_KEYS_FACTORY_SIZE));
    map.setInt(NODE_KEYS_PREALL_FACTORY_SIZE,
               config.getParamInt(TREE_NODE_KEYS_PREALL_FACTORY_SIZE));

    tree = new Root(fileTree, NULL, readOnly, map);

    boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
                                          - start;
    BOOST_LOG_TRIVIAL(debug) << "Time init tree KB = " << sec.count() * 1000 << " ms and " << Utils::get_max_mem() << " MB occupied";

    //Initialize the dictionaries
    dictionaries = new Root*[dictPartitions];
    statsStrings = new Stats[dictPartitions];
    stringbuffers = new StringBuffer*[dictPartitions];
    invDictionaries = new Root*[dictPartitions];

    if (dictEnabled) {
        boost::thread *threads = new boost::thread[dictPartitions - 1];
        for (int i = 1; i < dictPartitions; ++i) {
            threads[i - 1] = boost::thread(
                                 boost::bind(&KB::loadDict, this, i, &config));
        }
        loadDict(0, &config);
        for (int i = 1; i < dictPartitions; ++i) {
            threads[i - 1].join();
        }
        delete[] threads;
        sec = boost::chrono::system_clock::now() - start;
        BOOST_LOG_TRIVIAL(debug) << "Time init dictionaries KB = " << sec.count() * 1000 << " ms and " << Utils::get_max_mem() << " MB occupied";

        //Start thread for lookup of dictionary terms
        dictManager = new DictMgmt(stringbuffers, dictionaries, invDictionaries,
                                   dictPartitions, dictHash);
        //dictLookupThread = new boost::thread(
        //    boost::bind(&DictMgmt::run, dictManager));
    }

    //Initialize the memory tracker for the storage partitions
    bytesTracker[0] = new MemoryManager<FileSegment>(
        config.getParamLong(STORAGE_CACHE_SIZE));
    for (int i = 1; i < nindices; ++i) {
        if (!readOnly) {
            bytesTracker[i] = new MemoryManager<FileSegment>(
                config.getParamLong(STORAGE_CACHE_SIZE));
        } else {
            bytesTracker[i] = NULL;
        }
    }

    if (nindices == 3) {
        pso = new CacheIdx();
        osp = new CacheIdx();
    } else {
        pso = NULL;
        osp = NULL;
    }

    //Initialize the storage partitions
    for (int i = 0; i < nindices; ++i) {
        stringstream is;
        is << path << "/p" << i;
        if (readOnly) {
            files[i] = new TableStorage(readOnly, is.str(),
                                        config.getParamInt(STORAGE_MAX_FILE_SIZE),
                                        config.getParamInt(STORAGE_MAX_N_FILES), bytesTracker[0], stats);
        } else {
            files[i] = new TableStorage(readOnly, is.str(),
                                        config.getParamInt(STORAGE_MAX_FILE_SIZE),
                                        config.getParamInt(STORAGE_MAX_N_FILES), bytesTracker[i], stats);
        }
    }

    //Is there some sample data available?
    string sampleDir = path + string("/_sample");
    if (fs::exists(fs::path(sampleDir))) {
        KBConfig sampleConfig;
        sampleKB = new KB(sampleDir.c_str(), true, false, false, sampleConfig);
        sampleRate = (double) sampleKB->getSize() / this->totalNumberTriples;
    } else {
        sampleKB = NULL;
        sampleRate = 0;
    }

    sec = boost::chrono::system_clock::now() - start;
    BOOST_LOG_TRIVIAL(info) << "Time init KB = " << sec.count() * 1000 << " ms and " << Utils::get_max_mem() << " MB occupied";
}

void KB::loadDict(int id, KBConfig *config) {

    PropertyMap map;
    map.setBool(TEXT_KEYS, true);
    map.setBool(TEXT_VALUES, false);
    map.setBool(COMPRESSED_NODES, false);
    map.setInt(LEAF_SIZE_PREALL_FACTORY,
               config->getParamInt(DICT_MAXPREALLLEAVESCACHE));
    map.setInt(LEAF_SIZE_FACTORY, config->getParamInt(DICT_MAXLEAVESCACHE));
    map.setInt(MAX_NODES_IN_CACHE, config->getParamInt(DICT_MAXNODESINCACHE));
    map.setInt(NODE_MIN_BYTES, config->getParamInt(DICT_NODEMINBYTES));
    map.setLong(CACHE_MAX_SIZE, config->getParamLong(DICT_MAXSIZECACHETREE));
    map.setInt(FILE_MAX_SIZE, config->getParamInt(DICT_MAXFILESIZE));
    map.setInt(MAX_N_OPENED_FILES, config->getParamInt(DICT_MAXNFILES));
    map.setInt(MAX_EL_PER_NODE, config->getParamInt(DICT_MAXELEMENTSNODE));
    map.setInt(NODE_KEYS_FACTORY_SIZE,
               config->getParamInt(DICT_NODE_KEYS_FACTORY_SIZE));
    map.setInt(NODE_KEYS_PREALL_FACTORY_SIZE,
               config->getParamInt(DICT_NODE_KEYS_PREALL_FACTORY_SIZE));

    stringstream ss1;
    ss1 << path << "/dict/" << id;
    stringbuffers[id] = new StringBuffer(ss1.str(), readOnly,
                                         config->getParamInt(SB_PREALLBUFFERS),
                                         config->getParamLong(SB_CACHESIZE),
                                         statsStrings + id);
    dictionaries[id] = new Root(ss1.str(), stringbuffers[id], readOnly, map);

    //Initialize the inverse dictionaries
    map.setBool(TEXT_KEYS, false);
    map.setBool(TEXT_VALUES, true);
    map.setBool(COMPRESSED_NODES, false);
    map.setInt(LEAF_SIZE_PREALL_FACTORY,
               config->getParamInt(INVDICT_MAXPREALLLEAVESCACHE));
    map.setInt(LEAF_SIZE_FACTORY, config->getParamInt(INVDICT_MAXLEAVESCACHE));
    map.setInt(MAX_NODES_IN_CACHE,
               config->getParamInt(INVDICT_MAXNODESINCACHE));
    map.setInt(NODE_MIN_BYTES, config->getParamInt(INVDICT_NODEMINBYTES));
    map.setLong(CACHE_MAX_SIZE, config->getParamLong(INVDICT_MAXSIZECACHETREE));
    map.setInt(FILE_MAX_SIZE, config->getParamInt(INVDICT_MAXFILESIZE));
    map.setInt(MAX_N_OPENED_FILES, config->getParamInt(INVDICT_MAXNFILES));
    map.setInt(MAX_EL_PER_NODE, config->getParamInt(INVDICT_MAXELEMENTSNODE));
    map.setInt(NODE_KEYS_FACTORY_SIZE,
               config->getParamInt(INVDICT_NODE_KEYS_FACTORY_SIZE));
    map.setInt(NODE_KEYS_PREALL_FACTORY_SIZE,
               config->getParamInt(INVDICT_NODE_KEYS_PREALL_FACTORY_SIZE));

    stringstream ss2;
    ss2 << path << "/invdict/" << id;
    invDictionaries[id] = new Root(ss2.str(), NULL, readOnly, map);
}

Querier *KB::query() {
    return new Querier(tree, dictManager, files, totalNumberTriples, totalNumberTerms, nindices,
                       /*aggrIndices,*/ pso, sampleKB);
}

Inserter *KB::insert() {
    if (readOnly) {
        BOOST_LOG_TRIVIAL(error) << "Insert() is not available if the knowledge base is opened in read_only mode.";
    }

    return new Inserter(tree, files, totalNumberTerms + (dictEnabled ? dictManager->getNTermsInserted() : 0),
                        useFixedStrategy, storageFixedStrategy);
}

void KB::closeDict() {
    for (int i = 0; i < dictPartitions; ++i) {
        if (dictionaries[i] != NULL) {
            delete dictionaries[i];
            dictionaries[i] = NULL;
        }
    }
}

void KB::closeInvDict() {
    for (int i = 0; i < dictPartitions; ++i) {
        if (invDictionaries[i] != NULL) {
            delete invDictionaries[i];
            invDictionaries[i] = NULL;
        }
    }
}

void KB::closeStringBuffers() {
    for (int i = 0; i < dictPartitions; ++i) {
        if (stringbuffers[i] != NULL) {
            delete stringbuffers[i];
            stringbuffers[i] = NULL;
        }
    }
}

string KB::getDictPath(int i) {
    stringstream ss1;
    ss1 << path << "/dict/" << i;
    return ss1.str();
}

TreeItr *KB::getItrTerms() {
    return tree->itr();
}

Stats KB::getStats() {
    return stats;
}

Stats *KB::getStatsDict() {
    return statsStrings;
}

KB::~KB() {
    //Update stats about the KB
    if (!readOnly) {
        if (dictEnabled)
            totalNumberTerms += dictManager->getNTermsInserted();
        totalNumberTriples += files[0]->getNTriplesInserted();
    }

    if (dictEnabled) {
        //dictLookupThread->interrupt();
        //dictLookupThread->join();

        closeDict();
        closeInvDict();
        closeStringBuffers();
    }

    BOOST_LOG_TRIVIAL(debug) << "Delete array dictionaries";
    delete[] dictionaries;
    delete[] invDictionaries;
    delete[] stringbuffers;
    delete[] statsStrings;

    BOOST_LOG_TRIVIAL(debug) << "Delete tree";
    delete tree;

    if (dictEnabled) {
        //delete dictLookupThread;
        dictManager->clean();
        BOOST_LOG_TRIVIAL(debug) << "Delete dictmanager";
        delete dictManager;
    }

    for (int i = 0; i < nindices; ++i) {
        if (files[i] != NULL) {
            BOOST_LOG_TRIVIAL(debug) << "Delete storage " << i;
            delete files[i];
        }
    }

    // Delete bytesTrackers after deleting all files, because
    // in read-only case, all files share the same bytesTracker.
    for (int i = 0; i < nindices; ++i) {
        if (bytesTracker[i] != NULL) {
            delete bytesTracker[i];
        }
    }

    if (sampleKB != NULL) {
        BOOST_LOG_TRIVIAL(debug) << "Delete sample KB";
        delete sampleKB;
    }

    if (!readOnly) {
        //Write a file with some statistics
        std::ofstream fos;
        fos.open(this->path + string("/kbstats"));
        char data[8];
        //Write the number of dictionaries
        Utils::encode_long(data, 0, dictPartitions);
        fos.write(data, 8);

        //Write the total number of terms
        Utils::encode_long(data, 0, totalNumberTerms);
        fos.write(data, 8);

        //Write the total number of triples
        Utils::encode_long(data, 0, totalNumberTriples);
        fos.write(data, 8);

        //Write number indices
        Utils::encode_int(data, 0, nindices);
        fos.write(data, 4);

        //Write aggregated indices
        data[0] = aggrIndices ? 1 : 0;
        fos.write(data, 1);

        //Write whether the indices are complete or not
        fos.put(incompleteIndices);

        fos.put(dictHash);

        fos.close();
    }

    if (pso != NULL) {
        delete pso;
    }

    if (osp != NULL) {
        delete osp;
    }
}
