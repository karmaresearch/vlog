#include <trident/loader.h>
#include <trident/kb/memoryopt.h>
#include <trident/kb/kb.h>
#include <trident/kb/schema.h>

#include <trident/tree/nodemanager.h>

#include <tridentcompr/utils/lz4io.h>
#include <tridentcompr/utils/utils.h>
#include <tridentcompr/utils/triplewriters.h>
#include <tridentcompr/compression/compressor.h>
#include <tridentcompr/sorting/sorter.h>
#include <tridentcompr/sorting/filemerger.h>

#include <boost/filesystem.hpp>
#include <boost/timer.hpp>
#include <boost/log/trivial.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/gzip.hpp>

#include <sstream>
#include <limits>
#include <fstream>
#include <iostream>
#include <cstdio>
#include <cstdlib>

namespace fs = boost::filesystem;

void Loader::sortAndInsert(int permutation, int nindices, bool inputSorted,
                           string inputDir,
                           string *POSoutputDir, TreeWriter *treeWriter,
                           Inserter *ins, const bool aggregated,
                           const bool canSkipTables, const bool storeRaw,
                           SimpleTripleWriter *sampleWriter, double sampleRate) {
    SimpleTripleWriter *posWriter = NULL;
    if (POSoutputDir != NULL) {
        posWriter = new SimpleTripleWriter(*POSoutputDir, "inputAggr-" +
                                           to_string(permutation), true);
    }

    //Calculate number of threads
    int nprocs = Utils::getNumberPhysicalCores();
    int threadsPerPerm = max(1, nprocs / nindices);
    BOOST_LOG_TRIVIAL(debug) << "Sorting threads per partition: " << threadsPerPerm;

    //Sort the triples and store them into files.
    BOOST_LOG_TRIVIAL(debug) << "Start sorting...";
    Sorter::mergeSort(inputDir, threadsPerPerm, !inputSorted, SORTING_BLOCK_SIZE, 16);
    BOOST_LOG_TRIVIAL(debug) << "...completed.";

    BOOST_LOG_TRIVIAL(debug) << "Start inserting...";
    long ps, pp, po; //Previous values. Used to remove duplicates.
    ps = pp = po = -1;
    long count = 0;
    long countInput = 0;
    const int randThreshold = (int)(sampleRate * RAND_MAX);
    assert(sampleWriter == NULL || randThreshold > 0);
    FileMerger<Triple> merger(Utils::getFiles(inputDir));
    LZ4Writer *plainWriter = NULL;
    if (storeRaw) {
        std::string file = ins->getPathPermutationStorage(permutation) + std::string("raw");
        plainWriter = new LZ4Writer(file);
    }
    bool first = true;
    while (!merger.isEmpty()) {
        Triple t = merger.get();
        countInput++;
        if (count % 10000000 == 0) {
            BOOST_LOG_TRIVIAL(debug) << "..." << count << "...";
        }

        if (t.o != po || t.p != pp || t.s != ps) {
            count++;
            ins->insert(permutation, t.s, t.p, t.o, t.count, posWriter,
                        treeWriter,
                        aggregated,
                        canSkipTables);
            ps = t.s;
            pp = t.p;
            po = t.o;

            if (storeRaw) {
                plainWriter->writeVLong(t.s);
                plainWriter->writeVLong(t.p);
                plainWriter->writeVLong(t.o);
            }

            if (sampleWriter != NULL) {
                if (first || rand() < randThreshold) {
                    sampleWriter->write(t.s, t.p, t.o);
                    first = false;
                }
            }
        }
    }

    ins->flush(permutation, posWriter, treeWriter, aggregated, canSkipTables);

    if (plainWriter != NULL) {
        delete plainWriter;
    }

    if (printStats) {
        Statistics *stat = ins->getStats(permutation);
        if (stat != NULL) {
            BOOST_LOG_TRIVIAL(debug) << "Perm " << permutation << ": RowLayout" << stat->nListStrategies << " ClusterLayout " << stat->nGroupStrategies << " ColumnLayout " << stat->nList2Strategies;
            BOOST_LOG_TRIVIAL(debug) << "Perm " << permutation << ": Exact " << stat->exact << " Approx " << stat->approximate;
            BOOST_LOG_TRIVIAL(debug) << "Perm " << permutation << ": FirstElemCompr1 " << stat->nFirstCompr1 << " FirstElemCompr2 " << stat->nFirstCompr2;
            BOOST_LOG_TRIVIAL(debug) << "Perm " << permutation << ": SecondElemCompr1 " << stat->nSecondCompr1 << " SecondElemCompr2 " << stat->nSecondCompr2;
            BOOST_LOG_TRIVIAL(debug) << "Perm " << permutation << ": Diff " << stat->diff << " Nodiff " << stat->nodiff;
            BOOST_LOG_TRIVIAL(debug) << "Perm " << permutation << ": Aggregated " << stat->aggregated << " NotAggr " << stat->notAggregated;
            BOOST_LOG_TRIVIAL(debug) << "Perm " << permutation << ": NTables " << ins->getNTablesPerPartition(permutation);
            BOOST_LOG_TRIVIAL(debug) << "Perm " << permutation << ": NSkippedTables " << ins->getNSkippedTables(permutation);
        }
    }

    if (POSoutputDir != NULL) {
        delete posWriter;
    }

    //Remove the files
    fs::remove_all(fs::path(inputDir));

    BOOST_LOG_TRIVIAL(debug) << "...completed. Added " << count << " triples out of " << countInput;
}

void Loader::insertDictionary(const int part, DictMgmt *dict, string
                              dictFileInput, bool insertDictionary, bool insertInverseDictionary,
                              bool storeNumbersCoordinates, nTerm *maxValueCounter) {
    //Read the file
    LZ4Reader in(dictFileInput);

    LZ4Writer *tmpWriter = NULL;
    if (storeNumbersCoordinates) {
        tmpWriter = new LZ4Writer(dictFileInput + ".tmp");
    }

    nTerm maxValue = 0;
    while (!in.isEof()) {
        nTerm key = in.parseLong();
        if (key > maxValue) {
            maxValue = key;
        }
        int size;
        const char *value = in.parseString(size);

        bool resp;
        if (insertDictionary && insertInverseDictionary) {
            if (!storeNumbersCoordinates) {
                resp = dict->putPair(part, value + 2, size - 2, key);
            } else {
                //In this case I only insert the "dict" entries
                long coordinates;
                resp = dict->putDict(part, value + 2, size - 2, key, coordinates);
                tmpWriter->writeLong(key);
                tmpWriter->writeLong(coordinates);
            }
        } else if (insertDictionary) {
            resp = dict->putDict(part, value + 2, size - 2, key);
        } else {
            resp = dict->putInvDict(part, value + 2, size - 2, key);
        }

        if (!resp) {
            BOOST_LOG_TRIVIAL(error) << "This should not happen. Term " <<
                                     string(value + 2, size - 2) << "-" << key << " is already inserted.";
        }
    }
    *maxValueCounter = maxValue;

    //Sort the pairs <term,coordinates> by term and add them into the invdict
    //dictionaries
    if (storeNumbersCoordinates) {
        delete tmpWriter;

        //Sort the files
        vector<string> inputFiles;
        inputFiles.push_back(dictFileInput + ".tmp");
        vector<string> files = Sorter::sortFiles<PairLong>(inputFiles,
                               dictFileInput + ".tmp");

        if (files.size() > 1) {
            FileMerger<PairLong> merger(files);
            while (!merger.isEmpty()) {
                PairLong el = merger.get();
                dict->putInvDict(part, el.n1, el.n2);
            }
        } else {
            LZ4Reader reader(files[0]);
            while (!reader.isEof()) {
                PairLong p;
                p.readFrom(&reader);
                dict->putInvDict(part, p.n1, p.n2);
            }
        }

        //Delete the temporary files
        for (vector<string>::iterator itr = files.begin(); itr != files.end();
                ++itr) {
            fs::remove(*itr);
        }

        //Delete the input file
        fs::remove(dictFileInput + ".tmp");
    }
}

void Loader::addSchemaTerms(const int dictPartitions, nTerm highestNumber, DictMgmt *dict) {
    if (dictPartitions != 1) {
        BOOST_LOG_TRIVIAL(error) << "The addition of schema terms is supported only the dictionary is stored on one partition";
        throw 10;
    }
    vector<string> schemaTerms = Schema::getAllSchemaTerms();
    for (vector<string>::iterator itr = schemaTerms.begin(); itr != schemaTerms.end(); itr++) {
        nTerm key;
        if (!dict->getNumber(itr->c_str(), itr->size(), &key)) {
            //Add it in the dictionary
            BOOST_LOG_TRIVIAL(debug) << "Add in the dictionary the entry " << *itr << " with number " << (highestNumber + 1);
            dict->putPair(itr->c_str(), itr->size(), ++highestNumber);
        } else {
            BOOST_LOG_TRIVIAL(debug) << "The schema entry " << *itr << " was already in the input with the id " << key;
        }
    }
}

void Loader::load(bool onlyCompress, string triplesInputDir, string kbDir,
                  string dictMethod, int sampleMethod, int sampleArg,
                  int parallelThreads, int maxReadingThreads, int dictionaries,
                  int nindices, bool aggrIndices, const bool canSkipTables,
                  bool enableFixedStrat, int fixedStrat,
                  bool storePlainList, bool sample, double sampleRate,
                  int thresholdSkipTable) {

    //Check if kbDir exists
    if (!fs::exists(fs::path(kbDir))) {
        fs::create_directories(fs::path(kbDir));
    }

    //How many to use dictionaries?
    int ncores = Utils::getNumberPhysicalCores();
    if (parallelThreads > ncores) {
        BOOST_LOG_TRIVIAL(warning) << "The parallelThreads parameter is above the number pf physical cores. I set it to " << ncores;
        parallelThreads = ncores;
    }

    if (maxReadingThreads > parallelThreads) {
        BOOST_LOG_TRIVIAL(warning) << "I cannot read with more threads than the available ones. I set it to = " << parallelThreads;
        maxReadingThreads = parallelThreads;
    }

    if (dictionaries > parallelThreads) {
        BOOST_LOG_TRIVIAL(warning) << "The dictionary partitions cannot be higher than the maximum number of threads. I set it to = " << parallelThreads;
        dictionaries = parallelThreads;
    }

    BOOST_LOG_TRIVIAL(debug) << "Set number of dictionaries to " << dictionaries << " parallel threads=" << parallelThreads << " readingThreads=" << maxReadingThreads;

    //Create data structures to compress the input
    int nperms = 1;
    int signaturePerm = 0;
    if (nindices == 3) {
        if (aggrIndices) {
            nperms = 2;
            Compressor::addPermutation(IDX_SPO, signaturePerm);
            Compressor::addPermutation(IDX_OPS, signaturePerm);
        } else {
            nperms = 3;
            Compressor::addPermutation(IDX_SPO, signaturePerm);
            Compressor::addPermutation(IDX_OPS, signaturePerm);
            Compressor::addPermutation(IDX_POS, signaturePerm);
        }
    } else if (nindices == 4) {
        if (aggrIndices) {
            nperms = 2;
            Compressor::addPermutation(IDX_SPO, signaturePerm);
            Compressor::addPermutation(IDX_OPS, signaturePerm);
        } else {
            nperms = 4;
            Compressor::addPermutation(IDX_SPO, signaturePerm);
            Compressor::addPermutation(IDX_OPS, signaturePerm);
            Compressor::addPermutation(IDX_POS, signaturePerm);
            Compressor::addPermutation(IDX_PSO, signaturePerm);
        }
    } else if (nindices == 6) {
        if (aggrIndices) {
            nperms = 4;
            Compressor::addPermutation(IDX_SPO, signaturePerm);
            Compressor::addPermutation(IDX_OPS, signaturePerm);
            Compressor::addPermutation(IDX_SOP, signaturePerm);
            Compressor::addPermutation(IDX_OSP, signaturePerm);
        } else {
            nperms = 6;
            Compressor::addPermutation(IDX_SPO, signaturePerm);
            Compressor::addPermutation(IDX_OPS, signaturePerm);
            Compressor::addPermutation(IDX_POS, signaturePerm);
            Compressor::addPermutation(IDX_SOP, signaturePerm);
            Compressor::addPermutation(IDX_OSP, signaturePerm);
            Compressor::addPermutation(IDX_PSO, signaturePerm);
        }
    }
    string *permDirs = new string[nperms];
    for (int i = 0; i < nperms; ++i) {
        permDirs[i] = kbDir + string("/permtmp-") + to_string(i);
        fs::create_directories(fs::path(permDirs[i]));
    }
    string *fileNameDictionaries = new string[dictionaries];
    for (int i = 0; i < dictionaries; ++i) {
        fileNameDictionaries[i] = kbDir + string("/dict-") + to_string(i);
    }

    long totalCount = 0;
    Compressor comp(triplesInputDir, kbDir);
    //Parse the input
    comp.parse(dictionaries, sampleMethod, sampleArg, (int)(sampleRate * 100),
               parallelThreads, maxReadingThreads, false, false);
    //Compress it
    comp.compress(permDirs, nperms, signaturePerm, fileNameDictionaries,
                  dictionaries, parallelThreads);
    totalCount = comp.getTotalCount();


    KBConfig config;
    config.setParamInt(DICTPARTITIONS, dictionaries);
    config.setParamInt(NINDICES, nindices);
    config.setParamBool(AGGRINDICES, aggrIndices);
    config.setParamBool(USEFIXEDSTRAT, enableFixedStrat);
    config.setParamInt(FIXEDSTRAT, fixedStrat);
    config.setParamInt(THRESHOLD_SKIP_TABLE, thresholdSkipTable);
    MemoryOptimizer::optimizeForWriting(totalCount, config);
    if (dictMethod == DICT_HASH) {
        config.setParamBool(DICTHASH, true);
    }
    KB kb(kbDir.c_str(), false, false, true, config);

    loadKB(kb, kbDir, true, dictionaries, dictMethod, nindices, aggrIndices,
           canSkipTables, sample,
           sampleRate, storePlainList, fileNameDictionaries, nperms,
           signaturePerm, permDirs,
           totalCount);

    delete[] permDirs;
    delete[] fileNameDictionaries;
}

void Loader::loadKB(KB &kb, string kbDir, bool storeDicts, int dictionaries,
                    string dictMethod,
                    int nindices, bool aggrIndices, bool canSkipTables,
                    bool sample,
                    double sampleRate, bool storePlainList,
                    string *fileNameDictionaries, int nperms,
                    int signaturePerms,
                    string *permDirs, long totalCount) {

    boost::thread *threads;
    if (storeDicts) {
        BOOST_LOG_TRIVIAL(debug) << "Insert the dictionary in the trees";
        threads = new boost::thread[dictionaries - 1];
        nTerm *maxValues = new nTerm[dictionaries];
        if (dictMethod != DICT_SMART) {
            for (int i = 1; i < dictionaries; ++i) {
                threads[i - 1] = boost::thread(
                                     boost::bind(&Loader::insertDictionary, this, i,
                                                 kb.getDictMgmt(), fileNameDictionaries[i],
                                                 dictMethod != DICT_HASH, true, false, maxValues + i));
            }
            insertDictionary(0, kb.getDictMgmt(), fileNameDictionaries[0],
                             dictMethod != DICT_HASH, true, false, maxValues);
            for (int i = 1; i < dictionaries; ++i) {
                threads[i - 1].join();
            }
        } else {
            insertDictionary(0, kb.getDictMgmt(), fileNameDictionaries[0], true,
                             true, true, maxValues);
        }

        for (int i = 0; i < dictionaries; ++i) {
            fs::remove(fs::path(fileNameDictionaries[i]));
        }

        /*** If reasoning is activated, add all schema terms that are needed
         * during reasoning ***/

#ifdef REASONING
        addSchemaTerms(dictionaries, maxValues[0], kb.getDictMgmt());
#endif
        delete[] maxValues;
        delete[] threads;

        /*** Close the dictionaries ***/
        BOOST_LOG_TRIVIAL(debug) << "Closing dict...";
        kb.closeDict();
        BOOST_LOG_TRIVIAL(debug) << "Closing inv dict...";
        kb.closeInvDict();
        BOOST_LOG_TRIVIAL(debug) << "Closing string buffers...";
        kb.closeStringBuffers();
    }

    BOOST_LOG_TRIVIAL(debug) << "Insert the triples in the indices...";
    string *sTreeWriters = new string[nindices];
    TreeWriter **treeWriters = new TreeWriter*[nindices];
    for (int i = 0; i < nindices; ++i) {
        sTreeWriters[i] = kbDir + string("/tmpTree" ) + to_string(i);
        treeWriters[i] = new TreeWriter(sTreeWriters[i]);
    }

    //Use aggregated indices
    string aggr1Dir = kbDir + string("/aggr1");
    string aggr2Dir = kbDir + string("/aggr2");
    if (aggrIndices && nindices > 1) {
        fs::create_directories(fs::path(aggr1Dir));
        if (nindices > 3)
            fs::create_directories(fs::path(aggr2Dir));
    }

    //if sample is requested, create a subdir
    string sampleDir = kbDir + string("/sampledir");
    SimpleTripleWriter *sampleWriter = NULL;
    if (sample) {
        fs::create_directories(fs::path(sampleDir));
        sampleWriter = new SimpleTripleWriter(sampleDir, "input", false);
    }

    //Create n threads where the triples are sorted and inserted in the knowledge base
    Inserter *ins = kb.insert();
    if (nindices == 1) {
        sortAndInsert(0, nperms, false, permDirs[0], NULL, treeWriters[0], ins,
                      false, false, storePlainList, sampleWriter, sampleRate);
    } else if (nindices == 3 || nindices == 4) {
        boost::thread t = boost::thread(
                              boost::bind(&Loader::sortAndInsert, this,
                                          1, nperms, permDirs[1], aggrIndices ?
                                          &aggr1Dir : NULL,
                                          treeWriters[1], ins, false,
                                          false));
        boost::thread t2;
        boost::thread t3;
        if (!aggrIndices) {
            t2 = boost::thread(
                     boost::bind(&Loader::sortAndInsert, this,
                                 2, nperms, permDirs[2],
                                 (string*)NULL,
                                 treeWriters[2], ins, false, false));
            if (nindices == 4) {
                t3 = boost::thread(
                         boost::bind(&Loader::sortAndInsert, this,
                                     3, nperms, permDirs[3],
                                     (string*)NULL,
                                     treeWriters[3], ins, false, canSkipTables));
            }

        }
        sortAndInsert(0, nperms, false, permDirs[0], (nindices == 4 && aggrIndices) ?
                      &aggr2Dir : NULL, treeWriters[0], ins,
                      false, false, storePlainList, sampleWriter, sampleRate);
        t.join();
        t2.join();

        if (aggrIndices) {
            if (nindices == 4) {
                t2 = boost::thread(
                         boost::bind(&Loader::sortAndInsert, this,
                                     3, nperms, aggr2Dir,
                                     (string*)NULL,
                                     treeWriters[3], ins, true, canSkipTables));
            }
            sortAndInsert(2, nperms, aggr1Dir, NULL, treeWriters[2], ins, true,
                          false);
            if (nindices == 4) {
                t2.join();
            }
        }
    } else { //nindices = 6
        boost::thread ts[3];
        boost::thread at1, at2;
        if (!aggrIndices) {
            ts[0] = boost::thread(
                        boost::bind(&Loader::sortAndInsert, this,
                                    1, nperms, permDirs[1], (string*) NULL,
                                    treeWriters[1], ins, false,
                                    false));
            ts[1] = boost::thread(
                        boost::bind(&Loader::sortAndInsert, this,
                                    3, nperms, permDirs[3], (string*)NULL,
                                    treeWriters[3], ins, false,
                                    canSkipTables));
            ts[2] = boost::thread(
                        boost::bind(&Loader::sortAndInsert, this,
                                    4, nperms, permDirs[4], (string*)NULL,
                                    treeWriters[4], ins, false,
                                    canSkipTables));

            //Start two more threads
            at1 = boost::thread(boost::bind(&Loader::sortAndInsert, this, 2, nperms,
                                            permDirs[2], (string*) NULL,
                                            treeWriters[2], ins, false,
                                            false));
            at2 = boost::thread(boost::bind(&Loader::sortAndInsert, this, 5, nperms,
                                            permDirs[5], (string*) NULL,
                                            treeWriters[5], ins, false,
                                            canSkipTables));

        } else {
            ts[0] = boost::thread(
                        boost::bind(&Loader::sortAndInsert, this,
                                    1, nperms, permDirs[1], &aggr1Dir,
                                    treeWriters[1], ins, false,
                                    false));
            ts[1] = boost::thread(
                        boost::bind(&Loader::sortAndInsert, this,
                                    3, nperms, permDirs[2], (string*)NULL,
                                    treeWriters[3], ins, false,
                                    canSkipTables));
            ts[2] = boost::thread(
                        boost::bind(&Loader::sortAndInsert, this,
                                    4, nperms, permDirs[3], (string*)NULL,
                                    treeWriters[4], ins, false,
                                    canSkipTables));

        }

        sortAndInsert(0, nperms, false, permDirs[0], aggrIndices ? &aggr2Dir : NULL,
                      treeWriters[0], ins, false, false, storePlainList,
                      sampleWriter, sampleRate);
        for (int i = 0; i < 3; ++i) {
            ts[i].join();
        }
        at1.join();
        at2.join();

        //Aggregated
        if (aggrIndices) {
            boost::thread t[2];
            t[0] = boost::thread(boost::bind(&Loader::sortAndInsert, this, 2, nperms,
                                             aggr1Dir, (string*) NULL,
                                             treeWriters[2], ins, true, false));
            t[1] = boost::thread(boost::bind(&Loader::sortAndInsert, this, 5, nperms,
                                             aggr2Dir, (string*) NULL,
                                             treeWriters[5], ins, true, canSkipTables));
            t[0].join();
            t[1].join();
        }
    }

    for (int i = 0; i < nindices; ++i) {
        treeWriters[i]->finish();
    }

    threads = new boost::thread[dictionaries + 1];
    BOOST_LOG_TRIVIAL(debug) << "Compress the dictionary nodes...";
    for (int i = 0; i < dictionaries && storeDicts; ++i) {
        threads[i + 1] = boost::thread(
                             boost::bind(&NodeManager::compressSpace,
                                         kb.getDictPath(i)));
    }

    BOOST_LOG_TRIVIAL(debug) << "Start creating the tree...";
    bufferToFill = bufferToReturn = &buffer1;
    isFinished = false;
    buffersReady = 0;
    threads[0] = boost::thread(
                     boost::bind(&Loader::mergeTermCoordinates, this,
                                 sTreeWriters, nindices));
    processTermCoordinates(ins);
    for (int i = 0; i < (storeDicts ? dictionaries : 0) + 1; ++i) {
        threads[i].join();
    }

    for (int i = 0; i < nindices; ++i) {
        fs::remove(fs::path(sTreeWriters[i]));
        delete treeWriters[i];
    }
    delete[] sTreeWriters;
    delete[] treeWriters;

    delete ins;
    delete[] threads;

    if (sample) {
        BOOST_LOG_TRIVIAL(debug) << "Creating a sample dataset";
        delete sampleWriter;
        string sampleKB = kbDir + string("/_sample");

        string *samplePermDirs = new string[nperms];
        for (int i = 0; i < nperms; ++i) {
            samplePermDirs[i] = sampleKB + string("/permtmp-") + to_string(i);
            fs::create_directories(fs::path(samplePermDirs[i]));
        }

        //Create the permutations
        createPermutations(sampleDir, nperms, signaturePerms, samplePermDirs);

        //Load a smaller KB
        KBConfig config;
        config.setParamInt(DICTPARTITIONS, dictionaries);
        config.setParamInt(NINDICES, nindices);
        config.setParamBool(AGGRINDICES, aggrIndices);
        config.setParamBool(USEFIXEDSTRAT, false);
        printStats = false;
        MemoryOptimizer::optimizeForWriting((long)(totalCount * sampleRate), config);
        KB kb(sampleKB.c_str(), false, false, false, config);

        loadKB(kb, sampleKB, false, dictionaries, dictMethod, nindices, aggrIndices,
               canSkipTables, false, sampleRate, storePlainList, fileNameDictionaries,
               nperms,
               signaturePerms, samplePermDirs, totalCount);

        fs::remove_all(fs::path(sampleDir));
        delete[] samplePermDirs;
    }
    BOOST_LOG_TRIVIAL(debug) << "...completed.";
}

void Loader::createPermutations(string inputDir, int nperms, int signaturePerms,
                                string *outputPermFiles) {
    SimpleTripleWriter **permWriters = new SimpleTripleWriter*[nperms];
    for (int i = 0; i < nperms; ++i) {
        permWriters[i] = new SimpleTripleWriter(outputPermFiles[i], "input", false);
    }
    int detailPerms[6];
    Compressor::parsePermutationSignature(signaturePerms, detailPerms);

    //Read all triples
    vector<string> files = Utils::getFiles(inputDir);
    for (std::vector<string>::iterator itr = files.begin(); itr != files.end();
            ++itr) {
        LZ4Reader reader(*itr);
        char quad = reader.parseByte();
        assert(quad == 0);
        while (!reader.isEof()) {
            long triple[3];
            triple[0] = reader.parseLong();
            triple[1] = reader.parseLong();
            triple[2] = reader.parseLong();

            for (int i = 0; i < nperms; ++i) {
                switch (detailPerms[i]) {
                case IDX_SPO:
                    permWriters[i]->write(triple[0], triple[1], triple[2]);
                    break;
                case IDX_OPS:
                    permWriters[i]->write(triple[2], triple[1], triple[0]);
                    break;
                case IDX_SOP:
                    permWriters[i]->write(triple[0], triple[2], triple[1]);
                    break;
                case IDX_OSP:
                    permWriters[i]->write(triple[2], triple[0], triple[1]);
                    break;
                case IDX_PSO:
                    permWriters[i]->write(triple[1], triple[0], triple[2]);
                    break;
                case IDX_POS:
                    permWriters[i]->write(triple[1], triple[2], triple[0]);
                    break;
                }
            }
        }
    }

    for (int i = 0; i < nperms; ++i) {
        delete permWriters[i];
    }
    delete[] permWriters;
}

void Loader::processTermCoordinates(Inserter *ins) {
    BufferCoordinates *buffer;
    while ((buffer = getBunchTermCoordinates()) != NULL) {
        nTerm key;
        while (!buffer->isEmpty()) {
            TermCoordinates *value = buffer->getNext(key);
            ins->insert(key, value);
        }
        releaseBunchTermCoordinates(buffer);
    }
}

void Loader::mergeTermCoordinates(string *coordinates, int ncoordinates) {
    CoordinatesMerger merger(coordinates, ncoordinates);
    nTerm key;
    TermCoordinates *value = NULL;
    while ((value = merger.get(key)) != NULL) {
        if (bufferToFill->isFull()) {
            boost::unique_lock<boost::mutex> lock(mut);
            buffersReady++;
            if (buffersReady > 0) {
                cond.notify_one();
            }

            while (buffersReady == 2) {
                cond.wait(lock);
            }

            //Take next buffer
            bufferToFill = (bufferToFill == &buffer1) ? &buffer2 : &buffer1;
        }
        bufferToFill->add(key, value);
    }
    boost::unique_lock<boost::mutex> lock(mut);
    if (!bufferToFill->isEmpty()) {
        buffersReady++;
    }
    isFinished = true;
    cond.notify_one();
    lock.unlock();
}

BufferCoordinates *Loader::getBunchTermCoordinates() {
    boost::unique_lock<boost::mutex> lock(mut);
    while (buffersReady == 0) {
        if (isFinished) {
            lock.unlock();
            return NULL;
        }
        cond.wait(lock);
    }
    lock.unlock();

    BufferCoordinates *tmpBuffer = bufferToReturn;
    bufferToReturn = (bufferToReturn == &buffer1) ? &buffer2 : &buffer1;
    return tmpBuffer;

}

void Loader::releaseBunchTermCoordinates(BufferCoordinates *cord) {
    cord->clear();
    boost::unique_lock<boost::mutex> lock(mut);
    if (buffersReady == 2) {
        cond.notify_one();
    }
    buffersReady--;
    lock.unlock();
}

TermCoordinates *CoordinatesMerger::get(nTerm &key) {
    value.clear();

    if (ncoordinates == 1) {
        if (!spoFinished) {
            key = elspo.key;
            value.set(0, elspo.file, elspo.pos, elspo.nElements, elspo.strat);
            spoFinished = !getFirst(&elspo, &spo);
            return &value;
        } else {
            return NULL;
        }
    }

    //n==3 or n==6

    if (!spoFinished && (opsFinished || elspo.key <= elops.key)
            && (posFinished || elspo.key <= elpos.key)) {
        key = elspo.key;
        value.set(0, elspo.file, elspo.pos, elspo.nElements, elspo.strat);
        if (ncoordinates == 6 && !sopFinished && elsop.key == elspo.key) {
            value.set(3, elsop.file, elsop.pos, elsop.nElements, elsop.strat);
            sopFinished = !getFirst(&elsop, &sop);
        }
        if (!opsFinished && elops.key == elspo.key) {
            value.set(1, elops.file, elops.pos, elops.nElements, elops.strat);
            if (ncoordinates == 6 && !ospFinished && elosp.key == elspo.key) {
                value.set(4, elosp.file, elosp.pos, elosp.nElements, elosp.strat);
                ospFinished = !getFirst(&elosp, &osp);
            }
            opsFinished = !getFirst(&elops, &ops);
        }
        if (!posFinished && elpos.key == elspo.key) {
            value.set(2, elpos.file, elpos.pos, elpos.nElements, elpos.strat);
            if ((ncoordinates == 6 || ncoordinates == 4) && !psoFinished
                    && elpso.key == elspo.key) {
                assert(elpos.nElements == elpso.nElements);
                value.set(5, elpso.file, elpso.pos, elpso.nElements, elpso.strat);
                psoFinished = !getFirst(&elpso, &pso);
            }
            posFinished = !getFirst(&elpos, &pos);
        }
        spoFinished = !getFirst(&elspo, &spo);
        return &value;
    } else if (!opsFinished && (posFinished || elops.key <= elpos.key)) {
        key = elops.key;
        value.set(1, elops.file, elops.pos, elops.nElements, elops.strat);
        if (ncoordinates == 6 && !ospFinished && elosp.key == elops.key) {
            value.set(4, elosp.file, elosp.pos, elosp.nElements, elosp.strat);
            ospFinished = !getFirst(&elosp, &osp);
        }
        if (!posFinished && elpos.key == elops.key) {
            value.set(2, elpos.file, elpos.pos, elpos.nElements, elpos.strat);
            if ((ncoordinates == 6 || ncoordinates == 4) && !psoFinished
                    && elpso.key == elops.key) {
                assert(elpos.nElements == elpso.nElements);
                value.set(5, elpso.file, elpso.pos, elpso.nElements, elpso.strat);
                psoFinished = !getFirst(&elpso, &pso);
            }
            posFinished = !getFirst(&elpos, &pos);
        }
        opsFinished = !getFirst(&elops, &ops);
        return &value;
    } else if (!posFinished) {
        key = elpos.key;
        value.set(2, elpos.file, elpos.pos, elpos.nElements, elpos.strat);
        if ((ncoordinates == 6 || ncoordinates == 4) && !psoFinished
                && elpso.key == elpos.key) {
            assert(elpos.nElements == elpso.nElements);
            value.set(5, elpso.file, elpso.pos, elpso.nElements, elpso.strat);
            psoFinished = !getFirst(&elpso, &pso);
        }
        posFinished = !getFirst(&elpos, &pos);
        return &value;
    } else {
        return NULL;
    }
}

bool CoordinatesMerger::getFirst(TreeEl *el, ifstream *buffer) {
    if (buffer->read(supportBuffer, 23)) {
        el->file = Utils::decode_short((const char*) supportBuffer, 16);
        el->key = Utils::decode_long(supportBuffer, 0);
        el->nElements = Utils::decode_long(supportBuffer, 8);
        el->pos = Utils::decode_int(supportBuffer, 18);
        el->strat = supportBuffer[22];
        return true;
    }
    return false;
}
