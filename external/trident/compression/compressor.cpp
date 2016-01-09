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

#include <tridentcompr/compression/compressor.h>
#include <tridentcompr/compression/filereader.h>

#include <tridentcompr/utils/triplewriters.h>
#include <tridentcompr/utils/utils.h>
#include <tridentcompr/utils/hashfunctions.h>
#include <tridentcompr/utils/factory.h>
#include <tridentcompr/utils/lruset.h>
#include <tridentcompr/utils/flajolet.h>
#include <tridentcompr/utils/stringscol.h>
#include <tridentcompr/sorting/sorter.h>
#include <tridentcompr/sorting/filemerger.h>

#include <iostream>
#include <utility>

#include <boost/lexical_cast.hpp>
#include <boost/filesystem.hpp>
#include <boost/log/trivial.hpp>
#include <boost/thread.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/chrono.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/lexical_cast.hpp>

namespace fs = boost::filesystem;
namespace timens = boost::chrono;
using namespace std;

bool lessTermFrequenciesDesc(const std::pair<string, long> &p1,
                             const std::pair<string, long> &p2) {
    return p1.second > p2.second || (p1.second == p2.second && p1.first > p2.first);
}

bool sampledTermsSorter1(const std::pair<string, size_t> &p1,
                         const std::pair<string, size_t> &p2) {
    /*    int l1 = Utils::decode_short(p1.first);
        int l2 = Utils::decode_short(p2.first);
        int ret = memcmp(p1.first + 2, p2.first + 2, min(l1, l2));
        if (ret == 0) {
            return (l1 - l2) < 0;
        } else {
            return ret < 0;
        }*/
    return p1.first < p2.first;
}

bool sampledTermsSorter2(const std::pair<string, size_t> &p1,
                         const std::pair<string, size_t> &p2) {
    return p1.second > p2.second;
}

bool cmpSampledTerms(const char* s1, const char* s2) {
    if (s1 == s2) {
        return true;
    }
    if (s1 && s2 && s1[0] == s2[0] && s1[1] == s2[1]) {
        int l = Utils::decode_short(s1, 0);
        return Utils::compare(s1, 2, 2 + l, s2, 2, 2 + l) == 0;
    }
    return false;
}

struct cmpInfoFiles {
    bool operator()(FileInfo i, FileInfo j) {
        return (i.size > j.size);
    }
} cmpInfoFiles;

Compressor::Compressor(string input, string kbPath) :
    input(input), kbPath(kbPath) {
    tmpFileNames = NULL;
    finalMap = new ByteArrayToNumberMap();
    poolForMap = new StringCollection(64 * 1024 * 1024);
    totalCount = 0;
    nTerms = 0;
    dictFileNames = NULL;
    uncommonDictFileNames = NULL;
}

void Compressor::addPermutation(const int permutation, int &output) {
    output |= 1 << permutation;
}

void Compressor::parsePermutationSignature(int signature, int *output) {
    int p = 0, idx = 0;
    while (signature) {
        if (signature & 1) {
            output[idx++] = p;
        }
        signature = signature >> 1;
        p++;
    }
}

void Compressor::uncompressTriples(ParamsUncompressTriples params) {
    vector<FileInfo> &files = params.files;
    Hashtable *table1 = params.table1;
    Hashtable *table2 = params.table2;
    Hashtable *table3 = params.table3;
    string outFile = params.outFile;
    SchemaExtractor *extractor = params.extractor;
    long *distinctValues = params.distinctValues;
    std::vector<string> *resultsMGS = params.resultsMGS;
    size_t sizeHeap = params.sizeHeap;

    LZ4Writer out(outFile);
    long count = 0;
    long countNotValid = 0;
    const char *supportBuffer = NULL;

    char *supportBuffers[3];
    if (extractor != NULL) {
        supportBuffers[0] = new char[MAX_TERM_SIZE + 2];
        supportBuffers[1] = new char[MAX_TERM_SIZE + 2];
        supportBuffers[2] = new char[MAX_TERM_SIZE + 2];
    }
 
    FlajoletMartin estimator;

    for (int i = 0; i < files.size(); ++i) {
        FileReader reader(files[i]);
        while (reader.parseTriple()) {
            if (reader.isTripleValid()) {
                count++;

                int length;
                supportBuffer = reader.getCurrentS(length);

                if (extractor != NULL) {
                    Utils::encode_short(supportBuffers[0], length);
                    memcpy(supportBuffers[0] + 2, supportBuffer, length);
                }
                long h1 = table1->add(supportBuffer, length);
                long h2 = table2->add(supportBuffer, length);
                long h3 = table3->add(supportBuffer, length);
                out.writeByte(0);
                estimator.addElement(h1, h2, h3);

                //This is an hack to save memcpy...
                out.writeVLong(length + 2);
                out.writeShort(length);
                out.writeRawArray(supportBuffer, length);

                supportBuffer = reader.getCurrentP(length);

                if (extractor != NULL) {
                    Utils::encode_short(supportBuffers[1], length);
                    memcpy(supportBuffers[1] + 2, supportBuffer, length);
                }
                h1 = table1->add(supportBuffer, length);
                h2 = table2->add(supportBuffer, length);
                h3 = table3->add(supportBuffer, length);
                out.writeByte(0);
                estimator.addElement(h1, h2, h3);

                out.writeVLong(length + 2);
                out.writeShort(length);
                out.writeRawArray(supportBuffer, length);

                supportBuffer = reader.getCurrentO(length);

                h1 = table1->add(supportBuffer, length);
                h2 = table2->add(supportBuffer, length);
                h3 = table3->add(supportBuffer, length);
                out.writeByte(0);
                estimator.addElement(h1, h2, h3);

                out.writeVLong(length + 2);
                out.writeShort(length);
                out.writeRawArray(supportBuffer, length);
            } else {
                countNotValid++;
            }
        }
    }

    *distinctValues = estimator.estimateCardinality();

    if (extractor != NULL) {
        delete[] supportBuffers[0];
        delete[] supportBuffers[1];
        delete[] supportBuffers[2];
    }

    BOOST_LOG_TRIVIAL(debug) << "Parsed triples: " << count << " not valid: " << countNotValid;
}

void Compressor::sampleTerm(const char *term, int sizeTerm, int sampleArg,
                            int dictPartitions, GStringToNumberMap *map/*,
                            LRUSet *duplicateCache, LZ4Writer **dictFile*/) {
    if (abs(random() % 10000) < sampleArg) {
        GStringToNumberMap::iterator itr = map->find(string(term + 2, sizeTerm - 2));
        if (itr != map->end()) {
            itr->second = itr->second + 1;
        } else {
            //char *newTerm = new char[sizeTerm];
            //memcpy(newTerm, (const char*) term, sizeTerm);
            map->insert(std::make_pair(string(term + 2, sizeTerm - 2), 1));
        }
    } else {
        /*if (!duplicateCache->exists(term)) {
            AnnotatedTerm t;
            t.term = term;
            t.size = sizeTerm;
            t.tripleIdAndPosition = -1;

            //Which partition?
            int partition = Utils::getPartition(term + 2, sizeTerm - 2,
                                                dictPartitions);
            //Add it into the file
            t.writeTo(dictFile[partition]);
            //Add it into the map
            duplicateCache->add(term);
        }*/
    }
}

void Compressor::extractUncommonTerm(const char *term, const int sizeTerm,
                                     ByteArrayToNumberMap *map,
                                     LZ4Writer **udictFile,
                                     const long tripleId,
                                     const int pos,
                                     const int partitions,
                                     const bool copyHashes,
                                     char **prevEntries, int *sPrevEntries) {

    if (map->find(term) == map->end()) {
        const int partition = Utils::getPartition(term + 2, sizeTerm - 2,
                              partitions);
        AnnotatedTerm t;
        t.size = sizeTerm;
        t.term = term;
        t.tripleIdAndPosition = (long) (tripleId << 2) + (pos & 0x3);

        if (!copyHashes) {
            //Add it into the file
            t.useHashes = false;
        } else {
            //Output the three pairs
            t.useHashes = true;
            if (pos == 0) {
                long hashp = Hashes::murmur3_56(prevEntries[1] + 2, sPrevEntries[1] - 2);
                long hasho = Hashes::murmur3_56(prevEntries[2] + 2, sPrevEntries[2] - 2);
                t.hashT1 = hashp;
                t.hashT2 = hasho;
            } else if (pos == 1) {
                long hashs = Hashes::murmur3_56(prevEntries[0] + 2, sPrevEntries[0] - 2);
                long hasho = Hashes::murmur3_56(prevEntries[2] + 2, sPrevEntries[2] - 2);
                t.hashT1 = hashs;
                t.hashT2 = hasho;
            } else { //pos = 2
                long hashs = Hashes::murmur3_56(prevEntries[0] + 2, sPrevEntries[0] - 2);
                long hashp = Hashes::murmur3_56(prevEntries[1] + 2, sPrevEntries[1] - 2);
                t.hashT1 = hashs;
                t.hashT2 = hashp;
            }
        }
        t.writeTo(udictFile[partition]);
    }
}

unsigned long Compressor::getEstimatedFrequency(const string &e) const {
    long v1 = table1 ? table1->get(e.c_str(), e.size()) : 0;
    long v2 = table2 ? table2->get(e.c_str(), e.size()) : 0;
    long v3 = table3 ? table3->get(e.c_str(), e.size()) : 0;
    return min(v1, min(v2, v3));
}

void Compressor::extractCommonTerm(const char* term, const int sizeTerm,
                                   long &countFrequent,
                                   const long thresholdForUncommon, Hashtable *table1,
                                   Hashtable *table2, Hashtable *table3,
                                   const int dictPartitions,
                                   long &minValueToBeAdded,
                                   const long maxMapSize,  GStringToNumberMap *map,

                                   std::priority_queue<std::pair<string, long>,
                                   std::vector<std::pair<string, long> >,
                                   priorityQueueOrder> &queue) {
    long v1, v2, v3;
    bool v2Checked = false;
    bool v3Checked = false;

    bool valueHighEnough = false;
    bool termInfrequent = false;
    v1 = table1->get(term + 2, sizeTerm - 2);
    long minValue = -1;
    if (v1 < thresholdForUncommon) {
        termInfrequent = true;
    } else {
        v2 = table2->get(term + 2, sizeTerm - 2);
        v2Checked = true;
        if (v2 < thresholdForUncommon) {
            termInfrequent = true;
        } else {
            v3 = table3->get(term + 2, sizeTerm - 2);
            v3Checked = true;
            if (v3 < thresholdForUncommon) {
                termInfrequent = true;
            } else {
                minValue = min(v1, min(v2, v3));
                valueHighEnough = minValue > minValueToBeAdded;
            }
        }
    }

    /* if (termInfrequent) {
        countInfrequent++;
        int partition = Utils::getPartition(term + 2, sizeTerm - 2,
                                            dictPartitions);
        AnnotatedTerm t;
        t.size = sizeTerm;
        t.term = term;
        t.tripleIdAndPosition = (long) (tripleId << 2) + (pos & 0x3);

        if (!copyHashes) {
            //Add it into the file
            t.useHashes = false;
        } else {
            //Output the three pairs
            t.useHashes = true;
            if (pos == 0) {
                long hashp = Hashes::murmur3_56(prevEntries[1] + 2, sPrevEntries[1] - 2);
                long hasho = Hashes::murmur3_56(prevEntries[2] + 2, sPrevEntries[2] - 2);
                t.hashT1 = hashp;
                t.hashT2 = hasho;
            } else if (pos == 1) {
                long hashs = Hashes::murmur3_56(prevEntries[0] + 2, sPrevEntries[0] - 2);
                long hasho = Hashes::murmur3_56(prevEntries[2] + 2, sPrevEntries[2] - 2);
                t.hashT1 = hashs;
                t.hashT2 = hasho;
            } else { //pos = 2
                long hashs = Hashes::murmur3_56(prevEntries[0] + 2, sPrevEntries[0] - 2);
                long hashp = Hashes::murmur3_56(prevEntries[1] + 2, sPrevEntries[1] - 2);
                t.hashT1 = hashs;
                t.hashT2 = hashp;
            }
        }
        t.writeTo(udictFile[partition]);
    } else {*/
    countFrequent++;
    bool mapTooSmall = map->size() < maxMapSize;
    if ((mapTooSmall || valueHighEnough)
            && map->find(string(term + 2, sizeTerm - 2)) == map->end()) {
        //Create copy
        //char *newTerm = new char[sizeTerm];
        //memcpy(newTerm, term, sizeTerm);

        std::pair<string, long> pair = std::make_pair(string(term + 2, sizeTerm - 2),
                                       minValue);
        map->insert(pair);
        queue.push(pair);
        if (map->size() > maxMapSize) {
            //Replace term and minCount with values to be added
            std::pair<string, long> elToRemove = queue.top();
            queue.pop();
            map->erase(elToRemove.first);
            /*//Insert value into the dictionary
            if (!duplicateCache.exists(elToRemove.first)) {
                //Which partition?
                int partition = Utils::getPartition(
                                    elToRemove.first + 2,
                                    Utils::decode_short(elToRemove.first),
                                    dictPartitions);

                AnnotatedTerm t;
                t.size = Utils::decode_short((char*) elToRemove.first) + 2;
                t.term = elToRemove.first;
                t.tripleIdAndPosition = -1;
                t.writeTo(dictFile[partition]);

                //Add it into the map
                duplicateCache.add(elToRemove.first);
            }*/
            minValueToBeAdded = queue.top().second;
        }
    }/* else {
        //Copy the term in a file so that later it can be inserted in the dictionaries
        if (!duplicateCache.exists(term + 2, sizeTerm - 2)) {
            //Which partition?
            int partition = Utils::getPartition(term + 2,
                                                sizeTerm - 2, dictPartitions);

            AnnotatedTerm t;
            t.size = sizeTerm;
            t.term = term;
            t.tripleIdAndPosition = -1;
            t.writeTo(dictFile[partition]);

            //Add it into the map
            duplicateCache.add(term + 2, sizeTerm - 2);
        }
    }
    }*/
}

void Compressor::extractUncommonTerms(const int dictPartitions, string inputFile,
                                      const bool copyHashes, const int idProcess,
                                      const int parallelProcesses,
                                      string *udictFileName,
                                      const bool splitByHash) {

    //Either one or the other. Both are not supported in extractUncommonTerm
    assert(!splitByHash || dictPartitions == 1);

    int partitions = dictPartitions;
    if (splitByHash)
        partitions = partitions * parallelProcesses;

    LZ4Writer **udictFile = new LZ4Writer*[partitions];
    if (splitByHash) {
        string prefixFile;
        for (int i = 0; i < partitions; ++i) {
            const int modHash =  i % parallelProcesses;
            if (modHash == 0) {
                prefixFile = udictFileName[i / parallelProcesses];
            }
            udictFile[i] = new LZ4Writer(prefixFile + string(".") +
                                         to_string(modHash));
        }
    } else {
        for (int i = 0; i < partitions; ++i) {
            udictFile[i] = new LZ4Writer(udictFileName[i]);
        }
    }

    LZ4Reader reader(inputFile);
    char *prevEntries[3];
    int sPrevEntries[3];
    if (copyHashes) {
        prevEntries[0] = new char[MAX_TERM_SIZE + 2];
        prevEntries[1] = new char[MAX_TERM_SIZE + 2];
    } else {
        prevEntries[0] = prevEntries[1] = NULL;
    }
    long tripleId = idProcess;
    int pos = 0;

    while (!reader.isEof()) {
        int sizeTerm = 0;
        int flag = reader.parseByte(); //Ignore it. Should always be 0
        if (flag != 0) {
            BOOST_LOG_TRIVIAL(error) << "Flag should always be zero!";
            throw 10;
        }

        const char *term = reader.parseString(sizeTerm);
        if (copyHashes) {
            if (pos != 2) {
                memcpy(prevEntries[pos], term, sizeTerm);
                sPrevEntries[pos] = sizeTerm;
            } else {
                prevEntries[2] = (char*)term;
                sPrevEntries[2] = sizeTerm;

                extractUncommonTerm(prevEntries[0], sPrevEntries[0], finalMap,
                                    udictFile, tripleId, 0,
                                    (splitByHash) ? parallelProcesses : dictPartitions, copyHashes,
                                    prevEntries, sPrevEntries);

                extractUncommonTerm(prevEntries[1], sPrevEntries[1], finalMap,
                                    udictFile, tripleId, 1,
                                    (splitByHash) ? parallelProcesses : dictPartitions, copyHashes,
                                    prevEntries, sPrevEntries);

                extractUncommonTerm(term, sizeTerm, finalMap,
                                    udictFile, tripleId, pos,
                                    (splitByHash) ? parallelProcesses : dictPartitions, copyHashes,
                                    prevEntries, sPrevEntries);
            }
        } else {
            extractUncommonTerm(term, sizeTerm, finalMap,
                                udictFile, tripleId, pos,
                                (splitByHash) ? parallelProcesses : dictPartitions, copyHashes,
                                prevEntries, sPrevEntries);
        }

        pos = (pos + 1) % 3;
        if (pos == 0) {
            tripleId += parallelProcesses;
        }
    }

    if (copyHashes) {
        delete[] prevEntries[0];
        delete[] prevEntries[1];
    }

    for (int i = 0; i < partitions; ++i) {
        delete udictFile[i];
    }
    delete[] udictFile;
}

void Compressor::extractCommonTerms(ParamsExtractCommonTermProcedure params) {

    string inputFile = params.inputFile;
    Hashtable **tables = params.tables;
    GStringToNumberMap *map = params.map;
    int dictPartitions = params.dictPartitions;
    //string *dictFileName = params.dictFileName;
    int maxMapSize = params.maxMapSize;

    int pos = 0;
    //int parallelProcesses = params.parallelProcesses;
    //string *udictFileName = params.singleTerms;
    int thresholdForUncommon = params.thresholdForUncommon;
    //const bool copyHashes = params.copyHashes;

    Hashtable *table1 = tables[0];
    Hashtable *table2 = tables[1];
    Hashtable *table3 = tables[2];

    LZ4Reader reader(inputFile);
    map->set_empty_key(EMPTY_KEY);
    map->set_deleted_key(DELETED_KEY);

    long minValueToBeAdded = 0;
    std::priority_queue<std::pair<string, long>,
        std::vector<std::pair<string, long> >, priorityQueueOrder> queue;

    long countFrequent = 0;

    while (!reader.isEof()) {
        int sizeTerm = 0;
        reader.parseByte(); //Ignore it. Should always be 0
        const char *term = reader.parseString(sizeTerm);

        extractCommonTerm(term, sizeTerm, countFrequent,
                          thresholdForUncommon, table1, table2, table3,
                          dictPartitions, minValueToBeAdded,
                          maxMapSize, map, queue);

        pos = (pos + 1) % 3;
    }

    BOOST_LOG_TRIVIAL(debug) << "Hashtable size after extraction " << map->size() << ". Frequent terms " << countFrequent;

}

void Compressor::mergeCommonTermsMaps(ByteArrayToNumberMap *finalMap,
                                      GStringToNumberMap *maps, int nmaps) {
    char supportTerm[MAX_TERM_SIZE];
    for (int i = 0; i < nmaps; i++) {
        for (GStringToNumberMap::iterator itr = maps[i].begin();
                itr != maps[i].end(); ++itr) {
            Utils::encode_short(supportTerm, itr->first.size());
            memcpy(supportTerm + 2, itr->first.c_str(), itr->first.size());

            ByteArrayToNumberMap::iterator foundValue = finalMap->find(supportTerm);
            if (foundValue == finalMap->end()) {
                const char *newkey = poolForMap->addNew(supportTerm,
                                                        Utils::decode_short(supportTerm) + 2);
                finalMap->insert(std::make_pair(newkey, itr->second));
            }
        }
        maps[i].clear();
    }
}

bool comparePairs(std::pair<const char *, long> i,
                  std::pair<const char *, long> j) {
    if (i.second > j.second) {
        return true;
    } else if (i.second == j.second) {
        int s1 = Utils::decode_short((char*) i.first);
        int s2 = Utils::decode_short((char*) j.first);
        return Utils::compare(i.first, 2, s1 + 2, j.first, 2, s2 + 2) > 0;
    } else {
        return false;
    }
}

void Compressor::assignNumbersToCommonTermsMap(ByteArrayToNumberMap *map,
        long *counters, LZ4Writer **writers, LZ4Writer **invWriters,
        int ndictionaries, bool preserveMapping) {
    std::vector<std::pair<const char *, long> > pairs;
    for (ByteArrayToNumberMap::iterator itr = map->begin(); itr != map->end();
            ++itr) {
        std::pair<const char *, long> pair;
        pair.first = itr->first;
        pair.second = itr->second;
        pairs.push_back(pair);

#ifdef DEBUG
        /*        const char* text = SchemaExtractor::support.addNew(itr->first, Utils::decode_short(itr->first) + 2);
                long hash = Hashes::murmur3_56(itr->first + 2, Utils::decode_short(itr->first));
                SchemaExtractor::properties.insert(make_pair(hash, text));*/
#endif
    }
    std::sort(pairs.begin(), pairs.end(), &comparePairs);

    int counterIdx = 0;
    for (int i = 0; i < pairs.size(); ++i) {
        nTerm key;
        ByteArrayToNumberMap::iterator itr = map->find(pairs[i].first);
        int size = Utils::decode_short(pairs[i].first) + 2;
        int part = Utils::getPartition(pairs[i].first + 2,
                                       Utils::decode_short(pairs[i].first), ndictionaries);

        if (preserveMapping) {
            key = counters[part];
            counters[part] += ndictionaries;
        } else {
            key = counters[counterIdx];
            counters[counterIdx] += ndictionaries;
            counterIdx = (counterIdx + 1) % ndictionaries;
        }
        writers[part]->writeLong(key);
        writers[part]->writeString(pairs[i].first, size);
        itr->second = key;

        //This is needed for the smart compression
        if (invWriters != NULL) {
            int part2 = key % ndictionaries;
            invWriters[part2]->writeLong(key);
            invWriters[part2]->writeString(pairs[i].first, size);
        }
    }
}

void Compressor::newCompressTriples(ParamsNewCompressProcedure params) {
    //Read the file in input
    string in = params.inNames[params.part];
    fs::path pFile(in);
    fs::path pNewFile = pFile;
    pNewFile.replace_extension(to_string(params.itrN));
    string newFile = pNewFile.string();
    long compressedTriples = 0;
    long compressedTerms = 0;
    long uncompressedTerms = 0;
    LZ4Reader *uncommonTermsReader = NULL;

    long nextTripleId = -1;
    int nextPos = -1;
    long nextTerm = -1;
    if (params.uncommonTermsFile != NULL) {
        uncommonTermsReader = new LZ4Reader(*(params.uncommonTermsFile));
        if (!uncommonTermsReader->isEof()) {
            long tripleId = uncommonTermsReader->parseLong();
            nextTripleId = tripleId >> 2;
            nextPos = tripleId & 0x3;
            nextTerm = uncommonTermsReader->parseLong();
        } else {
            BOOST_LOG_TRIVIAL(debug) << "The file " << *(params.uncommonTermsFile) << " is empty";
        }
    } else {
        BOOST_LOG_TRIVIAL(debug) << "No uncommon file is provided";
    }

    long currentTripleId = params.part;
    int increment = params.parallelProcesses;

    if (fs::exists(pFile) && fs::file_size(pFile) > 0) {
        LZ4Reader r(pFile.string());
        LZ4Writer w(newFile);

        long triple[3];
        char *tTriple = new char[MAX_TERM_SIZE * 3];
        bool valid[3];

        SimpleTripleWriter **permWriters = new SimpleTripleWriter*[params.nperms];
        const int nperms = params.nperms;
        int detailPerms[6];
        Compressor::parsePermutationSignature(params.signaturePerms, detailPerms);
        for (int i = 0; i < nperms; ++i) {
            permWriters[i] = new SimpleTripleWriter(params.permDirs[i],
                                                    params.prefixOutputFile + to_string(params.part), false);
        }
        while (!r.isEof()) {
            for (int i = 0; i < 3; ++i) {
                valid[i] = false;
                int flag = r.parseByte();
                if (flag == 1) {
                    //convert number
                    triple[i] = r.parseLong();
                    valid[i] = true;
                } else {
                    //Match the text against the hashmap
                    int size;
                    const char *tTerm = r.parseString(size);

                    if (currentTripleId == nextTripleId && nextPos == i) {
                        triple[i] = nextTerm;
                        valid[i] = true;
                        if (!uncommonTermsReader->isEof()) {
                            long tripleId = uncommonTermsReader->parseLong();
                            nextTripleId = tripleId >> 2;
                            nextPos = tripleId & 0x3;
                            nextTerm = uncommonTermsReader->parseLong();
                        }
                        compressedTerms++;
                    } else {
                        bool ok = false;
                        //Check the hashmap
                        if (params.commonMap != NULL) {
                            ByteArrayToNumberMap::iterator itr =
                                params.commonMap->find(tTerm);
                            if (itr != params.commonMap->end()) {
                                triple[i] = itr->second;
                                valid[i] = true;
                                ok = true;
                                compressedTerms++;
                            }
                        }

                        if (!ok) {
                            CompressedByteArrayToNumberMap::iterator itr2 =
                                params.map->find(tTerm);
                            if (itr2 != params.map->end()) {
                                triple[i] = itr2->second;
                                valid[i] = true;
                                compressedTerms++;
                            } else {
                                memcpy(tTriple + MAX_TERM_SIZE * i, tTerm,
                                       size);
                                uncompressedTerms++;
                            }
                        }
                    }
                }
            }

            if (valid[0] && valid[1] && valid[2]) {
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
                /*switch (nperms) {
                case 1:
                    permWriters[0]->write(triple[0], triple[1], triple[2]);
                    break;
                case 2:
                    permWriters[0]->write(triple[0], triple[1], triple[2]);
                    permWriters[1]->write(triple[2], triple[1], triple[0]);
                    break;
                case 3:
                    permWriters[0]->write(triple[0], triple[1], triple[2]);
                    permWriters[1]->write(triple[2], triple[1], triple[0]);
                    permWriters[2]->write(triple[1], triple[2], triple[0]);
                    break;
                case 4:
                    permWriters[0]->write(triple[0], triple[1], triple[2]);
                    permWriters[1]->write(triple[2], triple[1], triple[0]);
                    permWriters[2]->write(triple[0], triple[2], triple[1]);
                    permWriters[3]->write(triple[2], triple[0], triple[1]);
                    break;
                case 6:
                    permWriters[0]->write(triple[0], triple[1], triple[2]);
                    permWriters[1]->write(triple[2], triple[1], triple[0]);
                    permWriters[2]->write(triple[0], triple[2], triple[1]);
                    permWriters[3]->write(triple[2], triple[0], triple[1]);
                    permWriters[5]->write(triple[1], triple[0], triple[2]);
                    permWriters[4]->write(triple[1], triple[2], triple[0]);
                    break;
                }*/
                compressedTriples++;
            } else {
                //Write it into the file
                for (int i = 0; i < 3; ++i) {
                    if (valid[i]) {
                        w.writeByte(1);
                        w.writeLong((long) triple[i]);
                    } else {
                        w.writeByte(0);
                        char *t = tTriple + MAX_TERM_SIZE * i;
                        w.writeString(t, Utils::decode_short(t) + 2);
                    }
                }
            }

            currentTripleId += increment;
        }

        for (int i = 0; i < nperms; ++i) {
            delete permWriters[i];
        }
        delete[] permWriters;

        if (uncommonTermsReader != NULL) {
            if (!(uncommonTermsReader->isEof())) {
                BOOST_LOG_TRIVIAL(error) << "There are still elements to read in the uncommon file";
            }
            delete uncommonTermsReader;
        }
        delete[] tTriple;
    } else {
        BOOST_LOG_TRIVIAL(debug) << "The file " << in << " does not exist or is empty";
    }

    BOOST_LOG_TRIVIAL(debug) << "Compressed triples " << compressedTriples << " compressed terms " << compressedTerms << " uncompressed terms " << uncompressedTerms;

    //Delete the input file and replace it with a new one
    fs::remove(pFile);
    params.inNames[params.part] = newFile;
}

bool Compressor::isSplittable(string path) {
    if (boost::algorithm::ends_with(path, ".gz")
            || boost::algorithm::ends_with(path, ".bz2")) {
        return false;
    } else {
        return true;
    }
}

vector<FileInfo> *Compressor::splitInputInChunks(const string &input, int nchunks) {
    /*** Get list all files ***/
    fs::path pInput(input);
    vector<FileInfo> infoAllFiles;
    long totalSize = 0;
    if (fs::is_directory(pInput)) {
        fs::directory_iterator end;
        for (fs::directory_iterator dir_iter(input); dir_iter != end;
                ++dir_iter) {
            if (dir_iter->path().filename().string()[0] != '.') {
                long fileSize = fs::file_size(dir_iter->path());
                totalSize += fileSize;
                FileInfo i;
                i.size = fileSize;
                i.start = 0;
                i.path = dir_iter->path().string();
                i.splittable = isSplittable(dir_iter->path().string());
                infoAllFiles.push_back(i);
            }
        }
    } else {
        long fileSize = fs::file_size(input);
        totalSize += fileSize;
        FileInfo i;
        i.size = fileSize;
        i.start = 0;
        i.path = input;
        i.splittable = isSplittable(input);
        infoAllFiles.push_back(i);
    }

    BOOST_LOG_TRIVIAL(info) << "Going to parse " << infoAllFiles.size() << " files. Total size in bytes: " << totalSize << " bytes";

    /*** Sort the input files by size, and split the files through the multiple processors ***/
    std::sort(infoAllFiles.begin(), infoAllFiles.end(), cmpInfoFiles);
    vector<FileInfo> *files = new vector<FileInfo> [nchunks];
    long splitTargetSize = totalSize / nchunks;
    int processedFiles = 0;
    int currentSplit = 0;
    long splitSize = 0;
    while (processedFiles < infoAllFiles.size()) {
        FileInfo f = infoAllFiles[processedFiles++];
        if (!f.splittable) {
            splitSize += f.size;
            files[currentSplit].push_back(f);
            if (splitSize >= splitTargetSize
                    && currentSplit < nchunks - 1) {
                currentSplit++;
                splitSize = 0;
            }
        } else {
            long assignedFileSize = 0;
            while (assignedFileSize < f.size) {
                long sizeToCopy;
                if (currentSplit == nchunks - 1) {
                    sizeToCopy = f.size - assignedFileSize;
                } else {
                    sizeToCopy = min(f.size - assignedFileSize,
                                     splitTargetSize - splitSize);
                }

                //Copy inside the split
                FileInfo splitF;
                splitF.path = f.path;
                splitF.start = assignedFileSize;
                splitF.splittable = true;
                splitF.size = sizeToCopy;
                files[currentSplit].push_back(splitF);

                splitSize += sizeToCopy;
                assignedFileSize += sizeToCopy;
                if (splitSize >= splitTargetSize
                        && currentSplit < nchunks - 1) {
                    currentSplit++;
                    splitSize = 0;
                }
            }
        }
    }
    infoAllFiles.clear();

    for (int i = 0; i < nchunks; ++i) {
        long totalSize = 0;
        for (vector<FileInfo>::iterator itr = files[i].begin();
                itr < files[i].end(); ++itr) {
            totalSize += itr->size;
        }
        BOOST_LOG_TRIVIAL(debug) << "Files in split " << i << ": " << files[i].size() << " size " << totalSize;
    }
    return files;
}

void Compressor::parse(int dictPartitions, int sampleMethod, int sampleArg,
                       int sampleArg2, int parallelProcesses, int maxReadingThreads,
                       bool copyHashes,
                       const bool splitUncommonByHash, bool onlySample) {
    tmpFileNames = new string[parallelProcesses];
    vector<FileInfo> *files = splitInputInChunks(input, parallelProcesses);

    /*** Set name dictionary files ***/
    dictFileNames = new string*[parallelProcesses];
    uncommonDictFileNames = new string*[parallelProcesses];
    for (int i = 0; i < parallelProcesses; ++i) {
        string *df = new string[dictPartitions];
        string *df2 = new string[dictPartitions];
        for (int j = 0; j < dictPartitions; ++j) {
            df[j] = kbPath + string("/dict-file-") + to_string(i) + string("-")
                    + to_string(j);
            df2[j] = kbPath + string("/udict-file-") + to_string(i)
                     + string("-") + to_string(j);
        }
        dictFileNames[i] = df;
        uncommonDictFileNames[i] = df2;
    }

    timens::system_clock::time_point start = timens::system_clock::now();
    GStringToNumberMap *commonTermsMaps =
        new GStringToNumberMap[parallelProcesses];
    do_countmin(dictPartitions, sampleArg, parallelProcesses,
                maxReadingThreads, copyHashes,
                files, commonTermsMaps, false);

    boost::chrono::duration<double> sec = boost::chrono::system_clock::now() - start;
    BOOST_LOG_TRIVIAL(debug) << "Time heavy hitters detection = " << sec.count() * 1000 << " ms";

    if (!onlySample) {
        /*** Extract the uncommon terms ***/
        BOOST_LOG_TRIVIAL(debug) << "Extract the uncommon terms";
        boost::thread *threads = new boost::thread[parallelProcesses];
        for (int i = 1; i < parallelProcesses; ++i) {
            threads[i - 1] = boost::thread(
                                 boost::bind(&Compressor::extractUncommonTerms, this,
                                             dictPartitions, tmpFileNames[i], copyHashes,
                                             i, parallelProcesses,
                                             uncommonDictFileNames[i],
                                             splitUncommonByHash));
        }
        extractUncommonTerms(dictPartitions, tmpFileNames[0], copyHashes, 0,
                             parallelProcesses,
                             uncommonDictFileNames[0],
                             splitUncommonByHash);
        for (int i = 1; i < parallelProcesses; ++i) {
            threads[i - 1].join();
        }
        BOOST_LOG_TRIVIAL(debug) << "Finished the extraction of the uncommon terms";
        delete[] threads;
    }
    delete[] commonTermsMaps;
    delete[] files;
}

unsigned int Compressor::getThresholdForUncommon(
    const int parallelProcesses,
    const int sizeHashTable,
    const int sampleArg,
    long *distinctValues,
    Hashtable **tables1,
    Hashtable **tables2,
    Hashtable **tables3) {
    nTerms = 0;
    for (int i = 0; i < parallelProcesses; ++i) {
        nTerms = max(nTerms, distinctValues[i]);
    }
    BOOST_LOG_TRIVIAL(debug) << "Estimated number of terms per partition: " << nTerms;
    long termsPerBlock = max((long)1, (long)(nTerms / sizeHashTable)); //Terms per block
    long tu1 = max((long) 1, tables1[0]->getThreshold(sizeHashTable - sampleArg));
    long tu2 = max((long) 1, tables2[0]->getThreshold(sizeHashTable - sampleArg));
    long tu3 = max((long) 1, tables3[0]->getThreshold(sizeHashTable - sampleArg));
    return max(4 * termsPerBlock, min(min(tu1, tu2), tu3));
}

void Compressor::do_countmin_secondpass(const int dictPartitions,
                                        const int sampleArg,
                                        const int parallelProcesses,
                                        bool copyHashes,
                                        const unsigned int sizeHashTable,
                                        Hashtable **tables1,
                                        Hashtable **tables2,
                                        Hashtable **tables3,
                                        long *distinctValues,
                                        GStringToNumberMap *commonTermsMaps) {

    /*** Calculate the threshold value to identify uncommon terms ***/
    totalCount = tables1[0]->getTotalCount() / 3;
    unsigned int thresholdForUncommon = getThresholdForUncommon(
                                            parallelProcesses, sizeHashTable,
                                            sampleArg, distinctValues,
                                            tables1, tables2, tables3);
    BOOST_LOG_TRIVIAL(debug) << "Threshold to mark elements as uncommon: " <<
                             thresholdForUncommon;

    /*** Extract the common URIs ***/
    Hashtable *tables[3];
    tables[0] = tables1[0];
    tables[1] = tables2[0];
    tables[2] = tables3[0];
    ParamsExtractCommonTermProcedure params;
    params.tables = tables;
    params.dictPartitions = dictPartitions;
    params.maxMapSize = sampleArg;
    params.parallelProcesses = parallelProcesses;
    params.thresholdForUncommon = thresholdForUncommon;
    params.copyHashes = copyHashes;
    boost::thread *threads = new boost::thread[parallelProcesses - 1];

    BOOST_LOG_TRIVIAL(debug) << "Extract the common terms";
    for (int i = 1; i < parallelProcesses; ++i) {
        params.inputFile = tmpFileNames[i];
        params.map = &commonTermsMaps[i];
        params.dictFileName = dictFileNames[i];
        params.idProcess = i;
        params.singleTerms = uncommonDictFileNames[i];
        threads[i - 1] = boost::thread(
                             boost::bind(&Compressor::extractCommonTerms, this, params));
    }
    params.inputFile = tmpFileNames[0];
    params.map = &commonTermsMaps[0];
    params.dictFileName = dictFileNames[0];
    params.idProcess = 0;
    params.singleTerms = uncommonDictFileNames[0];
    extractCommonTerms(params);
    for (int i = 1; i < parallelProcesses; ++i) {
        threads[i - 1].join();
    }
    delete[] threads;
}

void Compressor::do_countmin(const int dictPartitions, const int sampleArg,
                             const int parallelProcesses, const int maxReadingThreads,
                             const bool copyHashes,
                             vector<FileInfo> *files,
                             GStringToNumberMap *commonTermsMaps,
                             bool usemisgra) {
    /*** Uncompress the triples in parallel ***/
    Hashtable **tables1 = new Hashtable*[parallelProcesses];
    Hashtable **tables2 = new Hashtable*[parallelProcesses];
    Hashtable **tables3 = new Hashtable*[parallelProcesses];
    long *distinctValues = new long[parallelProcesses];
    memset(distinctValues, 0, sizeof(long)*parallelProcesses);

    boost::thread *threads = new boost::thread[parallelProcesses - 1];

    /*** If we intend to use Misra to store the popular terms, then we must init
     * it ***/
    vector<string> *resultsMGS = NULL;
    if (usemisgra) {
        resultsMGS = new vector<string>[parallelProcesses];
    }

    /*** Calculate size of the hash table ***/
    long nBytesInput = Utils::getNBytes(input);
    bool isInputCompressed = Utils::isCompressed(input);
    long maxSize;
    if (!isInputCompressed) {
        maxSize = nBytesInput / 1000;
    } else {
        maxSize = nBytesInput / 25;
    }
    BOOST_LOG_TRIVIAL(debug) << "Size Input: " << nBytesInput <<
                             " bytes. Max table size=" << maxSize;
    long memForHashTables = (long)(Utils::getSystemMemory() * 0.6)
                            / (1 + maxReadingThreads) / 3;
    //Divided numer hash tables
    const unsigned int sizeHashTable = std::min((long)maxSize,
                                       (long)std::max((unsigned int)1000000,
                                               (unsigned int)(memForHashTables / sizeof(long))));
    BOOST_LOG_TRIVIAL(debug) << "Size hash table " << sizeHashTable;

    int chunksToProcess = 0;

    ParamsUncompressTriples params;
    //Set only global params
    params.sizeHeap = sampleArg;

    while (chunksToProcess < parallelProcesses) {
        for (int i = 1;
                i < maxReadingThreads
                && (chunksToProcess + i) < parallelProcesses;
                ++i) {
            tables1[chunksToProcess + i] = new Hashtable(sizeHashTable,
                    &Hashes::dbj2s_56);
            tables2[chunksToProcess + i] = new Hashtable(sizeHashTable,
                    &Hashes::fnv1a_56);
            tables3[chunksToProcess + i] = new Hashtable(sizeHashTable,
                    &Hashes::murmur3_56);
            tmpFileNames[chunksToProcess + i] = kbPath + string("/tmp-")
                                                + boost::lexical_cast<string>(
                                                    chunksToProcess + i);

            params.files = files[chunksToProcess + i];
            params.table1 = tables1[chunksToProcess + i];
            params.table2 = tables2[chunksToProcess + i];
            params.table3 = tables3[chunksToProcess + i];
            params.outFile = tmpFileNames[chunksToProcess + i];
            params.extractor = NULL;
            params.distinctValues = distinctValues + i + chunksToProcess;
            params.resultsMGS = usemisgra ? &resultsMGS[chunksToProcess + i] : NULL;
            threads[i - 1] = boost::thread(
                                 boost::bind(&Compressor::uncompressTriples, this,
                                             params));
        }
        tables1[chunksToProcess] = new Hashtable(sizeHashTable,
                &Hashes::dbj2s_56);
        tables2[chunksToProcess] = new Hashtable(sizeHashTable,
                &Hashes::fnv1a_56);
        tables3[chunksToProcess] = new Hashtable(sizeHashTable,
                &Hashes::murmur3_56);
        tmpFileNames[chunksToProcess] = kbPath + string("/tmp-")
                                        + to_string(chunksToProcess);

        params.files = files[chunksToProcess];
        params.table1 = tables1[chunksToProcess];
        params.table2 = tables2[chunksToProcess];
        params.table3 = tables3[chunksToProcess];
        params.outFile = tmpFileNames[chunksToProcess];
        params.extractor = NULL;
        params.distinctValues = distinctValues + chunksToProcess;
        params.resultsMGS = usemisgra ? &resultsMGS[chunksToProcess] : NULL;
        uncompressTriples(params);

        for (int i = 1; i < maxReadingThreads; ++i) {
            threads[i - 1].join();
        }

        //Merging the tables
        BOOST_LOG_TRIVIAL(debug) << "Merging the tables...";

        for (int i = 0; i < maxReadingThreads; ++i) {
            if ((chunksToProcess + i) != 0) {
                BOOST_LOG_TRIVIAL(debug) << "Merge table " << (chunksToProcess + i);
                tables1[0]->merge(tables1[chunksToProcess + i]);
                tables2[0]->merge(tables2[chunksToProcess + i]);
                tables3[0]->merge(tables3[chunksToProcess + i]);
                delete tables1[chunksToProcess + i];
                delete tables2[chunksToProcess + i];
                delete tables3[chunksToProcess + i];
            }
        }

        chunksToProcess += maxReadingThreads;
    }


    do_countmin_secondpass(dictPartitions, sampleArg, parallelProcesses,
                           copyHashes, sizeHashTable, tables1, tables2,
                           tables3, distinctValues, commonTermsMaps);
    /*** Delete the hashtables ***/
    BOOST_LOG_TRIVIAL(debug) << "Delete some datastructures";
    //delete tables1[0];
    //delete tables2[0];
    //delete tables3[0];
    table1 = std::shared_ptr<Hashtable>(tables1[0]);
    table2 = std::shared_ptr<Hashtable>(tables2[0]);
    table3 = std::shared_ptr<Hashtable>(tables3[0]);
    delete[] distinctValues;
    delete[] tables1;
    delete[] tables2;
    delete[] tables3;

    /*** Merge the hashmaps with the common terms ***/
    if (!usemisgra) {
        finalMap->set_empty_key(EMPTY_KEY);
        finalMap->set_deleted_key(DELETED_KEY);
        BOOST_LOG_TRIVIAL(debug) << "Merge the local common term maps";
        mergeCommonTermsMaps(finalMap, commonTermsMaps, parallelProcesses);
    }
    BOOST_LOG_TRIVIAL(debug) << "Size hashtable with common terms " << finalMap->size();

    /*** Extract the uncommon terms ***/
    /*BOOST_LOG_TRIVIAL(debug) << "Extract the uncommon terms";
    for (int i = 1; i < parallelProcesses; ++i) {
        threads[i - 1] = boost::thread(
                             boost::bind(&Compressor::extractUncommonTerms, this,
                                         dictPartitions, tmpFileNames[i], copyHashes,
                                         i, parallelProcesses,
                                         uncommonDictFileNames[i]));
    }
    extractUncommonTerms(dictPartitions, tmpFileNames[0], copyHashes, 0,
                         parallelProcesses,
                         uncommonDictFileNames[0]);
    for (int i = 1; i < parallelProcesses; ++i) {
        threads[i - 1].join();
    }*/

    delete[] threads;
}

bool Compressor::areFilesToCompress(int parallelProcesses, string *tmpFileNames) {
    for (int i = 0; i < parallelProcesses; ++i) {
        fs::path pFile(tmpFileNames[i]);
        if (fs::exists(pFile) && fs::file_size(pFile) > 0) {
            return true;
        }
    }
    return false;
}

void Compressor::sortAndDumpToFile2(vector<TriplePair> &pairs,
                                    string outputFile) {
    std::sort(pairs.begin(), pairs.end(), TriplePair::sLess);
    LZ4Writer outputSegment(outputFile);
    for (vector<TriplePair>::iterator itr = pairs.begin(); itr != pairs.end();
            ++itr) {
        itr->writeTo(&outputSegment);
    }
}

void Compressor::sortAndDumpToFile(vector<AnnotatedTerm> &terms, string outputFile,
                                   bool removeDuplicates) {
    BOOST_LOG_TRIVIAL(debug) << "Sorting and writing to file " << outputFile << " " << terms.size() << " elements";
    std::sort(terms.begin(), terms.end(), AnnotatedTerm::sLess);
    LZ4Writer *outputSegment = new LZ4Writer(outputFile);
    const char *prevTerm = NULL;
    int sizePrevTerm = 0;
    long countOutput = 0;
    for (vector<AnnotatedTerm>::iterator itr = terms.begin(); itr != terms.end();
            ++itr) {
        if (!removeDuplicates || prevTerm == NULL
                || !itr->equals(prevTerm, sizePrevTerm)) {
            itr->writeTo(outputSegment);
            prevTerm = itr->term;
            sizePrevTerm = itr->size;
            countOutput++;
        }
    }
    delete outputSegment;
    BOOST_LOG_TRIVIAL(debug) << "Written sorted elements: " << countOutput;
}

void Compressor::immemorysort(string **inputFiles, int dictPartition,
                              int parallelProcesses, string outputFile, int *noutputFiles,
                              ByteArrayToNumberMap *map, bool removeDuplicates,
                              const long maxSizeToSort) {
    timens::system_clock::time_point start = timens::system_clock::now();

    if (dictPartition != 0) {
        BOOST_LOG_TRIVIAL(error) << "Multiple dict partitions are no longer supported";
        throw 10;
        //I replaced the old for loop with a parallel routine. If there are multiple dict, then we can have ndicts*parallelProcesses threads, which is not a good idea.
        // This exception catches this case. In case it occurs, then we should write
        // extra code to deal with it in a proper way
    }


    //Split maxSizeToSort in n threads
    const long maxMemPerThread = maxSizeToSort / parallelProcesses;
    boost::thread *threads = new boost::thread[parallelProcesses - 1];
    for (int i = 1; i < parallelProcesses; ++i) {
        string fileName = inputFiles[i][0];
        if (fs::exists(fs::path(fileName))) {
            threads[i - 1] = boost::thread(
                                 boost::bind(
                                     &Compressor::inmemorysort_seq,
                                     this, fileName, i, parallelProcesses,
                                     maxMemPerThread, map,
                                     removeDuplicates, outputFile));
        }
    }
    string fileName = inputFiles[0][0];
    if (fs::exists(fs::path(fileName))) {
        inmemorysort_seq(fileName, 0, parallelProcesses,
                         maxMemPerThread, map,
                         removeDuplicates, outputFile);
    }
    for (int i = 1; i < parallelProcesses; ++i) {
        threads[i - 1].join();
    }
    delete[] threads;

    //Collect all files
    std::vector<string> files;
    boost::filesystem::path parentDir =
        boost::filesystem::path(outputFile).parent_path();
    string prefix =
        boost::filesystem::path(outputFile).filename().string() + string(".");
    for (boost::filesystem::directory_iterator itr(parentDir);
            itr != boost::filesystem::directory_iterator(); ++itr) {
        if (boost::filesystem::is_regular_file(itr->path())) {
            if (boost::starts_with(itr->path().filename().string(), prefix)) {
                files.push_back(itr->path().string());
            }
        }
    }

    for (int i = 0; i < files.size(); ++i) {
        string renamedFile = files[i] + "-old";
        boost::filesystem::rename(boost::filesystem::path(files[i]),
                                  boost::filesystem::path(renamedFile));
        files[i] = renamedFile;
    }
    for (int i = 0; i < files.size(); ++i) {
        string of = outputFile + string(".") + to_string(i);
        boost::filesystem::rename(boost::filesystem::path(files[i]),
                                  boost::filesystem::path(of));
    }
    *noutputFiles = files.size();

    /* int idx = 0;
     vector<AnnotatedTerm> terms;
     StringCollection supportCollection(BLOCK_SUPPORT_BUFFER_COMPR);
     long bytesAllocated = 0;
         for (int i = 0; i < parallelProcesses; ++i) {
         //Read the file
         string fileName = inputFiles[i][dictPartition];
         //Process the file
         if (fs::exists(fs::path(fileName))) {
             LZ4Reader *fis = new LZ4Reader(fileName);
             while (!fis->isEof()) {
                 AnnotatedTerm t;
                 t.readFrom(fis);

                 if (map == NULL || map->find(t.term) == map->end()) {
                     if ((bytesAllocated + (sizeof(AnnotatedTerm) * terms.size()))
                             >= maxSizeToSort) {
                         string ofile = outputFile + string(".")
                                        + to_string(idx++);
                         sortAndDumpToFile(terms, ofile, removeDuplicates);
                         terms.clear();
                         supportCollection.clear();
                         bytesAllocated = 0;
                     }

                     t.term = supportCollection.addNew((char *) t.term, t.size);
                     terms.push_back(t);
                     bytesAllocated += t.size;
                 }
             }
             delete fis;
             fs::remove(fileName);
         }
     }

     if (terms.size() > 0) {
         sortAndDumpToFile(terms, outputFile + string(".") + to_string(idx++),
                           removeDuplicates);
     }*/

    boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
                                          - start;
    BOOST_LOG_TRIVIAL(debug) << "Total sorting time = " << sec.count() * 1000
                             << " ms";
}

void Compressor::inmemorysort_seq(const string inputFile,
                                  int idx,
                                  const int incrIdx,
                                  const long maxMemPerThread,
                                  ByteArrayToNumberMap *map,
                                  bool removeDuplicates,
                                  string outputFile) {

    vector<AnnotatedTerm> terms;
    vector<string> outputfiles;
    StringCollection supportCollection(BLOCK_SUPPORT_BUFFER_COMPR);
    LZ4Reader *fis = new LZ4Reader(inputFile);
    long bytesAllocated = 0;

    while (!fis->isEof()) {
        AnnotatedTerm t;
        t.readFrom(fis);

        if (map == NULL || map->find(t.term) == map->end()) {
            if ((bytesAllocated + (sizeof(AnnotatedTerm) * terms.size() * 2))
                    >= maxMemPerThread) {
                string ofile = outputFile + string(".") + to_string(idx);
                idx += incrIdx;
                sortAndDumpToFile(terms, ofile, removeDuplicates);
                outputfiles.push_back(ofile);
                terms.clear();
                supportCollection.clear();
                bytesAllocated = 0;
            }

            t.term = supportCollection.addNew((char *) t.term, t.size);
            terms.push_back(t);
            bytesAllocated += t.size;
        }
    }

    if (terms.size() > 0) {
        string ofile = outputFile + string(".") + to_string(idx);
        sortAndDumpToFile(terms, ofile, removeDuplicates);
        outputfiles.push_back(ofile);
    }

    //Should I merge all files that I created?
    int tmpIdx = 0;
    if (outputfiles.size() > 0) {
        string finalFile = outputfiles[0];
        if (outputfiles.size() > 1) {
            while (outputfiles.size() > 1) {
                BOOST_LOG_TRIVIAL(debug) << "Going to merge at most"
                                         " 4 of the " << outputfiles.size() << " remained files";
                string tmpFile = outputfiles[0] + "-tmp" + to_string(tmpIdx++);
                //Remove last 4 files to merge
                std::vector<string> filesToMerge;
                for (int i = 0; i < 4 && i < outputfiles.size(); ++i) {
                    filesToMerge.push_back(outputfiles[outputfiles.size() - 1 - i]);
                }
                FileMerger<AnnotatedTerm> merger(filesToMerge);
                {
                    LZ4Writer writer(tmpFile);
                    while (!merger.isEmpty()) {
                        AnnotatedTerm t = merger.get();
                        t.writeTo(&writer);
                    }
                }
                //Remove all old files
                for (std::vector<string>::iterator itr = filesToMerge.begin();
                        itr != filesToMerge.end(); ++itr) {
                    fs::remove(*itr);
                    outputfiles.pop_back();
                }
                outputfiles.insert(outputfiles.begin(), tmpFile);
            }
            fs::rename(fs::path(outputfiles[0]), fs::path(finalFile));
        }
    }

    delete fis;
    fs::remove(inputFile);
}

void Compressor::mergeNotPopularEntries(vector<string> *inputFiles,
                                        LZ4Writer * globalDictOutput, string outputFile1, string outputFile2,
                                        long * startCounter, int increment, int parallelProcesses) {
    FileMerger<AnnotatedTerm> merger(*inputFiles);
    char *previousTerm = new char[MAX_TERM_SIZE + 2];
    Utils::encode_short(previousTerm, 0);
    long nextCounter = *startCounter;
    long currentCounter = nextCounter;

    LZ4Writer output1(outputFile1);
    LZ4Writer **output2 = new LZ4Writer*[parallelProcesses];
    for (int i = 0; i < parallelProcesses; ++i) {
        output2[i] = new LZ4Writer(outputFile2 + string(".") + to_string(i));
    }

    while (!merger.isEmpty()) {
        AnnotatedTerm t = merger.get();
        if (!t.equals(previousTerm)) {
            //Write a new entry in the global file
            currentCounter = nextCounter;
            globalDictOutput->writeLong(currentCounter);
            globalDictOutput->writeString(t.term, t.size);
            nextCounter += increment;
            memcpy(previousTerm, t.term, t.size);
        } else if (t.tripleIdAndPosition == -1) {
            continue;
        }

        if (t.tripleIdAndPosition == -1) {
            //Write it in output1
            output1.writeLong(currentCounter);
            output1.writeString(t.term, t.size);
        } else {
            //Write in output2
            int idx = (long) (t.tripleIdAndPosition >> 2) % parallelProcesses;
            output2[idx]->writeLong(t.tripleIdAndPosition);
            output2[idx]->writeLong(currentCounter);
        }
    }

    *startCounter = nextCounter;

    for (int i = 0; i < parallelProcesses; ++i) {
        delete output2[i];
    }
    delete[] output2;
    delete[] previousTerm;
}

void Compressor::sortByTripleID(vector<string> *inputFiles, string outputFile,
                                const long maxMemory) {
    //First sort the input files in chunks of x elements
    int idx = 0;
    vector<string> filesToMerge;
    {
        vector<TriplePair> pairs;

        for (int i = 0; i < inputFiles->size(); ++i) {
            //Read the file
            string fileName = (*inputFiles)[i];
            //Process the file
            LZ4Reader *fis = new LZ4Reader(fileName);
            while (!fis->isEof()) {
                if (sizeof(TriplePair) * pairs.size() >= maxMemory) {
                    string file = outputFile + string(".") + to_string(idx++);
                    sortAndDumpToFile2(pairs, file);
                    filesToMerge.push_back(file);
                    pairs.clear();
                }

                TriplePair tp;
                tp.readFrom(fis);
                pairs.push_back(tp);
            }
            delete fis;
            fs::remove(fileName);
        }

        if (pairs.size() > 0) {
            string file = outputFile + string(".") + to_string(idx++);
            sortAndDumpToFile2(pairs, file);
            filesToMerge.push_back(file);
        }
        pairs.clear();
    }

    //Then do a merge sort and write down the results on outputFile
    FileMerger<TriplePair> merger(filesToMerge);
    LZ4Writer writer(outputFile);
    while (!merger.isEmpty()) {
        TriplePair tp = merger.get();
        writer.writeLong(tp.tripleIdAndPosition);
        writer.writeLong(tp.term);
    }

    //Remove the input files
    for (int i = 0; i < filesToMerge.size(); ++i) {
        fs::remove(filesToMerge[i]);
    }
}

void Compressor::compressTriples(const int parallelProcesses, const int ndicts,
                                 string * permDirs, int nperms, int signaturePerms, vector<string> &notSoUncommonFiles,
                                 vector<string> &finalUncommonFiles, string * tmpFileNames,
                                 StringCollection * poolForMap, ByteArrayToNumberMap * finalMap) {
    /*** Compress the triples ***/
    LZ4Reader **dictFiles = new LZ4Reader*[ndicts];
    for (int i = 0; i < ndicts; ++i) {
        dictFiles[i] = new LZ4Reader(notSoUncommonFiles[i]);
    }
    int iter = 0;
    int dictFileProcessed = 0;
    unsigned long maxMemorySize =
        calculateSizeHashmapCompression();
    BOOST_LOG_TRIVIAL(debug) << "Max hashmap size: " << maxMemorySize << " bytes. Initial size of the common map=" << finalMap->size() << " entries.";

    CompressedByteArrayToNumberMap uncommonMap;
    while (areFilesToCompress(parallelProcesses, tmpFileNames)) {
        string prefixOutputFile = "input-" + to_string(iter);

        //Put new terms in the finalMap
        int idx = 0;
        while (poolForMap->allocatedBytes() + uncommonMap.size() * 20
                < maxMemorySize && dictFileProcessed < ndicts) {
            LZ4Reader *dictFile = dictFiles[idx];
            if (dictFile != NULL) {
                if (!dictFile->isEof()) {
                    long compressedTerm = dictFile->parseLong();
                    int sizeTerm;
                    const char *term = dictFile->parseString(sizeTerm);
                    if (uncommonMap.find(term) == uncommonMap.end()) {
                        const char *newTerm = poolForMap->addNew((char*) term,
                                              sizeTerm);
                        uncommonMap.insert(
                            std::make_pair(newTerm, compressedTerm));
                    } else {
                        BOOST_LOG_TRIVIAL(error) << "This should not happen! Term " << term
                                                 << " was already being inserted";
                    }
                } else {
                    BOOST_LOG_TRIVIAL(debug) << "Finished putting in the hashmap the elements in file " << notSoUncommonFiles[idx];
                    delete dictFile;
                    fs::remove(fs::path(notSoUncommonFiles[idx]));
                    dictFiles[idx] = NULL;
                    dictFileProcessed++;
                    if (dictFileProcessed == ndicts) {
                        break;
                    }
                }
            }
            idx = (idx + 1) % ndicts;
        }

        BOOST_LOG_TRIVIAL(debug) << "Start compression threads... uncommon map size " << uncommonMap.size();
        boost::thread *threads = new boost::thread[parallelProcesses - 1];
        ParamsNewCompressProcedure p;
        p.permDirs = permDirs;
        p.nperms = nperms;
        p.signaturePerms = signaturePerms;
        p.prefixOutputFile = prefixOutputFile;
        p.itrN = iter;
        p.inNames = tmpFileNames;
        p.commonMap = iter == 0 ? finalMap : NULL;
        p.map = &uncommonMap;
        p.parallelProcesses = parallelProcesses;

        for (int i = 1; i < parallelProcesses; ++i) {
            p.part = i;
            p.uncommonTermsFile = iter == 0 ? &finalUncommonFiles[i] : NULL;
            threads[i - 1] = boost::thread(
                                 boost::bind(&Compressor::newCompressTriples, this, p));
        }
        p.part = 0;
        p.uncommonTermsFile = iter == 0 ? &finalUncommonFiles[0] : NULL;
        newCompressTriples(p);
        for (int i = 1; i < parallelProcesses; ++i) {
            threads[i - 1].join();
        }
        delete[] threads;

        //Clean the map
        finalMap->clear();
        uncommonMap.clear();
        poolForMap->clear();

        //New iteration!
        iter++;
    }

    delete[] dictFiles;
}

void Compressor::sortFilesByTripleSource(string kbPath, const int parallelProcesses, const int ndicts, vector<string> uncommonFiles, vector<string> &outputFiles) {
    /*** Sort the files which contain the triple source ***/
    BOOST_LOG_TRIVIAL(debug) << "Sort uncommon triples by triple id";
    vector<vector<string> > inputFinalSorting;

    for (int i = 0; i < parallelProcesses; ++i) {
        vector<string> inputFiles;
        for (int j = 0; j < uncommonFiles.size(); ++j) {
            inputFiles.push_back(uncommonFiles[j] + string(".") + to_string(i));
        }
        inputFinalSorting.push_back(inputFiles);
        outputFiles.push_back(kbPath + string("/listUncommonTerms") + to_string(i));
    }

    boost::thread *threads = new boost::thread[parallelProcesses - 1];
    const long maxMem = max((long) MIN_MEM_SORT_TRIPLES,
                            (long) (Utils::getSystemMemory() * 0.7) / parallelProcesses);
    for (int i = 1; i < parallelProcesses; ++i) {
        threads[i - 1] = boost::thread(
                             boost::bind(&Compressor::sortByTripleID, this,
                                         &inputFinalSorting[i], outputFiles[i], maxMem));
    }
    sortByTripleID(&inputFinalSorting[0], outputFiles[0], maxMem);
    for (int i = 1; i < parallelProcesses; ++i) {
        threads[i - 1].join();
    }
    delete[] threads;
}

void Compressor::sortDictionaryEntriesByText(string **input, const int ndicts,
        const int parallelProcesses, string * prefixOutputFiles,
        int *noutputfiles, ByteArrayToNumberMap * map, bool filterDuplicates) {
    long maxMemAllocate = max((long) (BLOCK_SUPPORT_BUFFER_COMPR * 2),
                              (long) (Utils::getSystemMemory() * 0.70 / ndicts));
    BOOST_LOG_TRIVIAL(debug) << "Sorting dictionary entries for partitions";
    boost::thread *threads = new boost::thread[ndicts - 1];

    BOOST_LOG_TRIVIAL(debug) << "Max memory to use to sort inmemory a number of terms: " << maxMemAllocate << " bytes";
    for (int i = 1; i < ndicts; ++i) {
        threads[i - 1] = boost::thread(
                             boost::bind(&Compressor::immemorysort, this, input, i,
                                         parallelProcesses, prefixOutputFiles[i], &noutputfiles[i],
                                         map, filterDuplicates, maxMemAllocate));
    }
    immemorysort(input, 0, parallelProcesses, prefixOutputFiles[0],
                 &noutputfiles[0], map, filterDuplicates, maxMemAllocate);
    for (int i = 1; i < ndicts; ++i) {
        threads[i - 1].join();
    }
    delete[] threads;
    BOOST_LOG_TRIVIAL(debug) << "...done";
}

void Compressor::compress(string * permDirs, int nperms, int signaturePerms,
                          string * dictionaries,
                          int ndicts, int parallelProcesses) {

    /*** Sort the infrequent terms ***/
    int *nsortedFiles = new int[ndicts];
    BOOST_LOG_TRIVIAL(debug) << "Sorting common dictionary entries for partitions";
    sortDictionaryEntriesByText(dictFileNames, ndicts, parallelProcesses, dictionaries, nsortedFiles, finalMap, true);
    BOOST_LOG_TRIVIAL(debug) << "...done";

    /*** Sort the very infrequent terms ***/
    int *nsortedFiles2 = new int[ndicts];
    BOOST_LOG_TRIVIAL(debug) << "Sorting uncommon dictionary entries for partitions";
    string *uncommonDictionaries = new string[ndicts];
    for (int i = 0; i < ndicts; ++i) {
        uncommonDictionaries[i] = dictionaries[i] + string("-u");
    }
    sortDictionaryEntriesByText(uncommonDictFileNames, ndicts, parallelProcesses, uncommonDictionaries, nsortedFiles2, NULL, false);
    BOOST_LOG_TRIVIAL(debug) << "...done";

    /*** Deallocate the dictionary files ***/
    for (int i = 0; i < parallelProcesses; ++i) {
        delete[] dictFileNames[i];
        delete[] uncommonDictFileNames[i];
    }
    delete[] dictFileNames;
    delete[] uncommonDictFileNames;

    /*** Create the final dictionaries to be written and initialize the
     * counters and other data structures ***/
    LZ4Writer **writers = new LZ4Writer*[ndicts];
    long *counters = new long[ndicts];
    vector<vector<string> > filesToBeMerged;
    vector<string> notSoUncommonFiles;
    vector<string> uncommonFiles;

    for (int i = 0; i < ndicts; ++i) {
        vector<string> files;
        for (int j = 0; j < nsortedFiles[i]; ++j) {
            files.push_back(dictionaries[i] + string(".") + to_string(j));
        }
        for (int j = 0; j < nsortedFiles2[i]; ++j) {
            files.push_back(uncommonDictionaries[i] + string(".") + to_string(j));
        }
        filesToBeMerged.push_back(files);
        writers[i] = new LZ4Writer(dictionaries[i]);
        counters[i] = i;
        notSoUncommonFiles.push_back(dictionaries[i] + string("-np1"));
        uncommonFiles.push_back(dictionaries[i] + string("-np2"));
    }
    delete[] nsortedFiles;
    delete[] nsortedFiles2;
    delete[] uncommonDictionaries;

    /*** Assign a number to the popular entries ***/
    BOOST_LOG_TRIVIAL(debug) << "Assign a number to " << finalMap->size() <<
                             " popular terms in the dictionary";
    assignNumbersToCommonTermsMap(finalMap, counters, writers, NULL, ndicts, true);

    /*** Assign a number to the other entries. Split them into two files.
     * The ones that must be loaded into the hashmap, and the ones used for the merge join ***/
    BOOST_LOG_TRIVIAL(debug) << "Merge (and assign counters) of dictionary entries";
    boost::thread *threads = new boost::thread[ndicts - 1];
    for (int i = 1; i < ndicts; ++i) {
        threads[i - 1] = boost::thread(
                             boost::bind(&Compressor::mergeNotPopularEntries, this,
                                         &filesToBeMerged[i], writers[i], notSoUncommonFiles[i],
                                         uncommonFiles[i], &counters[i], ndicts,
                                         parallelProcesses));
    }
    mergeNotPopularEntries(&filesToBeMerged[0], writers[0],
                           notSoUncommonFiles[0], uncommonFiles[0], &counters[0], ndicts,
                           parallelProcesses);
    for (int i = 1; i < ndicts; ++i) {
        threads[i - 1].join();
    }
    delete[] threads;
    BOOST_LOG_TRIVIAL(debug) << "... done";

    /*** Close the dictionaries and remove unused data structures ***/
    for (int i = 0; i < ndicts; ++i) {
        delete writers[i];
        vector<string> filesToBeRemoved = filesToBeMerged[i];
        for (int j = 0; j < filesToBeRemoved.size(); ++j) {
            fs::remove(fs::path(filesToBeRemoved[j]));
        }
    }

    /*** Sort files by triple source ***/
    vector<string> sortedFiles;
    sortFilesByTripleSource(kbPath, parallelProcesses, ndicts, uncommonFiles, sortedFiles);

    /*** Compress the triples ***/
    compressTriples(parallelProcesses, ndicts, permDirs, nperms, signaturePerms,
                    notSoUncommonFiles, sortedFiles, tmpFileNames,
                    poolForMap, finalMap);

    /*** Clean up remaining datastructures ***/
    delete[] counters;
    for (int i = 0; i < parallelProcesses; ++i) {
        fs::remove(tmpFileNames[i]);
        fs::remove(sortedFiles[i]);
    }
    delete[] tmpFileNames;
    delete poolForMap;
    delete finalMap;
    poolForMap = NULL;
    finalMap = NULL;
}

bool stringComparator(string stringA, string stringB) {
    const char *ac = stringA.c_str();
    const char *bc = stringB.c_str();
    for (int i = 0; ac[i] != '\0' && bc[i] != '\0'; i++) {
        if (ac[i] != bc[i]) {
            return ac[i] < bc[i];
        }
    }
    return stringA.size() < stringB.size();
}

Compressor::~Compressor() {
    if (finalMap != NULL)
        delete finalMap;
    if (poolForMap != NULL)
        delete poolForMap;
}

unsigned long Compressor::calculateSizeHashmapCompression() {
    long memoryAvailable = Utils::getSystemMemory() * 0.70;
    return memoryAvailable;
}

unsigned long Compressor::calculateMaxEntriesHashmapCompression() {
    long memoryAvailable = min((int)(Utils::getSystemMemory() / 3 / 50), 90000000);
    return memoryAvailable;
}
