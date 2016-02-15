/*
 * dictmgmt.h
 *
 *  Created on: Nov 30, 2013
 *      Author: jacopo
 */

#ifndef DICTMGMT_H_
#define DICTMGMT_H_

#include <trident/kb/consts.h>

#include <tridentcompr/utils/hashfunctions.h>
#include <tridentcompr/utils/utils.h>

#include <boost/thread.hpp>
#include <boost/unordered_map.hpp>

class Root;
class StringBuffer;
class TreeItr;

using namespace std;

class Row {
private:
    char *bData[MAX_N_PATTERNS];
    int size;
public:
    Row() {
        size = 0;
        for (int i = 0; i < MAX_N_PATTERNS; ++i) {
            bData[i] = NULL;
        }
    }
    void setSize(int size) {
        if (this->size < size) {
            for (int i = this->size; i < size; ++i) {
                if (bData[i] == NULL) {
                    bData[i] = new char[MAX_TERM_SIZE];
                }
            }
        }
        this->size = size;
    }

    char *getRawData(const int j) {
        return bData[j];
    }

    void printRow() {
        for (int i = 0; i < size; ++i) {
            std::cout << bData[i] << " ";
        }
        std::cout << "\n";
    }
    ~Row() {
        for (int i = 0; i < MAX_N_PATTERNS; ++i) {
            if (bData[i] != NULL)
                delete[] bData[i];
        }
    }
};

class DictMgmt {

private:

    boost::condition_variable waitForData;
    boost::condition_variable canAddData;
    boost::mutex mutex1;
    boost::mutex mutex2;
    bool threadIsWaiting;

    //Define the task to execute
    long* dataToProcess;
    int nTuples;
    int sTuples;
    bool printTuples;

    StringBuffer **stringbuffers;
    Root **dicts;
    Root **invDicts;
    const int dictPartitions;

    long *insertedNewTerms;
    long largestID;

    bool hash;

//  boost::unordered_map<char *, nTerm, KeyHasher, KeyCmp> standardTerms;

    Row row;

//  int long_hashcode(const long value) {
//      return (int) (value ^ (value >> 32));
//  }

    void loadStandardTerms();

    void parseDictFile(string path);
public:

    DictMgmt(StringBuffer **stringbuffers, Root **dicts, Root **invDicts,
             int dictPartitions, bool hash) :
        dictPartitions(dictPartitions), hash(hash) {
        threadIsWaiting = true;
        dataToProcess = NULL;
        nTuples = 0;
        this->stringbuffers = stringbuffers;
        this->dicts = dicts;
        this->invDicts = invDicts;
        printTuples = false;
        sTuples = 0;
        insertedNewTerms = new long[dictPartitions];
        for (int i = 0; i < dictPartitions; ++i) {
            insertedNewTerms[i] = 0;
        }
        largestID = 0;
    }

    int getPartitions() {
        return dictPartitions;
    }

    long getNTermsInserted() {
        long total = 0;
        for (int i = 0; i < dictPartitions; ++i) {
            total += insertedNewTerms[i];
        }
        return total;
    }

    long getLargestIDInserted() {
        return largestID;
    }

    int getDictPartition(const char *key, const int sizeKey);

    TreeItr *getInvDictIterator(int part);

    bool getText(nTerm key, char *value);

    bool getText(nTerm key, char *value, int &size);

    void getTextFromCoordinates(int part, long coordinates, char *output,
                                int &sizeOutput);

    bool getNumber(const char *key, const int sizeKey, nTerm *value);

    bool putDict(int part, const char *key, int sizeKey, nTerm &value);

    bool putDict(int part, const char *key, int sizeKey, nTerm &value,
                 long &coordinates);

    bool putInvDict(int part, const char *key, int sizeKey, nTerm &value);

    bool putInvDict(int part, const nTerm key, const long coordinates);

    bool putPair(const char *key, int sizeKey, nTerm &value);

    bool putPair(int part, const char *key, int sizeKey, nTerm &value);

    void threadlookupTerms(long *data, int nTuples, int sTuples, bool print);

    void lookupTerms(long *data, int nTuples, int sTuples, bool print);

    //void run();

    void loadDict(string dirFiles);

    void waitUntilFinish();

    bool useHashForCompression() {
        return hash;
    }

    void clean() {
    }

    ~DictMgmt() {
        delete[] insertedNewTerms;
    }
};

#endif /* DICTMGMT_H_ */
