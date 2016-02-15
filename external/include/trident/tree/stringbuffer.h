#ifndef STRINGBUFFER_H_
#define STRINGBUFFER_H_

#include <trident/kb/consts.h>
#include <trident/kb/statistics.h>

#include <tridentcompr/utils/factory.h>
#include <tridentcompr/utils/hashfunctions.h>
#include <tridentcompr/utils/utils.h>
#include <tridentcompr/utils/hashmap.h>

#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/thread.hpp>

#include <iostream>
#include <fstream>
#include <string>
#include <vector>

struct eqint {
    bool operator()(int i1, int i2) const {
        return i1 == i2;
    }
};

struct hashint {
    std::size_t operator()(int i) const {
        return i;
    }
};

class StringBuffer {
private:
    static char FINISH_THREAD[1];

    Stats *stats;
    std::string dir;

    char uncompressSupportBuffer[SB_BLOCK_SIZE * 2];
    char termSupportBuffer[MAX_TERM_SIZE];

    PreallocatedArraysFactory<char> factory;
    const bool readOnly;
//  const bool compressDomains;
    std::vector<char*> blocks;
    std::vector<long> sizeCompressedBlocks;
    long uncompressedSize;
    char *currentBuffer;
    int writingCurrentBufferSize;

    std::fstream sb;

    //Used by the compression thread
    boost::mutex fileLock;
    char *bufferToCompress;
    int sizeBufferToCompress;

    boost::condition_variable compressWait;
    boost::mutex _compressMutex;
    boost::thread compressionThread;

    boost::mutex sizeLock;

    //Used to store domains of URIs
//  DomainToNumberMap domainMap;
//  InverseDomainToNumberMap invDomainMap;
//  int counterNewDomain;

    int posBaseEntry;
    int sizeBaseEntry;
    int entriesSinceBaseEntry;
    int nMatchedChars;

    //Cache
    vector<std::list<int>::iterator> cacheVector;
    std::list<int> cacheList;
    int elementsInCache;
    const int maxElementsInCache;

    void addCache(int idx);
    void compressBlocks();
    void compressLastBlock();
    void uncompressBlock(int b);

    char *getBlock(int idxBlock);

    int getFlag(int &blockId, char *&block, int &offset) {
        if (offset < SB_BLOCK_SIZE) {
            return block[offset++];
        } else {
            blockId++;
            block = getBlock(blockId);
            offset = 1;
            return block[0];
        }
    }

    int getVInt(int &blockId, char *&block, int &offset);

    void writeVInt(int n);

    void setCurrentAsBaseEntry(int size);

    int calculatePrefixWithBaseEntry(char *origBuffer, char *string, int size);

public:
    StringBuffer(string dir, bool readOnly, int factorySize, long cacheSize,
                 Stats *stats);

    long getSize();

    void append(char *string, int size);

    void get(long pos, char* outputBuffer, int &size);

    char* get(long pos, int &size);

    int cmp(long pos, char *string, int sizeString);

    ~StringBuffer();
};

#endif /* STRINGBUFFER_H_ */
