#ifndef _COMPRFILEDESCRIPTOR_H
#define _COMPRFILEDESCRIPTOR_H

#include <trident/kb/consts.h>
#include <trident/utils/memorymgr.h>

#include <tridentcompr/utils/utils.h>
#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>

#include <fstream>
#include <sstream>
#include <string>

#define COMPRESSION_ENABLED 0
#define MAX_N_MAPPINGS  5000

namespace bip = boost::interprocess;

struct FileSegment {
    char block[BLOCK_SIZE];
    int memId;
};

class Stats;
class ComprFileDescriptor {
private:
    const std::string file;
    const int id;
    const bool readOnly;
    int sizeMappings;
    int mappings[MAX_N_MAPPINGS];

    int uncompressedSize;
    FileSegment *uncompressedBuffers[MAX_N_MAPPINGS];

    //char specialTmpBuffer[BLOCK_MIN_SIZE];
    std::unique_ptr<char[]> specialTmpBuffers[MAX_SESSIONS];

    int lastAccessedSegment;

    //Used for writing
    int rawSize;
    char* rawBuffer;

    std::ifstream readOnlyInput;
    MemoryManager<FileSegment> *tracker;

    Stats* const stats;

    void uncompressBlock(const int b);
    void writeFile();
    //bool isUsed();
public:
    ComprFileDescriptor(bool readOnly, int id, std::string file, int maxSize,
                        MemoryManager<FileSegment> *tracker,
                        ComprFileDescriptor **parentArray, Stats* const stats);

    char* getBuffer(int offset, int *length, const int sesID);

    char* getBuffer(int offset, int *length, int &memoryBlock, const int sesID);

    int getFileLength() {
        return uncompressedSize;
    }

    int getId() {
        return id;
    }

    void appendLong(long v) {
        Utils::encode_long(rawBuffer, uncompressedSize, v);
        uncompressedSize += 8;
    }

    int appendVLong(long v) {
        uncompressedSize = Utils::encode_vlong(rawBuffer, uncompressedSize, v);
        return uncompressedSize;
    }

    int appendVLong2(long v) {
        uncompressedSize = Utils::encode_vlong2(rawBuffer, uncompressedSize, v);
        return uncompressedSize;
    }

    void reserveBytes(const uint8_t n) {
        uncompressedSize += n;
    }

    void append(char *bytes, int size) {
        if (size == 1) {
            rawBuffer[uncompressedSize++] = *bytes;
        } else {
            memcpy(rawBuffer + uncompressedSize, bytes, size);
            uncompressedSize += size;
        }
    }

    void overwriteAt(int pos, char byte) {
        rawBuffer[pos] = byte;
    }

    void overwriteVLong2At(int pos, long number) {
        Utils::encode_vlong2(rawBuffer, pos, number);
    }

    ~ComprFileDescriptor();
};

#endif
