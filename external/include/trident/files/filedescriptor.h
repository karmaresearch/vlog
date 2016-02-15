#ifndef FILEDESCRIPTOR_H_
#define FILEDESCRIPTOR_H_

#include <trident/utils/memorymgr.h>

#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>

#include <string>

namespace bip = boost::interprocess;

//#define SMALLEST_INCR 1*1024*1024
#define SMALLEST_INCR 16*1024*1024

class Stats;
class FileDescriptor {

private:

    const std::string filePath;

    const bool readOnly;

    const int id;

    bip::file_mapping *mapping;

    bip::mapped_region* mapped_rgn;

    char* buffer;

    int size;

    int sizeFile;

    MemoryManager<FileDescriptor> *tracker;
    int memoryTrackerId;
    FileDescriptor **parentArray;

    void mapFile(int requiredIncrement);

public:
    FileDescriptor(bool readOnly, int id, std::string file, int fileMaxSize,
                   MemoryManager<FileDescriptor> *tracker, FileDescriptor **parents,
                   Stats * const stats);

    char* getBuffer(int offset, int *length);

    char* getBuffer(int offset, int *length, int &memoryBlock, const int sesID);

    int getFileLength();

    int getId() {
        return id;
    }

    bool isUsed();

    void shiftFile(int pos, int diff);

    void append(char *bytes, const int size);

    int appendVLong(const long v);

    int appendVLong2(const long v);

    void appendLong(const long v);

    void appendLong(const uint8_t nbytes, const uint64_t v);

    void overwriteAt(int pos, char byte);

    void overwriteVLong2At(int pos, long number);

    void reserveBytes(const uint8_t n);

    ~FileDescriptor();
};

#endif /* FILEDESCRIPTOR_H_ */
