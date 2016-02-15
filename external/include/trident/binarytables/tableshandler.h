#ifndef TWOTERMSTORAGE_H_
#define TWOTERMSTORAGE_H_

#include <trident/binarytables/fileindex.h>
#include <trident/binarytables/binarytable.h>
#include <trident/binarytables/binarytableinserter.h>
#include <trident/kb/consts.h>
#include <trident/kb/statistics.h>
#include <trident/files/filemanager.h>

#include <boost/filesystem.hpp>
#include <boost/chrono.hpp>
#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>

#include <string>
#include <iostream>

namespace fs = boost::filesystem;
namespace bip = boost::interprocess;
namespace timens = boost::chrono;

#define MAX_LENGTH_PATHFILE 1024
#define MAX_N_SECTORS 5000
using namespace std;

class FileMarks {
private:
    const char *offsetSectors;
    const char *beginCoordinatesIndices;
    char *beginIndices;
    //int beginMarksOffset;
    //int beginIndicesOffset;

public:
    int posSectors[MAX_N_SECTORS];
    short sectors[MAX_N_SECTORS];
    int sizeMarks;
    short sizeSectors;

    bip::file_mapping *filemapping;
    bip::mapped_region *mappedrgn;

    int sizeIndices;
    FileIndex **indices;
    //int *posIndices;
    //int *offsetIndices;

    const char *getBeginTableCoordinates() {
        return offsetSectors;
    }

    void setBeginSectors(const char* p) {
        offsetSectors = p;
    }

    void setBeginIndices(char *p) {
        beginIndices = p;
    }

    void setBeginCoordinatesIndices(const char* p) {
        beginCoordinatesIndices = p;
    }

    const char *getEndTableCoordinates() {
        return offsetSectors + (2 + 9) * sizeMarks;
    }

    int getPos(const int mark, int &suggestedSector) {
        int pos = 0;
        //1- search the correct sector
        if (sizeSectors > 0 && mark >= posSectors[0]) {
            int b = 0;
            int e = sizeSectors - 1;
            if (suggestedSector != -1 && suggestedSector < sizeSectors
                    && mark >= posSectors[suggestedSector]
                    && (suggestedSector == e
                        || posSectors[suggestedSector + 1] > mark)) {
                e = suggestedSector;
            } else {
                while (b <= e) {
                    int pivot = (e + b) >> 1;
                    if (posSectors[pivot] > mark) {
                        e = pivot - 1;
                    } else if (posSectors[pivot] < mark) {
                        b = pivot + 1;
                    } else {
                        e = pivot;
                        break;
                    }
                }
            }
            pos = (sectors[e] & 0xFFFF) << 16;
            suggestedSector = e;
        }

        //2- add the offset
        pos += (offsetSectors[mark * (2 + 9)] & 255) << 8;
        pos += offsetSectors[mark * (2 + 9) + 1] & 255;
        return pos;
    }

    FileIndex *getIndex(const int mark);

    ~FileMarks() {
        if (sizeIndices > 0) {
            for (int i = 0; i < sizeIndices; ++i) {
                if (indices[i] != NULL) {
                    delete indices[i];
                }
            }
            delete[] indices;
            //delete[] posIndices;
            //delete[] offsetIndices;
        }
        delete mappedrgn;
        delete filemapping;
    }
};

class TableStorage {
private:
    const bool readOnly;
    char pathDir[MAX_LENGTH_PATHFILE];
    int sizePathDir;
    long nTriplesInserted;

    FileMarks *marks[MAX_N_FILES];
    bool marksLoaded[MAX_N_FILES];

    FileManager<FileDescriptor, FileDescriptor> *cache;

    short lastCreatedFile;
    int sizeLastCreatedFile;

    //Statistics
    Stats stats;

    //Cheap caching
    int lastMark;
    short lastFile;
    int lastPos;
    short lastNextFile;
    int lastNextPos;
    FileIndex *lastIndex;
    int lastSector;

    //*** INSERT ***
    BinaryTableInserter* insertHandler;
    int **writeMarks;
    long **keyTables;
    char **strats;
    int *sizeWriteMarks;
    int *sizeUsedMarks;
    int fileCurrentIndex;
    int filePreviousIndex;
    FileIndex *currentIndex;
    FileIndex **indices[MAX_N_FILES];
    int sizeIndices[MAX_N_FILES];
    //*** END INSERT ***

    void storeFileIndices();
    void storeFileIndex(int *fileMarks, long *keys, char *strats,
                        int sizeMarks, FileIndex **indices,
                        string pathFile);

public:
    TableStorage(bool readOnly, std::string pathDir, int maxFileSize,
                 int maxNFiles, MemoryManager<FileDescriptor> *bytesTracker,
                 Stats &stats);

    std::string getPath();

    void setupPairHandler(BinaryTable *handler, short file, int mark);

    int startAppend(const long key,
                    const char strat,
                    BinaryTableInserter* handler);

    void setStrategy(const char strat);

    void append(long v1, long v2);

    void stopAppend();

    long getNTriplesInserted();

    short getLastCreatedFile() {
        return lastCreatedFile;
    }

    int getLastFileSize() {
        return sizeLastCreatedFile;
    }

    bool doesFileHaveCoordinates(short file);

    const char *getBeginTableCoordinates(short file);

    const char *getEndTableCoordinates(short file);

    ~TableStorage();
};

#endif
