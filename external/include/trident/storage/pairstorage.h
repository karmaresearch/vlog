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

#ifndef TWOTERMSTORAGE_H_
#define TWOTERMSTORAGE_H_

#include <trident/storage/fileindex.h>
#include <trident/storage/pairhandler.h>
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
public:
    int posSectors[MAX_N_SECTORS];
    short sectors[MAX_N_SECTORS];
    char *offsetSectors;
    int beginMarksOffset;
    int sizeMarks;
    short sizeSectors;

    bip::file_mapping *filemapping;
    bip::mapped_region *mappedrgn;

    int sizeIndices;
    FileIndex **indices;
    int *posIndices;
    int *offsetIndices;
    int beginIndicesOffset;

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
        pos += (offsetSectors[beginMarksOffset + mark * 2] & 255) << 8;
        pos += offsetSectors[beginMarksOffset + mark * 2 + 1] & 255;
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
            delete[] posIndices;
            delete[] offsetIndices;
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

    FileManager<ComprFileDescriptor, FileSegment> *cache;

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
    PairHandler *insertHandler;
    int **writeMarks;
    int *sizeWriteMarks;
    int *sizeUsedMarks;
    int fileCurrentIndex;
    int filePreviousIndex;
    FileIndex *currentIndex;
    FileIndex **indices[MAX_N_FILES];
    int sizeIndices[MAX_N_FILES];
    //*** END INSERT ***

    void storeFileIndices();
    void storeFileIndex(int *fileMarks, int sizeMarks, FileIndex **indices,
                        string pathFile);

public:
    TableStorage(bool readOnly, std::string pathDir, int maxFileSize,
                 int maxNFiles, MemoryManager<FileSegment> *bytesTracker,
                 Stats &stats);

    std::string getPath();

    void setupPairHandler(PairHandler *handler, short file, int mark);

    int startAppend(PairHandler *handler);

    void append(long v1, long v2);

    void stopAppend();

    long getNTriplesInserted();

    short getLastCreatedFile() {
        return lastCreatedFile;
    }

    int getLastFileSize() {
        return sizeLastCreatedFile;
    }

    ~TableStorage();
};

#endif /* TWOTERMSTORAGE_H_ */


