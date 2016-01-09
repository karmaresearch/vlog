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

#include <trident/storage/pairstorage.h>
#include <trident/storage/pairhandler.h>

#include <tridentcompr/utils/utils.h>

#include <boost/filesystem.hpp>
#include <boost/chrono.hpp>
#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>

#include <iostream>
#include <string>
#include <fstream>
#include <stdlib.h>
#include <stdio.h>

using namespace std;
namespace fs = boost::filesystem;
namespace timens = boost::chrono;
namespace bip = boost::interprocess;

FileMarks *parseIndexFile(char* p, const int idx) {
    if (!fs::exists(p)) {
        return NULL;
    }

//  timens::system_clock::time_point start = timens::system_clock::now();

    FileMarks *mark = new FileMarks;
    mark->filemapping = new bip::file_mapping(p, bip::read_only);
    mark->mappedrgn = new bip::mapped_region(*(mark->filemapping),
            bip::read_only);
    char *raw_input = static_cast<char*>(mark->mappedrgn->get_address());
    mark->offsetSectors = raw_input;

    //Size marks
    mark->sizeMarks = Utils::decode_int(raw_input, 0);

    //Parse the sectors
    mark->sizeSectors = Utils::decode_short(raw_input, 4);
    int currentPosition = 6;
    for (int i = 0; i < mark->sizeSectors; ++i) {
        mark->posSectors[i] = Utils::decode_int(raw_input, currentPosition);
        currentPosition += 4;
        mark->sectors[i] = Utils::decode_short((const char*) raw_input,
                                               currentPosition);
        currentPosition += 2;
    }

    mark->beginMarksOffset = currentPosition;
    currentPosition += 2 * mark->sizeMarks;

    int sizeIndex = Utils::decode_int(raw_input, currentPosition);
    currentPosition += 4;
    FileIndex **indices = NULL;
    int *posIndices = NULL;
    int *offsetIndices = NULL;
    if (sizeIndex > 0) {
        indices = new FileIndex*[sizeIndex];
        posIndices = new int[sizeIndex];
        offsetIndices = new int[sizeIndex];
        for (int j = 0; j < sizeIndex; j++) {
            posIndices[j] = Utils::decode_int(raw_input, currentPosition);
            currentPosition += 4;
            offsetIndices[j] = Utils::decode_int(raw_input, currentPosition);
            currentPosition += 4;
            indices[j] = NULL;
        }
    } else {
        indices = NULL;
        posIndices = NULL;
        offsetIndices = NULL;
    }

    mark->indices = indices;
    mark->sizeIndices = sizeIndex;
    mark->posIndices = posIndices;
    mark->offsetIndices = offsetIndices;
    mark->beginIndicesOffset = currentPosition;

//  boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
//          - start;
//  cerr << "Time loading file " << p << " = " << sec.count() * 1000
//          << " milliseconds" << endl;

    return mark;

}

TableStorage::TableStorage(bool readOnly, string pathDir, int maxFileSize,
                           int maxNFiles, MemoryManager<FileSegment> *bytesTracker, Stats &stats) :
    readOnly(readOnly), marks(), marksLoaded(), stats(stats) {
    strcpy(this->pathDir, pathDir.c_str());
    this->sizePathDir = strlen(this->pathDir);
    this->pathDir[sizePathDir++] = '/';

    //Determine the highest number of a file
    fs::path dir(pathDir);
    fs::directory_iterator end_iter;
    lastCreatedFile = 0;
    sizeLastCreatedFile = 0;
    if (fs::exists(dir) && fs::is_directory(dir)) {
        for (fs::directory_iterator dir_iter(dir); dir_iter != end_iter;
                ++dir_iter) {
            int idx = atoi(dir_iter->path().filename().c_str());
            if (lastCreatedFile < idx)
                lastCreatedFile = (short) idx;
        }

        cache = new FileManager<ComprFileDescriptor, FileSegment>(pathDir,
                readOnly, maxFileSize, maxNFiles, lastCreatedFile,
                bytesTracker, &stats);

        sizeLastCreatedFile = cache->sizeFile(lastCreatedFile);
    } else {
        //Create the directory if it does not exist
        if (!readOnly) {
            fs::create_directories(pathDir);
        }

        cache = new FileManager<ComprFileDescriptor, FileSegment>(pathDir,
                readOnly, maxFileSize, maxNFiles, lastCreatedFile,
                bytesTracker, &stats);
    }

    lastMark = -1;
    lastNextFile = lastFile = -1;
    lastPos = lastNextPos = -1;
    lastIndex = NULL;
    lastSector = -1;
    insertHandler = NULL;
    nTriplesInserted = 0;

    if (!readOnly) {
        writeMarks = new int*[MAX_N_FILES];
        sizeWriteMarks = new int[MAX_N_FILES];
        sizeUsedMarks = new int[MAX_N_FILES];
        for (int i = 0; i < MAX_N_FILES; ++i) {
            writeMarks[i] = NULL;
            indices[i] = NULL;
            sizeIndices[i] = 0;
            sizeWriteMarks[i] = 0;
            sizeUsedMarks[i] = 0;
        }
    } else {
        writeMarks = NULL;
        sizeWriteMarks = NULL;
        sizeUsedMarks = NULL;
    }

    fileCurrentIndex = -1;
    filePreviousIndex = 0;
    currentIndex = NULL;
}

std::string TableStorage::getPath() {
    return std::string(pathDir, sizePathDir);
}

void TableStorage::setupPairHandler(PairHandler *handler, short file,
                                    int mark) {
    //Caching
    if (file != lastFile || mark != lastMark) {
        lastMark = mark;
        lastFile = file;
        if (!marksLoaded[file]) {
            sprintf(pathDir + sizePathDir, "%d.idx", file);
            marks[file] = parseIndexFile(pathDir, file);
            marksLoaded[file] = true;
        }
        lastPos = marks[file]->getPos(mark, lastSector);
        short nextFile = file;
        int nextPosition;
        if (marks[file]->sizeMarks == mark + 1) {
            if (nextFile < lastCreatedFile) {
                do {
                    nextFile++;
                    if (nextFile <= lastCreatedFile && !marksLoaded[nextFile]) {
                        sprintf(pathDir + sizePathDir, "%d.idx", nextFile);
                        marks[nextFile] = parseIndexFile(pathDir, nextFile);
                        marksLoaded[nextFile] = true;
                    }
                } while (marks[nextFile] == NULL && nextFile < lastCreatedFile);
                if (marks[nextFile] != NULL) {
                    lastSector = -1;
                    nextPosition = marks[nextFile]->getPos(0, lastSector);
                } else {
                    nextPosition = sizeLastCreatedFile;
                }
            } else {
                nextPosition = sizeLastCreatedFile;
            }
        } else {
            lastSector = -1;
            nextPosition = marks[file]->getPos(mark + 1, lastSector);
        }
        lastNextFile = nextFile;
        lastNextPos = nextPosition;
        lastIndex =
            marks[file]->indices != NULL ?
            marks[file]->getIndex(mark) : NULL;
    }
    handler->init(cache, lastIndex);
    handler->setBasePosition(file, lastPos);
    handler->setPosition(file, lastPos);

    //For now, I always assume a gap of zero.
    //  nextPosition -= marks[file][mark2 + 1];
    handler->setLimitPosition(lastNextFile, lastNextPos);
    handler->start_reading();
}

int TableStorage::startAppend(PairHandler *handler) {
    // Mark a new entry
    if (writeMarks[lastCreatedFile] == NULL) {
        writeMarks[lastCreatedFile] = new int[1000000];
        sizeWriteMarks[lastCreatedFile] = 1000000;
        sizeUsedMarks[lastCreatedFile] = 0;
    }
    if (sizeWriteMarks[lastCreatedFile] <= sizeUsedMarks[lastCreatedFile] * 2) {
        int newSize = sizeWriteMarks[lastCreatedFile] * 2;
        int *newArray = new int[newSize];
        //Copy it
        memcpy(newArray, writeMarks[lastCreatedFile],
               sizeof(int) * sizeUsedMarks[lastCreatedFile] * 2);
        delete[] writeMarks[lastCreatedFile];
        writeMarks[lastCreatedFile] = newArray;
        sizeWriteMarks[lastCreatedFile] = newSize;
    }
    writeMarks[lastCreatedFile][sizeUsedMarks[lastCreatedFile] * 2] =
        sizeLastCreatedFile;
    writeMarks[lastCreatedFile][sizeUsedMarks[lastCreatedFile] * 2 + 1] = 0;

    // Record the file/position to store a possible index
    fileCurrentIndex = lastCreatedFile;

    // Set up the pairhandler
    if (currentIndex == NULL)
        currentIndex = new FileIndex();
    handler->setup(cache, lastCreatedFile, sizeLastCreatedFile, currentIndex);
    handler->setBasePosition(lastCreatedFile, sizeLastCreatedFile);
    handler->startAppend();

    insertHandler = handler;
    return sizeUsedMarks[lastCreatedFile]++;
}

void TableStorage::append(long t1, long t2) {
    nTriplesInserted++;
    insertHandler->appendPair(t1, t2);
}

long TableStorage::getNTriplesInserted() {
    return nTriplesInserted;
}

void TableStorage::stopAppend() {
    insertHandler->stopAppend();

    // Add the index if exists
    int idx = sizeUsedMarks[fileCurrentIndex] - 1;
    //Check whether there is space for it
    if (idx >= sizeIndices[fileCurrentIndex]) {
        //Increase it
        int oldsize = sizeIndices[fileCurrentIndex];
        int newsize = max(1024, idx * 2);
        FileIndex **newarray = new FileIndex*[newsize];
        if (oldsize > 0) {
            memcpy(newarray, indices[fileCurrentIndex],
                   sizeof(FileIndex*) * oldsize);
            delete[] indices[fileCurrentIndex];
        }
        indices[fileCurrentIndex] = newarray;
        sizeIndices[fileCurrentIndex] = newsize;
    }

    if (!currentIndex->isEmpty()) {
        indices[fileCurrentIndex][idx] = currentIndex;
        currentIndex = NULL;
    } else {
        indices[fileCurrentIndex][idx] = NULL;
    }

    lastCreatedFile = insertHandler->getCurrentFile();
    sizeLastCreatedFile = insertHandler->getCurrentPosition();
    insertHandler->cleanup();
    insertHandler = NULL;

    //Should I write the previous entries and free some memory?
    if (filePreviousIndex != fileCurrentIndex) {
        //Store filePreviousIndex
        sprintf(pathDir + sizePathDir, "%d.idx", filePreviousIndex);
        storeFileIndex(writeMarks[filePreviousIndex],
                       sizeUsedMarks[filePreviousIndex], indices[filePreviousIndex],
                       string(pathDir));

        //Delete indices and writeMarks to save memory
        FileIndex **index = indices[filePreviousIndex];
        if (index != NULL) {
            for (int j = 0; j < sizeUsedMarks[filePreviousIndex]; ++j) {
                if (index[j] != NULL) {
                    delete index[j];
                }
            }
            delete[] index;
            indices[filePreviousIndex] = NULL;
        }
        delete[] writeMarks[filePreviousIndex];
        writeMarks[filePreviousIndex] = NULL;
    }
    filePreviousIndex = fileCurrentIndex;
}

void TableStorage::storeFileIndices() {
    for (int i = 0; i < MAX_N_FILES; ++i) {
        int *fileMarks = writeMarks[i];
        if (fileMarks != NULL) {
            sprintf(pathDir + sizePathDir, "%d.idx", i);
            storeFileIndex(fileMarks, sizeUsedMarks[i], indices[i],
                           string(pathDir));
        }
    }
}

void TableStorage::storeFileIndex(int *fileMarks, int sizeMarks,
                                  FileIndex **indices, string pathFile) {
    ofstream oFile(pathFile);
    char supportArray[16];

    // Total number of entries
    Utils::encode_int(supportArray, 0, sizeMarks);
    oFile.write(supportArray, 4);

    // Build the pos->sector mapping
    vector<int> positions;
    vector<short> sectors;
    int currentBlock = 0;
    for (int i = 0; i < sizeMarks * 2; i += 2) {
        int sector = fileMarks[i] >> 16;
        if (sector != currentBlock) {
            positions.push_back(i / 2);
            sectors.push_back((short) sector);
            currentBlock = sector;
        }
    }
    // Write the pos->sector mapping
    Utils::encode_short(supportArray, positions.size());
    oFile.write(supportArray, 2);
    for (int i = 0; i < positions.size(); ++i) {
        Utils::encode_int(supportArray, 0, positions[i]);
        Utils::encode_short(supportArray + 4, sectors[i]);
        oFile.write(supportArray, 6);
    }

    // Write the mapping between indices and sectors
    for (int i = 0; i < sizeMarks * 2; i += 2) {
        Utils::encode_short(supportArray, fileMarks[i]);
        oFile.write(supportArray, 2);
        // Assume that the gap is 0. Otherwise return exception
        if (fileMarks[i + 1] != 0) {
            oFile.close();
            BOOST_LOG_TRIVIAL(error) << "gap != 0. Not supported";
            throw 10;
        }
    }

    // Find which indices to write
    vector<int> indicesToWrite;
    if (indices != NULL)
        for (int i = 0; i < sizeMarks; i++) {
            if (indices[i] != NULL) {
                indicesToWrite.push_back(i);
            }
        }

    Utils::encode_int(supportArray, 0, indicesToWrite.size());
    oFile.write(supportArray, 4);

    int sizeSupportVector = 1024;
    int offsetSupportVector = 0;
    char *supportVector = new char[sizeSupportVector];
    for (int i = 0; i < indicesToWrite.size(); ++i) {
        int p = indicesToWrite[i];
        Utils::encode_int(supportArray, 0, p);
        Utils::encode_int(supportArray, 4, offsetSupportVector);
        oFile.write(supportArray, 8);

        supportVector = indices[p]->serialize(supportVector,
                                              offsetSupportVector, sizeSupportVector);
    }
    oFile.write(supportVector, offsetSupportVector);
    oFile.close();
    delete[] supportVector;
}

TableStorage::~TableStorage() {

    if (currentIndex != NULL) {
        delete currentIndex;
    }

    //Store file indices
    if (!readOnly) {
        storeFileIndices();
    }

    for (int i = 0; i <= lastCreatedFile; ++i) {
        if (marks[i] != NULL) {
            delete marks[i];
        }

        if (!readOnly) {
            if (writeMarks != NULL && writeMarks[i] != NULL) {
                delete[] writeMarks[i];
            }

            FileIndex **index = indices[i];
            if (index != NULL) {
                for (int j = 0; j < sizeUsedMarks[i]; ++j) {
                    if (index[j] != NULL) {
                        delete index[j];
                    }
                }
                delete[] index;
            }
        }
    }

    if (writeMarks != NULL) {
        delete[] writeMarks;
        delete[] sizeWriteMarks;
        delete[] sizeUsedMarks;
    }

    delete cache;
}

FileIndex *FileMarks::getIndex(const int mark) {
    int b = 0;
    int e = sizeIndices - 1;
    while (b <= e) {
        int pivot = (e + b) >> 1;
        if (posIndices[pivot] < mark) {
            b = pivot + 1;
        } else if (posIndices[pivot] > mark) {
            e = pivot - 1;
        } else {
            if (indices[pivot] == NULL) {
                //Load index
                FileIndex *idx = new FileIndex();
                int p = beginIndicesOffset + offsetIndices[pivot];
                idx->unserialize(offsetSectors, &p);
                indices[pivot] = idx;
            }
            return indices[pivot];
        }
    }
    return NULL;
}
