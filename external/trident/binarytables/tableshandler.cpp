#include <trident/binarytables/tableshandler.h>
#include <trident/binarytables/binarytable.h>

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

FileMarks *parseIndexFile(char* p) {
    if (!fs::exists(p)) {
        return NULL;
    }

//  timens::system_clock::time_point start = timens::system_clock::now();

    FileMarks *mark = new FileMarks;
    mark->filemapping = new bip::file_mapping(p, bip::read_only);
    mark->mappedrgn = new bip::mapped_region(*(mark->filemapping),
            bip::read_only);
    char *raw_input = static_cast<char*>(mark->mappedrgn->get_address());

    //Size marks
    mark->sizeMarks = Utils::decode_int(raw_input, 0);

    //Parse the sectors
    mark->sizeSectors = Utils::decode_short(raw_input, 4);
    int currentPosition = 6;
    //Loop max 5K
    for (int i = 0; i < mark->sizeSectors; ++i) {
        mark->posSectors[i] = Utils::decode_int(raw_input, currentPosition);
        currentPosition += 4;
        mark->sectors[i] = Utils::decode_short((const char*) raw_input,
                                               currentPosition);
        currentPosition += 2;
    }

    //mark->beginMarksOffset = currentPosition;
    mark->setBeginSectors(raw_input + currentPosition);
    currentPosition += (2 + 9) * mark->sizeMarks;

    //Parse the index
    int sizeIndex = Utils::decode_int(raw_input, currentPosition);
    currentPosition += 4;

    FileIndex **indices = NULL;
    //int *posIndices = NULL;
    //int *offsetIndices = NULL;

    mark->setBeginCoordinatesIndices(raw_input + currentPosition);
    mark->sizeIndices = sizeIndex;

    if (sizeIndex > 0) {
        indices = new FileIndex*[sizeIndex];
        //posIndices = new int[sizeIndex];
        //offsetIndices = new int[sizeIndex];
        for (int j = 0; j < sizeIndex; j++) {
            //posIndices[j] = Utils::decode_int(raw_input, currentPosition);
            //currentPosition += 4;
            //offsetIndices[j] = Utils::decode_int(raw_input, currentPosition);
            //currentPosition += 4;
            indices[j] = NULL;
        }
        currentPosition += sizeIndex * 8;
    }
    mark->setBeginIndices(raw_input + currentPosition);
    mark->indices = indices;
    //mark->posIndices = posIndices;
    //mark->offsetIndices = offsetIndices;

    //mark->beginIndices = raw_input + currentPosition;

//  boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
//          - start;
//  cerr << "Time loading file " << p << " = " << sec.count() * 1000
//          << " milliseconds" << endl;

    return mark;

}

TableStorage::TableStorage(bool readOnly, string pathDir, int maxFileSize,
                           int maxNFiles, MemoryManager<FileDescriptor> *bytesTracker, Stats &stats) :
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

        cache = new FileManager<FileDescriptor, FileDescriptor>(pathDir,
                readOnly, maxFileSize, maxNFiles, lastCreatedFile,
                bytesTracker, &stats);

        sizeLastCreatedFile = cache->sizeFile(lastCreatedFile);
    } else {
        //Create the directory if it does not exist
        if (!readOnly) {
            fs::create_directories(pathDir);
        }

        cache = new FileManager<FileDescriptor, FileDescriptor>(pathDir,
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
        keyTables = new long*[MAX_N_FILES];
        strats = new char*[MAX_N_FILES];
        sizeWriteMarks = new int[MAX_N_FILES];
        sizeUsedMarks = new int[MAX_N_FILES];
        for (int i = 0; i < MAX_N_FILES; ++i) {
            writeMarks[i] = NULL;
            keyTables[i] = NULL;
            strats[i] = NULL;
            indices[i] = NULL;
            sizeIndices[i] = 0;
            sizeWriteMarks[i] = 0;
            sizeUsedMarks[i] = 0;
        }
    } else {
        writeMarks = NULL;
        keyTables = NULL;
        strats = NULL;
        sizeWriteMarks = NULL;
        sizeUsedMarks = NULL;
    }

    fileCurrentIndex = -1;
    filePreviousIndex = 0;
    currentIndex = NULL;
}

bool TableStorage::doesFileHaveCoordinates(short file) {
    if (!marksLoaded[file]) {
        sprintf(pathDir + sizePathDir, "%d.idx", file);
        marks[file] = parseIndexFile(pathDir);
        marksLoaded[file] = true;
    }
    return marks[file] != NULL;
}

const char *TableStorage::getBeginTableCoordinates(short file) {
    if (!marksLoaded[file]) {
        sprintf(pathDir + sizePathDir, "%d.idx", file);
        marks[file] = parseIndexFile(pathDir);
        marksLoaded[file] = true;
    }
    return marks[file]->getBeginTableCoordinates();
}

const char *TableStorage::getEndTableCoordinates(short file) {
    if (!marksLoaded[file]) {
        sprintf(pathDir + sizePathDir, "%d.idx", file);
        marks[file] = parseIndexFile(pathDir);
        marksLoaded[file] = true;
    }
    return marks[file]->getEndTableCoordinates();
}

std::string TableStorage::getPath() {
    return std::string(pathDir, sizePathDir);
}

void TableStorage::setupPairHandler(BinaryTable *handler, short file,
                                    int mark) {
    //Caching
    if (file != lastFile || mark != lastMark) {
        lastMark = mark;
        lastFile = file;
        if (!marksLoaded[file]) {
            sprintf(pathDir + sizePathDir, "%d.idx", file);
            marks[file] = parseIndexFile(pathDir);
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
                        marks[nextFile] = parseIndexFile(pathDir);
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
    handler->first();
}

int TableStorage::startAppend(const long key,
                              const char strat,
                              BinaryTableInserter *handler) {

    // Mark a new entry
    if (writeMarks[lastCreatedFile] == NULL) {
        writeMarks[lastCreatedFile] = new int[1000000];
        keyTables[lastCreatedFile] = new long[1000000];
        strats[lastCreatedFile] = new char[1000000];
        sizeWriteMarks[lastCreatedFile] = 1000000;
        sizeUsedMarks[lastCreatedFile] = 0;
    }
    if (sizeWriteMarks[lastCreatedFile] <= sizeUsedMarks[lastCreatedFile] * 2) {
        int newSize = sizeWriteMarks[lastCreatedFile] * 2;
        int *newArray = new int[newSize];
        long *newArray2 = new long[newSize];
        char *newArray3 = new char[newSize];
        //Copy it
        memcpy(newArray, writeMarks[lastCreatedFile],
               sizeof(int) * sizeUsedMarks[lastCreatedFile] * 2);
        memcpy(newArray2, keyTables[lastCreatedFile],
               sizeof(long) * sizeUsedMarks[lastCreatedFile]);
        memcpy(newArray3, strats[lastCreatedFile],
               sizeof(char) * sizeUsedMarks[lastCreatedFile]);
        delete[] writeMarks[lastCreatedFile];
        delete[] keyTables[lastCreatedFile];
        delete[] strats[lastCreatedFile];
        writeMarks[lastCreatedFile] = newArray;
        keyTables[lastCreatedFile] = newArray2;
        strats[lastCreatedFile] = newArray3;
        sizeWriteMarks[lastCreatedFile] = newSize;
    }
    writeMarks[lastCreatedFile][sizeUsedMarks[lastCreatedFile] * 2] =
        sizeLastCreatedFile;
    writeMarks[lastCreatedFile][sizeUsedMarks[lastCreatedFile] * 2 + 1] = 0;
    keyTables[lastCreatedFile][sizeUsedMarks[lastCreatedFile]] = key;
    strats[lastCreatedFile][sizeUsedMarks[lastCreatedFile]] = strat;
    // Record the file/position to store a possible index
    fileCurrentIndex = lastCreatedFile;

    // Set up the pairhandler
    if (currentIndex == NULL)
        currentIndex = new FileIndex();
    handler->setup(cache, /*lastCreatedFile, sizeLastCreatedFile,*/ currentIndex);
    handler->setBasePosition(lastCreatedFile, sizeLastCreatedFile);
    handler->startAppend();

    insertHandler = handler;
    return sizeUsedMarks[lastCreatedFile]++;
}

void TableStorage::setStrategy(const char strat) {
    strats[lastCreatedFile][sizeUsedMarks[lastCreatedFile] - 1] = strat;
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
                       keyTables[filePreviousIndex],
                       strats[filePreviousIndex],
                       sizeUsedMarks[filePreviousIndex],
                       indices[filePreviousIndex],
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
        delete[] keyTables[filePreviousIndex];
        delete[] strats[filePreviousIndex];
        writeMarks[filePreviousIndex] = NULL;
        keyTables[filePreviousIndex] = NULL;
        strats[filePreviousIndex] = NULL;
    }
    filePreviousIndex = fileCurrentIndex;
}

void TableStorage::storeFileIndices() {
    for (int i = 0; i < MAX_N_FILES; ++i) {
        int *fileMarks = writeMarks[i];
        if (fileMarks != NULL) {
            sprintf(pathDir + sizePathDir, "%d.idx", i);
            storeFileIndex(fileMarks,
                           keyTables[i],
                           strats[i],
                           sizeUsedMarks[i], indices[i],
                           string(pathDir));
        }
    }
}

void TableStorage::storeFileIndex(int *fileMarks,
                                  long *keys,
                                  char *strats,
                                  int sizeMarks,
                                  FileIndex **indices,
                                  string pathFile) {
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
    if (positions.size() > MAX_N_SECTORS) {
        BOOST_LOG_TRIVIAL(error) << "Too many sectors";
        throw 10;
    }
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
        Utils::encode_long(supportArray, 2, keys[i / 2]);
        supportArray[10] = strats[i / 2];
        oFile.write(supportArray, 11);
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
            if (keyTables != NULL && keyTables[i] != NULL) {
                delete[] keyTables[i];
            }
            if (strats != NULL && strats[i] != NULL) {
                delete[] strats[i];
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
        delete[] keyTables;
        delete[] strats;
        delete[] sizeWriteMarks;
        delete[] sizeUsedMarks;
    }

    delete cache;
}

FileIndex *FileMarks::getIndex(const int mark) {
    //int b = 0;
    //int e = sizeIndices - 1;

    //timens::system_clock::time_point start = timens::system_clock::now();

    const char* b = beginCoordinatesIndices;
    const char* e = beginCoordinatesIndices + sizeIndices * 8;

    while (b <= e) {
        const char* pivot = b + ((e - b) / 8 >> 1) * 8;
        const int idx = Utils::decode_int(pivot);
        if (idx < mark) {
            b = pivot + 8;
        } else if (idx > mark) {
            e = pivot - 8;
        } else {
            const int idxPivot = (pivot - beginCoordinatesIndices) / 8;
            if (indices[idxPivot] == NULL) {
                //Load index
                FileIndex *idx = new FileIndex();
                int p = Utils::decode_int(pivot + 4);
                idx->unserialize(beginIndices, &p);
                indices[idxPivot] = idx;
            }
            //boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
            //                                      - start;
            //BOOST_LOG_TRIVIAL(info) << "Time search (found) " << sec.count();
            return indices[idxPivot];
        }
    }

    //boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
    //                                      - start;
    //BOOST_LOG_TRIVIAL(info) << "Time search " << sec.count();
    return NULL;
}
