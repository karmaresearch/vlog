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

#ifndef FILEMANAGER_H_
#define FILEMANAGER_H_

#include <trident/memory/memorymgr.h>
#include <trident/kb/consts.h>
#include <trident/kb/statistics.h>

#include <boost/log/trivial.hpp>

#include <list>
#include <string>
#include <sstream>
#include <vector>
#include <iostream>

using namespace std;

template<class T, class K>
class FileManager {
private:
    const bool readOnly;
    const std::string cacheDir;
    const int fileMaxSize;
    const int maxFiles;
    int lastFileId;
    int nOpenedFiles;

    MemoryManager<K> *bytesTracker;
    T *openedFiles[MAX_N_FILES];
    list<int> trackerOpenedFiles;

    //Cache the uncompressed size of file used during the writing
    vector<size_t> cacheFileSize;

    int sessions[MAX_SESSIONS];
    int lastSession;

    Stats* const stats;

    bool isFileLoaded(const int id) {
        return openedFiles[id] != NULL;
    }

    void load_file(const int id) {
        if (!isFileLoaded(id)) {
            if (nOpenedFiles == maxFiles) {
                //Take the last opened file
                int idxFileToRemove = trackerOpenedFiles.front();
                int firstFileRemoved = -1;
                trackerOpenedFiles.pop_front();
                bool rem = true;
                while (openedFiles[idxFileToRemove]->isUsed()) {
                    trackerOpenedFiles.push_back(idxFileToRemove);
                    if (firstFileRemoved == -1) {
                        firstFileRemoved = idxFileToRemove;
                    }

                    idxFileToRemove = trackerOpenedFiles.front();
                    trackerOpenedFiles.pop_front();
                    if (idxFileToRemove == firstFileRemoved) {
                        rem = false;
                        break;
                    }
                }
                if (rem) {
                    delete openedFiles[idxFileToRemove];
                    openedFiles[idxFileToRemove] = NULL;
                    nOpenedFiles--;
                }
            }

            std::stringstream filePath;
            filePath << cacheDir << "/" << id;
            T* f = new T(readOnly, id, filePath.str(), fileMaxSize,
                         bytesTracker, openedFiles, stats);
            openedFiles[id] = f;
            trackerOpenedFiles.push_back(id);
            nOpenedFiles++;
        }
    }
public:
    FileManager(std::string path, bool readOnly, int fileMaxSize,
                int maxNumberFiles, int lastFileId, MemoryManager<K> *bytesTracker,
                Stats * const stats) :
        readOnly(readOnly), cacheDir(path), fileMaxSize(fileMaxSize),
        maxFiles(maxNumberFiles), lastFileId(lastFileId), bytesTracker(bytesTracker),
        stats(stats) {
        for (int i = 0; i < MAX_N_FILES; ++i) {
            openedFiles[i] = NULL;
        }

        lastSession = 0;
        for (int i = 0; i < MAX_SESSIONS; ++i) {
            sessions[i] = FREE_SESSION;
        }
        nOpenedFiles = 0;
    }

    char* getBuffer(short id, int offset, int *length) {
        load_file(id);
        return openedFiles[id]->getBuffer(offset, length);
    }

    char* getBuffer(short id, int offset, int *length, int sessionId) {
        load_file(id);
        int memoryBlock;

        char *result = openedFiles[id]->getBuffer(offset, length, memoryBlock);

        if (sessionId != EMPTY_SESSION && sessions[sessionId] != memoryBlock) {
            //Tell the memory manager that we don't need this block anymore
            if (sessions[sessionId] >= 0) {
                bytesTracker->releaseLock(sessions[sessionId]);
            }

            sessions[sessionId] = memoryBlock;

            if (memoryBlock >= 0) {
                bytesTracker->addLock(memoryBlock);
            }
        }

        return result;
    }

    int newSession() {
        int cnt = 0;
        while (sessions[lastSession] != FREE_SESSION) {
            lastSession = (lastSession + 1) % MAX_SESSIONS;
            cnt++;
            if (cnt > MAX_SESSIONS) {
                BOOST_LOG_TRIVIAL(error) << "Max number of sessions is reached";
                throw 10;
            }
        }
	sessions[lastSession] = EMPTY_SESSION;
	cnt = lastSession;
	lastSession = (lastSession + 1) % MAX_SESSIONS;
	// BOOST_LOG_TRIVIAL(debug) << "This = " << this << ", Open session " << cnt;
        return cnt;
    }

    void closeSession(int idx) {
        //Release the lock
        if (sessions[idx] >= 0) {
            bytesTracker->releaseLock(sessions[idx]);
        }
	// BOOST_LOG_TRIVIAL(debug) << "This = " << this << ", Close session " << idx;
        sessions[idx] = FREE_SESSION;
    }

    int sizeFile(const int idx) {
        if (isFileLoaded(idx)) {
            return openedFiles[idx]->getFileLength();
        }

        if (idx < cacheFileSize.size() && cacheFileSize[idx] != 0) {
            return cacheFileSize[idx];
        }

        load_file(idx);
        int size =  openedFiles[idx]->getFileLength();

        if (idx >= cacheFileSize.size()) {
            cacheFileSize.resize(idx + 1, 0);
        }
        cacheFileSize[idx] = size;
        return size;
    }

    int sizeLastFile() {
        return sizeFile(lastFileId);
    }


    void shiftRemainingFile(int idx, int pos, int diff) {
        load_file(idx);
        openedFiles[idx]->shiftFile(pos, diff);
    }

    int getIdLastFile() {
        return lastFileId;
    }

    int getFileMaxSize() {
        return fileMaxSize;
    }

    short createNewFile() {
        lastFileId++;
        if (lastFileId == MAX_N_FILES) {
            BOOST_LOG_TRIVIAL(error) << "Max number of files is reached";
            throw 10;
        }
        load_file(lastFileId);
        return (short) lastFileId;
    }

    void append(char *bytes, int size) {
        load_file(lastFileId);
        openedFiles[lastFileId]->append(bytes, size);
    }

    int appendVLong(long n) {
        load_file(lastFileId);
        return openedFiles[lastFileId]->appendVLong(n);

    }

    int appendVLong2(long n) {
        load_file(lastFileId);
        return openedFiles[lastFileId]->appendVLong2(n);
    }

    void appendLong(long n) {
        load_file(lastFileId);
        openedFiles[lastFileId]->appendLong(n);
    }

    void reserveBytes(const uint8_t bytes) {
        load_file(lastFileId);
        openedFiles[lastFileId]->reserveBytes(bytes);
    }

    void overwriteAt(short file, int pos, char byte) {
        load_file(file);
        openedFiles[file]->overwriteAt(pos, byte);
    }

    void overwriteVLong2At(short file, int pos, long number) {
        load_file(file);
        openedFiles[file]->overwriteVLong2At(pos, number);
    }

    ~FileManager() {
        for (int i = 0; i < MAX_N_FILES; ++i) {
            if (openedFiles[i] != NULL) {
                delete openedFiles[i];
            }
        }
    }
};

#endif /* FILEMANAGER_H_ */
