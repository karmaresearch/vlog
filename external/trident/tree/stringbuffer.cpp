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

#include <trident/tree/stringbuffer.h>

#include <lz4.h>
#include <lz4hc.h>
#include <boost/filesystem.hpp>
#include <boost/log/trivial.hpp>
#include <string>
#include <cstring>
#include <algorithm>

using namespace std;
namespace fs = boost::filesystem;

char StringBuffer::FINISH_THREAD[1];

//Domain EMPTY_DOMAIN(NULL, -1);
//Domain DEL_DOMAIN(NULL, -2);

StringBuffer::StringBuffer(string dir, bool readOnly, int factorySize,
                           long cacheSize, Stats *stats) :
    dir(dir), factory(SB_BLOCK_SIZE, 2, factorySize), readOnly(readOnly), maxElementsInCache(
        max(3, (int) (cacheSize / SB_BLOCK_SIZE))) {

    //BOOST_LOG_TRIVIAL(debug)<< "Init string buffer: factorySize=" << factorySize << " maxElementsInCache=" << maxElementsInCache;

    uncompressedSize = 0;
    writingCurrentBufferSize = 0;
    bufferToCompress = NULL;
    sizeBufferToCompress = 0;
    elementsInCache = 0;
    this->stats = stats;

    sizeBaseEntry = 0;
    posBaseEntry = 0;
    entriesSinceBaseEntry = 0;
    nMatchedChars = 0;

    ios_base::openmode mode =
        readOnly ?
        std::fstream::in :
        std::fstream::in | std::fstream::out | std::fstream::app;
    if (!fs::exists(fs::path(dir))) {
        fs::create_directories(fs::path(dir));
    }
    sb.open((dir + string("/sb")).c_str(), mode);

    if (readOnly) {
        //Load the size of the compressed blocks and initialize the other vector
        std::ifstream file(dir + string("/sb.idx"), std::ios::binary);
        file.read(reinterpret_cast<char*>(&uncompressedSize), sizeof(long));
        while (!file.eof()) {
            long pos;
            if (file.read(reinterpret_cast<char*>(&pos), sizeof(long))) {
                sizeCompressedBlocks.push_back(pos);
            }
        }
        blocks.resize(sizeCompressedBlocks.size());
        cacheVector.resize(sizeCompressedBlocks.size());
        file.close();
    } else {
        currentBuffer = factory.get();
        blocks.push_back(currentBuffer);
        addCache(0);

        //Start a compression thread
        compressionThread = boost::thread(
                                boost::bind(&StringBuffer::compressBlocks, this));
    }
}

void StringBuffer::addCache(int idx) {
    if (elementsInCache == maxElementsInCache) {
        int idxToRemove = cacheList.front();
        cacheList.pop_front();
        while (!readOnly && idxToRemove >= blocks.size() - 3) {
            cacheVector[idxToRemove] = cacheList.insert(cacheList.end(),
                                       idxToRemove);
            idxToRemove = cacheList.front();
            cacheList.pop_front();
        }
        factory.release(blocks[idxToRemove]);
        blocks[idxToRemove] = NULL;
        elementsInCache--;
    }

    if (idx == cacheVector.size()) {
        cacheVector.push_back(cacheList.insert(cacheList.end(), idx));
    } else {
        cacheVector[idx] = cacheList.insert(cacheList.end(), idx);
    }
    elementsInCache++;
}

void StringBuffer::compressBlocks() {
    char *compressedBuffer = new char[LZ4_compressBound(SB_BLOCK_SIZE)];
    while (true) {

        boost::unique_lock<boost::mutex> lock(_compressMutex);
        while (bufferToCompress == NULL) {
            compressWait.wait(lock);
        }
        char *currentUncompressedBuffer = bufferToCompress;
        int sCurrentUncompressedBuffer = sizeBufferToCompress;
        bufferToCompress = NULL;
        lock.unlock();

        compressWait.notify_one();

        if (currentUncompressedBuffer == FINISH_THREAD) {
            break;
        }

        int cs = LZ4_compressHC(currentUncompressedBuffer, compressedBuffer,
                                sCurrentUncompressedBuffer);

        long totalSize = 0;

        sizeLock.lock();
        if (sizeCompressedBlocks.size() == 0) {
            sizeCompressedBlocks.push_back(cs);
        } else {
            totalSize = sizeCompressedBlocks[sizeCompressedBlocks.size() - 1];
            sizeCompressedBlocks.push_back(totalSize + cs);
        }
        sizeLock.unlock();

        fileLock.lock();
        //Since I opened the file with app, all changes happen at the end
        sb.seekp(totalSize);
        sb.write(compressedBuffer, cs);
        fileLock.unlock();
    }
    delete[] compressedBuffer;
}

void StringBuffer::setCurrentAsBaseEntry(int size) {
    posBaseEntry = writingCurrentBufferSize;
    sizeBaseEntry = size;
    nMatchedChars = entriesSinceBaseEntry = 0;
}

int StringBuffer::calculatePrefixWithBaseEntry(char *origBuffer, char *string,
        int size) {
    if (size > 0 && entriesSinceBaseEntry < 1024) {
        int commonCharacters = Utils::commonPrefix((tTerm*) origBuffer,
                               posBaseEntry, posBaseEntry + sizeBaseEntry, (tTerm *) string, 0,
                               size);
        if (commonCharacters > 4 && commonCharacters >= this->nMatchedChars) {
            return commonCharacters;
        }
    }
    return 0;
}

void StringBuffer::writeVInt(int n) {
    int remSize = SB_BLOCK_SIZE - writingCurrentBufferSize;
    if (remSize < 4) {
        int nByteSize = Utils::numBytes2(n);
        if (remSize < nByteSize) {
            char supportBuffer[4];
            int offset = 0;
            int i = 0;
            offset = Utils::encode_vint2(supportBuffer, offset, n);
            while (writingCurrentBufferSize < SB_BLOCK_SIZE) {
                currentBuffer[writingCurrentBufferSize++] = supportBuffer[i++];
            }
            uncompressedSize += i;
            compressLastBlock();
            while (i < offset) {
                currentBuffer[writingCurrentBufferSize++] = supportBuffer[i++];
                uncompressedSize++;
            }

        } else {
            writingCurrentBufferSize = Utils::encode_vint2(currentBuffer,
                                       writingCurrentBufferSize, n);
            uncompressedSize += nByteSize;
        }
    } else {
        int oldSize = writingCurrentBufferSize;
        writingCurrentBufferSize = Utils::encode_vint2(currentBuffer,
                                   writingCurrentBufferSize, n);
        uncompressedSize += writingCurrentBufferSize - oldSize;
    }
}

void StringBuffer::append(char *string, int size) {

    char *origBuffer = currentBuffer;

    writeVInt(size);

    //Insert a flag of 0
    int remSize = SB_BLOCK_SIZE - writingCurrentBufferSize;
    if (remSize == 0) {
        compressLastBlock();
    }

    //Check if it is worth to refer to a previous entry
    int prefixSize = calculatePrefixWithBaseEntry(origBuffer, string, size);
    if (prefixSize == 0) {
        currentBuffer[writingCurrentBufferSize++] = 0;
        setCurrentAsBaseEntry(size);
    } else {
        currentBuffer[writingCurrentBufferSize++] = 1;
        writeVInt(posBaseEntry);
        writeVInt(prefixSize);
        size -= prefixSize;
        string += prefixSize;
        entriesSinceBaseEntry++;
        nMatchedChars = prefixSize;
    }

    remSize = SB_BLOCK_SIZE - writingCurrentBufferSize;
    if (remSize >= size) {
        memcpy(currentBuffer + writingCurrentBufferSize, string, size);
        writingCurrentBufferSize += size;
    } else {
        if (remSize > 0) {
            memcpy(currentBuffer + writingCurrentBufferSize, string, remSize);
            uncompressedSize += remSize;
            size -= remSize;
            writingCurrentBufferSize += remSize;
        }
        compressLastBlock();
        memcpy(currentBuffer, string + remSize, size);
        writingCurrentBufferSize = size;
    }
    uncompressedSize += size + 1;
}

long StringBuffer::getSize() {
    return uncompressedSize;
}

void StringBuffer::compressLastBlock() {
    boost::unique_lock<boost::mutex> lock(_compressMutex);
    while (bufferToCompress != NULL) {
        compressWait.wait(lock);
    }
    bufferToCompress = currentBuffer;
    sizeBufferToCompress = writingCurrentBufferSize;
    lock.unlock();

//Notify the compression thread in case it is waiting
    compressWait.notify_one();

    addCache(blocks.size());

    currentBuffer = factory.get();
    blocks.push_back(currentBuffer);
    writingCurrentBufferSize = 0;

    sizeBaseEntry = 0;
    entriesSinceBaseEntry = 0;
}

void StringBuffer::uncompressBlock(int b) {
    long start = 0;
    int length = 0;

    if (!readOnly) {

        sizeLock.lock();
        if (b > 0) {
            start = sizeCompressedBlocks[b - 1];
        }
        length = sizeCompressedBlocks[b] - start;
        sizeLock.unlock();

        fileLock.lock();
        sb.seekg(start);
        sb.read(uncompressSupportBuffer, length);
        if (!sb) {
            BOOST_LOG_TRIVIAL(error) << "error: only " << sb.gcount() << " could be read";
        }
        fileLock.unlock();
    } else {

        if (b > 0) {
            start = sizeCompressedBlocks[b - 1];
        }
        length = sizeCompressedBlocks[b] - start;

        sb.seekg(start);
        sb.read(uncompressSupportBuffer, length);
        if (!sb) {
            BOOST_LOG_TRIVIAL(error) << "error: only " << sb.gcount() << " could be read";
        }
    }

    char *uncompressedBuffer = factory.get();
    int sizeUncompressed = SB_BLOCK_SIZE;
    if (b == sizeCompressedBlocks.size() - 1) {
        sizeUncompressed = uncompressedSize % SB_BLOCK_SIZE;
    }
    int bytesUncompressed = LZ4_decompress_fast(uncompressSupportBuffer, uncompressedBuffer, sizeUncompressed);
    stats->incrNReadIndexBlocks();
    stats->addNReadIndexBytes(length);
    if (bytesUncompressed < 0) {
        BOOST_LOG_TRIVIAL(error) << "Decompression of block " << b << " has failed. Read at pos " << start << " with length " << length;
    }

    blocks[b] = uncompressedBuffer;
}

int StringBuffer::getVInt(int &blockId, char *&block, int &offset) {
    //Retrieve the size of the string. I use five instead of 4 because there is also a flag after it.
    if (SB_BLOCK_SIZE - offset < 4) {
        char supportBuffer[4];
        int offsetSupportBuffer = 0;
        for (int i = offset; i < SB_BLOCK_SIZE; ++i) {
            supportBuffer[offsetSupportBuffer++] = block[i];
        }

        char *nextBlock = NULL;
        if (blockId < blocks.size() - 1) {
            nextBlock = getBlock(blockId + 1);
            int startNextBlock = 0;
            while (offsetSupportBuffer < 4) {
                supportBuffer[offsetSupportBuffer++] =
                    nextBlock[startNextBlock++];
            }
        }

        int supportOffset = 0;
        int size = Utils::decode_vint2(supportBuffer, &supportOffset);

        //Update current position
        offset += supportOffset;
        if (offset >= SB_BLOCK_SIZE) {
            offset -= SB_BLOCK_SIZE;
            blockId++;
            block = nextBlock;
        }
        return size;
    } else {
        return Utils::decode_vint2(block, &offset);
    }
}

void StringBuffer::get(long pos, char* outputBuffer, int &size) {

    int idxBlock = pos / SB_BLOCK_SIZE;
    int initialIdx = idxBlock;

    char *block = getBlock(idxBlock);
    int start = pos - idxBlock * SB_BLOCK_SIZE;

    //Get the size
    size = getVInt(idxBlock, block, start);
    int sizeToCopy = size;
    //Ignore the flag
    int flag = getFlag(idxBlock, block, start);
    if (flag == 1) {
        int posPrefix = getVInt(idxBlock, block, start);
        int sizePrefix = getVInt(idxBlock, block, start);

        if (initialIdx != idxBlock) {
            char *baseTermBlock = getBlock(initialIdx);
            memcpy(outputBuffer, baseTermBlock + posPrefix, sizePrefix);
            block = getBlock(idxBlock); // It could be that the block got offloaded
        } else {
            memcpy(outputBuffer, block + posPrefix, sizePrefix);
        }
        //Copy the remaining size - sizePrefix bytes
        sizeToCopy -= sizePrefix;
        outputBuffer += sizePrefix;
    }

    if (start == SB_BLOCK_SIZE) {
        start = 0;
        idxBlock++;
        block = getBlock(idxBlock);
    }

    //Check whether the string is inside the block or not
    if (start + sizeToCopy > SB_BLOCK_SIZE) {
        int remSize = SB_BLOCK_SIZE - start;
        memcpy(outputBuffer, block + start, remSize);
        idxBlock++;
        block = getBlock(idxBlock);
        memcpy(outputBuffer + remSize, block, sizeToCopy - remSize);
    } else {
        memcpy(outputBuffer, block + start, sizeToCopy);
    }
}

char* StringBuffer::get(long pos, int &size) {
    get(pos, termSupportBuffer, size);
    termSupportBuffer[size] = '\0';
    return termSupportBuffer;
}

char *StringBuffer::getBlock(int idxBlock) {
    char *block = blocks[idxBlock];
    if (block == NULL) {
        addCache(idxBlock);
        uncompressBlock(idxBlock);
        block = blocks[idxBlock];
    } else {
        //Update the cache
        if (cacheVector[idxBlock] != cacheList.end()) {
            cacheList.splice(cacheList.end(), cacheList, cacheVector[idxBlock]);
        }
    }
    return block;
}

int StringBuffer::cmp(long pos, char *string, int sizeString) {
    int startBlock = pos / SB_BLOCK_SIZE;
    const int initialBlock = startBlock;
    char *block = getBlock(startBlock);

    int blockStartPos = pos % SB_BLOCK_SIZE;
    int stringStart = 0;

    //Get the size of the term
    int size = getVInt(startBlock, block, blockStartPos);
    int flag = getFlag(startBlock, block, blockStartPos);
    if (flag == 1) {
        int posBaseTerm = getVInt(startBlock, block, blockStartPos);
        int sizeBaseTerm = getVInt(startBlock, block, blockStartPos);
        if (initialBlock != startBlock) {
            int result = Utils::prefixEquals(blocks[initialBlock] + posBaseTerm,
                                             sizeBaseTerm, string, sizeString);
            if (result != 0) {
                return result;
            }
        } else {
            //Do the comparison
            int result = Utils::prefixEquals(block + posBaseTerm, sizeBaseTerm,
                                             string, sizeString);
            if (result != 0) {
                return result;
            }
        }
        string += sizeBaseTerm;
        sizeString -= sizeBaseTerm;
        size -= sizeBaseTerm;
    }

    int stringBeginningCmp = stringStart;
    if (blockStartPos + size <= SB_BLOCK_SIZE) {
        for (int i = 0; i < size && stringStart < sizeString; ++i) {
            if (block[blockStartPos + i] != string[stringStart++]) {
                return ((int) block[blockStartPos + i] & 0xff) - ((int) string[stringStart - 1] & 0xff);
            }
        }
    } else {
        int remSize = SB_BLOCK_SIZE - blockStartPos;
        for (int i = 0; i < remSize && stringStart < sizeString; ++i) {
            if (block[blockStartPos + i] != string[stringStart++]) {
                return ((int) block[blockStartPos + i] & 0xff) - ((int) string[stringStart - 1] & 0xff);
            }
        }
        startBlock++;
        block = getBlock(startBlock);
        size -= remSize;
        stringBeginningCmp = stringStart;
        for (int i = 0; i < size && stringStart < sizeString; ++i) {
            if (block[i] != string[stringStart++]) {
                return block[i] - string[stringStart - 1];
                return ((int) block[i] & 0xff) - ((int) string[stringStart - 1] & 0xff);
            }
        }
    }
    return size - sizeString + stringBeginningCmp;
}

//int StringBuffer::cmp(long pos, char *string) {
//  int startBlock = pos / SB_BLOCK_SIZE;
//  const int initialBlock = startBlock;
//  char *block = getBlock(startBlock);
//  int blockStartPos = pos % SB_BLOCK_SIZE;
//  int stringStart = 0;
//  const char endingChar = '\0';
//
//  int size = getVInt(startBlock, block, blockStartPos);
//  int flag = getFlag(startBlock, block, blockStartPos);
//
//  if (flag == 1) {
//      int posBaseTerm = getVInt(startBlock, block, blockStartPos);
//      int sizeBaseTerm = getVInt(startBlock, block, blockStartPos);
//      if (initialBlock != startBlock) {
//          int result = Utils::prefixEquals(blocks[initialBlock] + posBaseTerm,
//                  sizeBaseTerm, string);
//          if (result != 0) {
//              return result;
//          }
//      } else {
//          //Do the comparison
//          int result = Utils::prefixEquals(block + posBaseTerm, sizeBaseTerm,
//                  string);
//          if (result != 0) {
//              return result;
//          }
//      }
//      string += sizeBaseTerm;
//      size -= sizeBaseTerm;
//  }
//
//  if (blockStartPos + size <= SB_BLOCK_SIZE) {
//      int i = 0;
//      for (; i < size && string[stringStart] != endingChar; ++i) {
//          if (block[blockStartPos + i] != string[stringStart++]) {
//              return block[blockStartPos + i] - string[stringStart - 1];
//          }
//      }
//      if (i == size && string[stringStart] == endingChar) {
//          return 0;
//      } else if (i < size) {
//          return 1;
//      } else {
//          return -1;
//      }
//  } else {
//      int remSize = SB_BLOCK_SIZE - blockStartPos;
//      for (int i = 0; i < remSize && string[stringStart] != endingChar; ++i) {
//          if (block[blockStartPos + i] != string[stringStart++]) {
//              return block[blockStartPos + i] - string[stringStart - 1];
//          }
//      }
//      startBlock++;
//      block = getBlock(startBlock);
//      size -= remSize;
//      int i = 0;
//      for (; i < size && string[stringStart] != endingChar; ++i) {
//          if (block[i] != string[stringStart++]) {
//              return block[i] - string[stringStart - 1];
//          }
//      }
//
//      if (i == size && string[stringStart] == endingChar) {
//          return 0;
//      } else if (i < size) {
//          return 1;
//      } else {
//          return -1;
//      }
//  }
//}

StringBuffer::~StringBuffer() {
    if (!readOnly) {
        int usedSize = uncompressedSize % SB_BLOCK_SIZE;
        if (usedSize > 0) {
            compressLastBlock();
        }

        boost::unique_lock<boost::mutex> lock(_compressMutex);
        while (bufferToCompress != NULL) {
            compressWait.wait(lock);
        }
        lock.unlock();

        bufferToCompress = FINISH_THREAD;
        compressWait.notify_one();
        compressionThread.join();

        sb.flush();
        sb.close();

        //Write all the indices
        std::ofstream file(dir + string("/sb.idx"), std::ios::binary);
        file.write(reinterpret_cast<char*>(&uncompressedSize), sizeof(long));
        for (int i = 0; i < sizeCompressedBlocks.size(); ++i) {
            file.write(reinterpret_cast<char*>(&(sizeCompressedBlocks[i])),
                       sizeof(long));
        }
        file.close();

//      //Read the domain table
//      std::ofstream domFile(dir + string("/sb.dom"), std::ios::binary);
//      BOOST_LOG_TRIVIAL(debug)<< "Writing " << domainMap.size() << " domains.";
//      for (DomainToNumberMap::iterator itr = domainMap.begin();
//              itr != domainMap.end(); itr++) {
//          int v = itr->second;
//          domFile.write(reinterpret_cast<char*>(&v), sizeof(int));
//          v = itr->first.size;
//          domFile.write(reinterpret_cast<char*>(&v), sizeof(int));
//          domFile.write(itr->first.text, itr->first.size);
//      }
//      domFile.close();
    }

//Check whether there are buffers to be released
    for (int i = 0; i < blocks.size(); ++i) {
        char *block = blocks[i];
        if (block != NULL) {
            factory.release(block);
        }
    }
}


