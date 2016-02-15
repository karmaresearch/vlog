#include <trident/files/comprfiledescriptor.h>
#include <trident/files/filemanager.h>
#include <trident/kb/consts.h>

#include <tridentcompr/utils/utils.h>

#include <boost/chrono.hpp>
#include <boost/filesystem.hpp>
#include <boost/log/trivial.hpp>

#include <lz4.h>
#include <lz4hc.h>
#include <fstream>
#include <iostream>

using namespace std;
namespace timens = boost::chrono;
namespace fs = boost::filesystem;

ComprFileDescriptor::ComprFileDescriptor(bool readOnly, int id,
        std::string file, int maxSize, MemoryManager<FileSegment> *tracker,
        ComprFileDescriptor **parentArray, Stats * const stats) :
    file(file), id(id), readOnly(readOnly), stats(stats) {
    this->tracker = tracker;
    lastAccessedSegment = NO_BLOCK_SESSION;

    string comprFile = file + string(".compr");
    if (fs::exists(comprFile)) {
        ifstream is(comprFile);
        is.seekg(0, std::ios_base::end);
        std::size_t size = is.tellg();
        is.seekg(0, std::ios_base::beg);
        char raw_input[size];
        is.read(raw_input, size);
        int offset = 0;
        uncompressedSize = Utils::decode_int(raw_input, offset);
        offset += 4;
        sizeMappings = uncompressedSize / BLOCK_SIZE;
        if (uncompressedSize % BLOCK_SIZE > 0)
            sizeMappings++;

        for (int i = 0; i < sizeMappings; ++i) {
            mappings[i] = Utils::decode_int(raw_input, offset);
            offset += 4;
            uncompressedBuffers[i] = NULL;
        }
        is.close();
    } else {
        sizeMappings = 0;
        uncompressedSize = 0;
        uncompressedBuffers[0] = NULL;
    }

    //Load the compressed file
    if (readOnly) {
        if (uncompressedSize > 0) {
            readOnlyInput.open(file);
            //BOOST_LOG_TRIVIAL(debug) << "Readonly open file " << file;
        } else {
            BOOST_LOG_TRIVIAL(error) << "File empty!" << endl;
            throw 10;
        }
        rawSize = -1;
        rawBuffer = new char[BLOCK_SIZE * 2];
    } else {
        rawSize = max(uncompressedSize, maxSize);
        rawBuffer = new char[rawSize];

        //Read the entire file if it exists
        int p = 0;
        char compressedBlock[BLOCK_SIZE + (int) (BLOCK_SIZE * 0.1)];
        ifstream readFile(file);
        int bytesReadSoFar = 0;
        for (int block = 0; block < sizeMappings; ++block) {
            //Read one block
            int compressedBlockSize = mappings[block] - bytesReadSoFar;
            if (COMPRESSION_ENABLED) {
                readFile.read(compressedBlock, compressedBlockSize);
                LZ4_decompress_safe(compressedBlock, rawBuffer + p,
                                    compressedBlockSize, BLOCK_SIZE);
            } else {
                readFile.read(rawBuffer + p, compressedBlockSize);
            }
            stats->incrNReadIndexBlocks();
            stats->addNReadIndexBytes(BLOCK_SIZE);
            p += BLOCK_SIZE;
            bytesReadSoFar = mappings[block];
        }
        readFile.close();
    }
}

void ComprFileDescriptor::uncompressBlock(const int block) {
    int startCompr = block > 0 ? mappings[block - 1] : 0;
    FileSegment *uncompressedBuffer = NULL;
    uncompressedBuffer = new FileSegment();

    readOnlyInput.seekg(startCompr);
    //BOOST_LOG_TRIVIAL(debug) << "Readonly seek to pos " << startCompr << " file " << file;
    if (readOnlyInput.fail() || readOnlyInput.eof()) {
        BOOST_LOG_TRIVIAL(error) << "Problems with the seek at pos " << startCompr << " file " << file;
        throw 10;
    }

    if (COMPRESSION_ENABLED) {
        readOnlyInput.read(rawBuffer, mappings[block] - startCompr);
        //BOOST_LOG_TRIVIAL(debug) << "Readonly read " << (mappings[block] - startCompr) << " from " << file;
        LZ4_decompress_safe(rawBuffer, uncompressedBuffer->block,
                            mappings[block] - startCompr, BLOCK_SIZE);
    } else {
        readOnlyInput.read(uncompressedBuffer->block, mappings[block] - startCompr);
        //BOOST_LOG_TRIVIAL(debug) << "Readonly read " << (mappings[block] - startCompr) << " from " << file;
    }
    stats->addNReadIndexBytes(mappings[block] - startCompr);
    stats->incrNReadIndexBlocks();

    uncompressedBuffers[block] = uncompressedBuffer;
    int memoryId = tracker->add(BLOCK_SIZE, uncompressedBuffer, block,
                                uncompressedBuffers);
    uncompressedBuffer->memId = memoryId;
}

char* ComprFileDescriptor::getBuffer(int pos, int *length, int &memoryBlock,
                                     const int sessionID) {
    char *ret = getBuffer(pos, length, sessionID);
    memoryBlock = lastAccessedSegment;
    return ret;
}

char* ComprFileDescriptor::getBuffer(int pos, int *length,
                                     const int sessionID) {
    if (!readOnly) {
        if (pos + *length >= uncompressedSize) {
            *length = uncompressedSize - pos;
        }
        return rawBuffer + pos;
    }

    // 1- First determine the block that contain the pos
    int startBlock = pos / BLOCK_SIZE;
    int offsetBlock = pos % BLOCK_SIZE;

    // 2- If the block was not opened then I must decompress it
    if (uncompressedBuffers[startBlock] == NULL && startBlock < sizeMappings) {
        uncompressBlock(startBlock);
    }

    // 3- Check the length of the buffer
    int len = BLOCK_SIZE - offsetBlock;
    char *b = uncompressedBuffers[startBlock]->block + offsetBlock;
    lastAccessedSegment = uncompressedBuffers[startBlock]->memId;
    if (pos + len > uncompressedSize) {
        len = uncompressedSize - pos;
    } else if (len < BLOCK_MIN_SIZE) {
        //3a - Check if the next buffer is available
        if (uncompressedBuffers[startBlock + 1] == NULL) {
            uncompressBlock(startBlock + 1);
        }
        //3b - Copy the buffer
        if (specialTmpBuffers[sessionID] == NULL) {
            specialTmpBuffers[sessionID] = std::move(std::unique_ptr<char[]>(new char[BLOCK_MIN_SIZE]));
        }
        memcpy(specialTmpBuffers[sessionID].get(), b, len);
        memcpy(specialTmpBuffers[sessionID].get() + len, uncompressedBuffers[startBlock + 1],
               BLOCK_MIN_SIZE - len);

        len = BLOCK_MIN_SIZE;
        b = specialTmpBuffers[sessionID].get();

        lastAccessedSegment = NO_BLOCK_SESSION;
    }
    *length = len;
    return b;
}

/*bool ComprFileDescriptor::isUsed() {
    for (int i = 0; i < sizeMappings; ++i) {
        if (uncompressedBuffers[i] != NULL) {
            if (tracker->isUsed(uncompressedBuffers[i]->memId))
                return true;
        }
    }
    return false;
}*/

ComprFileDescriptor::~ComprFileDescriptor() {
    if (!readOnly) {
        writeFile();
    } else {
        //Check if there are segments to remove
        for (int i = 0; i < sizeMappings; ++i) {
            if (uncompressedBuffers[i] != NULL) {
                tracker->removeBlock(uncompressedBuffers[i]->memId);
            }
        }

        readOnlyInput.close();
        //BOOST_LOG_TRIVIAL(debug) << "Readonly close " << file;
    }
    delete[] rawBuffer;
}

void ComprFileDescriptor::writeFile() {
    //Open two files: one with the mappings while the others with the data
    ofstream mappingsFile(file + string(".compr"), std::ofstream::out);
    ofstream dataFile(file, std::ofstream::out);

//  //Recompress everything
    int maxSizeSupportBuffer = LZ4_compressBound(BLOCK_SIZE);
    char supportBuffer[maxSizeSupportBuffer];
    int compressedBytes = 0;
    int sizeCompressedFile = 0;

    Utils::encode_int(supportBuffer, 0, uncompressedSize);
    mappingsFile.write(supportBuffer, 4);

    while (compressedBytes < uncompressedSize) {
        int sizeBlock;
        if (compressedBytes + BLOCK_SIZE < uncompressedSize) {
            sizeBlock = BLOCK_SIZE;
        } else {
            sizeBlock = uncompressedSize - compressedBytes;
        }


        int comprSize = 0;
        if (COMPRESSION_ENABLED) {
            comprSize = LZ4_compressHC(rawBuffer + compressedBytes, supportBuffer,
                                       sizeBlock);
            dataFile.write(supportBuffer, comprSize);
        } else {
            comprSize = sizeBlock;
            dataFile.write(rawBuffer + compressedBytes, comprSize);
        }

        compressedBytes += sizeBlock;
        sizeCompressedFile += comprSize;
        Utils::encode_int(supportBuffer, 0, sizeCompressedFile);
        mappingsFile.write(supportBuffer, 4);
    }
    mappingsFile.close();
    dataFile.close();
}
