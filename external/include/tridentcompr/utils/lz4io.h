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

#ifndef LZ4IO_H_
#define LZ4IO_H_

#include "utils.h"
#include "../main/consts.h"
#include <lz4.h>
#include <boost/log/trivial.hpp>
#include <iostream>
#include <fstream>
#include <string>
#include <cstring>

#define SIZE_SEG 64*1024
#define SIZE_COMPRESSED_SEG 70*1024

using namespace std;

class LZ4Writer {
private:
	string path;
	std::ofstream os;
	char compressedBuffer[SIZE_COMPRESSED_SEG];
	char uncompressedBuffer[SIZE_SEG];
	int uncompressedBufferLen;

	void compressAndWriteBuffer();
public:

	LZ4Writer(string file) :
			os(file) {
		this->path = file;
		if (!os.good()) {
			BOOST_LOG_TRIVIAL(error)<< "Failed to open the file " << file;
		}

		uncompressedBufferLen = 0;
		memset(compressedBuffer, 0, sizeof(char) * SIZE_COMPRESSED_SEG);
		strcpy(compressedBuffer, "LZOBLOCK");
	}

	void writeByte(char i);

	void writeLong(long n);

	void writeShort(short n);

	void writeVLong(long n);

//	void writeString(const char *el);

	void writeRawArray(const char *bytes, int length);

	void writeString(const char *rawStr, int length);

	~LZ4Writer();
};

class LZ4Reader {

private:
	char supportBuffer[MAX_TERM_SIZE + 2];
	char compressedBuffer[SIZE_COMPRESSED_SEG];
	char uncompressedBuffer[SIZE_SEG];
	int uncompressedBufferLen;
	int currentOffset;

	std::ifstream is;
	string file;

	int uncompressBuffer() {
		//The total header size is 21 bytes
		char header[21];
		is.read(header, 21);
		if (is.eof()) {
			currentOffset = 0;
			return 0;
		}

		if (!is.good() || is.gcount() != 21) {
			BOOST_LOG_TRIVIAL(error)<< "Problems reading from the file. Only " << is.gcount() << " out of 21 were being read" << endl;
		}

		int token = header[8] & 0xFF;
		int compressionMethod = token & 0xF0;

		//First 8 bytes is a fixed string (LZ4Block). Then there is one token byte.
		int compressedLen = Utils::decode_intLE(header, 9);
		int uncompressedLen = Utils::decode_intLE(header, 13);
		switch (compressionMethod) {
		case 16:
			is.read(uncompressedBuffer, uncompressedLen);
			if (!is.good()) {
				BOOST_LOG_TRIVIAL(error)<< "Problems reading from the file. Only " << is.gcount() << " out of " << uncompressedLen << " were being read" << endl;
			}
			break;
			case 32:
			is.read(compressedBuffer, compressedLen);
			if (!is.good()) {
				BOOST_LOG_TRIVIAL(error)<< "Problems reading from the file. Only " << is.gcount() << " out of " << compressedLen << " were being read" << endl;
			}

			if (!LZ4_decompress_fast(compressedBuffer, uncompressedBuffer,
							uncompressedLen)) {
				BOOST_LOG_TRIVIAL(error)<< "Error in the decompression.";
			}
			break;
			default:
			BOOST_LOG_TRIVIAL(error) << "Unrecognized block format. This should not happen. File " << file << " is broken.";
			exit(1);
		}
		currentOffset = 0;
		return uncompressedLen;
	}

public:
	LZ4Reader(string file) :
			is(file) {

		if (!is.good()) {
			BOOST_LOG_TRIVIAL(error)<< "Failed to open the file " << file;
		}

		uncompressedBufferLen = 0;
		currentOffset = 0;
		this->file = file;
	}

	bool isEof() {
		if (currentOffset == uncompressedBufferLen) {
			if (is.good()) {
				uncompressedBufferLen = uncompressBuffer();
				return uncompressedBufferLen == 0;
			} else {
				return true;
			}
		}
		return false;
	}

	long parseLong();

	long parseVLong();

	int parseInt();

	char parseByte();

	const char *parseString(int &size) {
		size = parseVLong();

		if (currentOffset + size <= uncompressedBufferLen) {
			memcpy(supportBuffer, uncompressedBuffer + currentOffset, size);
			currentOffset += size;
		} else {
			int remSize = uncompressedBufferLen - currentOffset;
			memcpy(supportBuffer, uncompressedBuffer + currentOffset, remSize);
			currentOffset += remSize;
			isEof(); //Load the next buffer
			memcpy(supportBuffer + remSize, uncompressedBuffer, size - remSize);
			currentOffset += size - remSize;
		}
		supportBuffer[size] = '\0';

		return supportBuffer;
//
//
//		for (size = 0; !isEof() && uncompressedBuffer[currentOffset] != '\0';
//				currentOffset++) {
//			supportBuffer[size++] = uncompressedBuffer[currentOffset];
//		}
//		supportBuffer[size] = '\0';
//		currentOffset++;
//		return supportBuffer;
	}

	~LZ4Reader() {
		is.close();
	}
};

#endif /* LZ4IO_H_ */
