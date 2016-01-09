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

#include <tridentcompr/compression/filereader.h>
#include <tridentcompr/main/consts.h>

#include <fstream>
#include <iostream>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/log/trivial.hpp>
#include <climits>

namespace io = boost::iostreams;
namespace fs = boost::filesystem;
namespace timens = boost::chrono;

string GZIP_EXTENSION = string(".gz");

FileReader::FileReader(FileInfo sFile) :
		compressed(!sFile.splittable), rawFile(sFile.path,
				ios_base::in | ios_base::binary) {
	//First check the extension to identify what kind of file it is.
	fs::path p(sFile.path);
	if (p.has_extension() && p.extension() == GZIP_EXTENSION) {
		compressedFile.push(io::gzip_decompressor());
		compressedFile.push(rawFile);
	}

	if (sFile.splittable) {
		end = sFile.start + sFile.size;
	} else {
		end = LONG_MAX;
	}

	//If start != 0 then move to first '\n'
	if (sFile.start > 0) {
		rawFile.seekg(sFile.start);
//Seek to the first '\n'
		while (!rawFile.eof() && rawFile.get() != '\n') {
		};
	}
	countBytes = rawFile.tellg();
	tripleValid = false;

	startS = startP = startO = NULL;
	lengthS = lengthP = lengthO = 0;
}

bool FileReader::parseTriple() {
	bool ok = false;
	if (compressed) {
		ok = (bool) std::getline(compressedFile, currentLine);
	} else {
		ok = countBytes <= end && std::getline(rawFile, currentLine);
		if (ok) {
			countBytes = rawFile.tellg();
		}
	}

	if (ok) {
		if (currentLine.size() == 0 || currentLine.at(0) == '#') {
			return parseTriple();
		}
		tripleValid = parseLine(currentLine.c_str(), (int)currentLine.size());
		return true;
	}
	tripleValid = false;
	return false;
}

const char *FileReader::getCurrentS(int &length) {
	length = lengthS;
	return startS;
}

const char *FileReader::getCurrentP(int &length) {
	length = lengthP;
	return startP;
}

const char *FileReader::getCurrentO(int &length) {
	length = lengthO;
	return startO;
}

bool FileReader::isTripleValid() {
	return tripleValid;
}

void FileReader::checkRange(const char *pointer, const char* start,
		const char *end) {
	if (pointer == NULL || pointer <= (start + 1) || pointer > end) {
		throw ex;
	}
}

bool FileReader::parseLine(const char *line, const int sizeLine) {

	try {
		const char* endLine = line + sizeLine;

		// Parse subject
		const char *endS;
		startS = line;
		if (line[0] == '<') {
			endS = strchr(line, '>') + 1;
		} else { // Is a bnode
			endS = strchr(line, ' ');
		}
		checkRange(endS, startS, endLine);
		lengthS = (int)(endS - startS);

		//Parse predicate. Skip one space
		startP = line + lengthS + 1;
		const char *endP = strchr(startP, '>');
		checkRange(endP, startP, endLine);
		lengthP = (int)(endP + 1 - startP);

		// Parse object
		startO = startP + lengthP + 1;
		const char *endO = NULL;
		if (startO[0] == '<') { // URI
			endO = strchr(startO, '>') + 1;
		} else if (startO[0] == '"') { // Literal
			//Copy until the end of the string and remove character
			endO = strrchr(startO, '.')  - 1;
		} else { // Bnode
			endO = strchr(startO, ' ');
		}
		checkRange(endO, startO, endLine);
		lengthO = (int)(endO - startO);

		if (lengthS > 0 && lengthS < (MAX_TERM_SIZE - 1) && lengthP > 0
				&& lengthP < (MAX_TERM_SIZE - 1) && lengthO > 0
				&& lengthO < (MAX_TERM_SIZE - 1)) {
			return true;
		} else {
            BOOST_LOG_TRIVIAL(error) << "The triple was not parsed correctly: " << lengthS << " " << lengthP << " " << lengthO;
			return false;
		}

	} catch (std::exception &e) {
		BOOST_LOG_TRIVIAL(error)<< "Failed parsing line: " + string(line,sizeLine);
	}
	return false;
}
