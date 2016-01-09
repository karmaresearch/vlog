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

#ifndef FILEREADER_H_
#define FILEREADER_H_

#include <fstream>
#include <iostream>
#include <string>
#include <exception>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/chrono.hpp>

using namespace std;

class ParseException: public exception {
public:
	virtual const char* what() const throw () {
		return "Line is not parsed correctly";
	}
};

typedef struct FileInfo {
	long size;
	long start;
	bool splittable;
	string path;
} FileInfo;

class FileReader {
private:
	const bool compressed;
	ifstream rawFile;
	boost::iostreams::filtering_istream compressedFile;
	string currentLine;
	bool tripleValid;
	long end;
	long countBytes;

	const char *startS;
	int lengthS;
	const char* startP;
	int lengthP;
	const char *startO;
	int lengthO;

	ParseException ex;
	bool parseLine(const char *input, const int sizeInput);

	void checkRange(const char *pointer, const char* start, const char *end);

public:
	FileReader(FileInfo file);

	bool parseTriple();

	bool isTripleValid();

	const char *getCurrentS(int &length);

	const char *getCurrentP(int &length);

	const char *getCurrentO(int &length);
};

#endif /* FILEREADER_H_ */
