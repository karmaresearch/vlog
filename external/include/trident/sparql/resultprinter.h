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

#ifndef _OUTPUTQUERY_H
#define _OUTPUTQUERY_H

#include <trident/kb/consts.h>

class OutputBuffer {

    long buffer1[OUTPUT_BUFFER_SIZE];
    long buffer2[OUTPUT_BUFFER_SIZE];

    long *currentBuffer;
    bool firstBufferActive;
    int currentSize;

public:
    OutputBuffer() {
        currentBuffer = buffer1;
        firstBufferActive = true;
        currentSize = 0;
    }

    void addTerm(long t) {
        currentBuffer[currentSize++] = t;
    }

    long *getBuffer() {
        long *o = currentBuffer;
        if (firstBufferActive) {
            currentBuffer = buffer2;
        } else {
            currentBuffer = buffer1;
        }
        firstBufferActive = !firstBufferActive;
        currentSize = 0;
        return o;
    }
};

class ResultPrinter {
private:

    void printHeader();

    void printNumericRow(long *row, int size);

    void printFooter();

public:


};

#endif
