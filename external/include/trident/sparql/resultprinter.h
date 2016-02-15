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
