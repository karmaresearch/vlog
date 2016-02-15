#ifndef _CLUSTERTABLE_INSERTER_H
#define _CLUSTERTABLE_INSERTER_H

#include <trident/binarytables/fileindex.h>
#include <trident/binarytables/binarytableinserter.h>
#include <trident/kb/consts.h>

class ClusterTableInserter : public BinaryTableInserter {
private:
    //Common vars
    long previousFirstTerm, previousSecondTerm;
    short baseSecondTermFile;
    int baseSecondTermPos;
    short fileLastFirstTerm;
    int posLastFirstTerm;

    FileIndex *secondTermIndex;
    bool removeSecondTermIndex;

    long lastSecondTerm;
    long nElementsForIndexing;

    //Additional vars used to write the second term
    int bytesUsed;
    bool smallGroupMode;
    int nElementsGroup;

    int diffMode1;
    int compr1;
    int compr2;

    int getRelativeSecondTermPos();
    int getRelativeSecondTermPos(short file, int absPos);
    void writeNElementsAt(char b, short file, int pos);
    void insertSecondTerm(bool last);
    void updateSecondTermIndex(long lastTermWritten, int bytesTaken,
                               short currentFile, int currentPos);
    long calculateSecondTermToWrite(long term);
    int writeSecondTerm(long termToWrite);
    void updateFirstTermIndex(const long t1);
    void writeFirstTerm(long termToWrite);
    long calculateFirstTermToWrite(long termToWrite);

public:
    int getType() {
        return CLUSTER_ITR;
    }

    void startAppend();

    void append(long t1, long t2);

    void stopAppend();

    void mode_difference(int modeValue1);

    void mode_compression(int compr1, int compr2);
};

#endif
