#ifndef LOADER_H_
#define LOADER_H_

#include <trident/kb/kb.h>
#include <trident/tree/coordinates.h>
#include <trident/kb/inserter.h>

#include <tridentcompr/sorting/sorter.h>
#include <tridentcompr/utils/lz4io.h>
#include <tridentcompr/utils/triple.h>

#include <string>
#include <vector>

using namespace std;


typedef class PairLong {
public:
    long n1;
    long n2;

    void readFrom(LZ4Reader *reader) {
        n1 = reader->parseLong();
        n2 = reader->parseLong();
    }

    void writeTo(LZ4Writer *writer) {
        writer->writeLong(n1);
        writer->writeLong(n2);
    }

    static bool less(const PairLong &p1, const PairLong &p2) {
        return p1.n1 < p2.n1;
    }

    bool greater(const PairLong &p) const {
        return !less(*this, p);
    }
} PairLong;

typedef struct TreeEl {
    nTerm key;
    long nElements;
    int pos;
    short file;
    char strat;
} TreeEl;

class TreeWriter: public TreeInserter {
private:
    ofstream fos;
    char supportBuffer[23];
public:
    TreeWriter(string path) {
        fos.open(path);
    }

    void addEntry(nTerm key, long nElements, short file, int pos,
                  char strategy) {
        Utils::encode_long(supportBuffer, 0, key);
        Utils::encode_long(supportBuffer, 8, nElements);
        Utils::encode_short(supportBuffer, 16, file);
        Utils::encode_int(supportBuffer, 18, pos);
        supportBuffer[22] = strategy;
        fos.write(supportBuffer, 23);
    }

    void finish() {
        fos.close();
    }

    ~TreeWriter() {
    }
    ;
};

class CoordinatesMerger {
private:
    ifstream spo, ops, pos;
    ifstream sop, osp, pso;
    char supportBuffer[23];

    const int ncoordinates;

    TreeEl elspo, elops, elpos, elsop, elosp, elpso;
    bool spoFinished, opsFinished, posFinished,
         sopFinished, ospFinished, psoFinished;
    TermCoordinates value;

    bool getFirst(TreeEl *el, ifstream *buffer);

public:
    CoordinatesMerger(string *coordinates, int ncoordinates) :
        ncoordinates(ncoordinates) {

        //Open the three files
        spo.open(coordinates[0]);
        spoFinished = !getFirst(&elspo, &spo);

        if (ncoordinates > 1) {
            ops.open(coordinates[1]);
            opsFinished = !getFirst(&elops, &ops);
            pos.open(coordinates[2]);
            posFinished = !getFirst(&elpos, &pos);
        }

        if (ncoordinates == 4) {
            pso.open(coordinates[3]);
            getFirst(&elpso, &pso);
        } else if (ncoordinates == 6) {
            sop.open(coordinates[3]);
            osp.open(coordinates[4]);
            pso.open(coordinates[5]);
            sopFinished = !getFirst(&elsop, &sop);
            ospFinished = !getFirst(&elosp, &osp);
            psoFinished = !getFirst(&elpso, &pso);
        }
    }

    TermCoordinates *get(nTerm &key);

    ~CoordinatesMerger() {
        spo.close();

        if (ncoordinates > 1) {
            ops.close();
            pos.close();
        }

        if (ncoordinates == 4) {
            pso.close();
        } else if (ncoordinates == 6) {
            sop.close();
            osp.close();
            pso.close();
        }
    }
};


class BufferCoordinates {
    nTerm keys[10000];
    TermCoordinates values[10000];
    int size;
    int pos;
public:
    BufferCoordinates() {
        size = pos = 0;
    }

    void add(nTerm key, TermCoordinates *value) {
        keys[size] = key;
        values[size++].copyFrom(value);
    }

    TermCoordinates *getNext(nTerm &key) {
        key = keys[pos];
        return values + pos++;
    }

    bool isFull() {
        return size == 10000;
    }

    bool isEmpty() {
        return pos == size;
    }

    void clear() {
        pos = size = 0;
    }
};

class SimpleTripleWriter;
class Loader {
private:
    //Used during the writing of the tree
    int buffersReady;
    bool isFinished;
    boost::condition_variable cond;
    boost::mutex mut;
    bool printStats;

    BufferCoordinates buffer1;
    BufferCoordinates buffer2;
    BufferCoordinates *bufferToFill;
    BufferCoordinates *bufferToReturn;

    void sortAndInsert(int permutation, int nindices, string inputDir,
                       string *POSoutputDir, TreeWriter *treeWriter, Inserter *ins,
                       const bool aggregated,
                       const bool canSkipTables) {
        sortAndInsert(permutation, nindices, false, inputDir, POSoutputDir,
                      treeWriter, ins, aggregated, canSkipTables,
                      false, NULL, 0.0);
    }

    void sortAndInsert(int permutation, int nindices, bool inputSorted, string inputDir,
                       string *POSoutputDir, TreeWriter *treeWriter, Inserter *ins,
                       const bool aggregated,
                       const bool canSkipTables,
                       const bool storeRaw,
                       SimpleTripleWriter *sampleWriter,
                       double sampleRate);

    void mergeTermCoordinates(string *coordinates, int ncoordinates);

    BufferCoordinates *getBunchTermCoordinates();

    void releaseBunchTermCoordinates(BufferCoordinates *cord);

    void processTermCoordinates(Inserter *ins);

    void insertDictionary(const int part, DictMgmt *dict, string dictFileInput,
                          bool insertDictionary, bool insertInverseDictionary,
                          bool sortNumberCoordinates, nTerm *maxValueCounter);

    void addSchemaTerms(const int dictPartitions, nTerm highestNumber, DictMgmt *dict);

    void loadKB(KB &kb, string kbDir, bool storeDicts, int dictionaries, string dictMethod,
                int nindices, bool aggrIndices, const bool canSkipTables,
                bool sample, double sampleRate,
                bool storePlainList, string *fileNameDictionaries, int nperms,
                int signaturePerms, string *permDirs, long totalCount);

    void createPermutations(string inputDir, int nperms, int signaturePerms, string *outputPermFiles);
public:

    Loader() {
        bufferToFill = bufferToReturn = &buffer1;
        buffersReady = 0;
        isFinished = false;
        printStats = true;
    }

    void load(bool onlyCompress, string triplesInputDir, string kbDir,
              string dictMethod, int sampleMethod, int sampleArg,
              int parallelThreads, int maxReadingThreads, int dictionaries,
              int nindices, bool aggrIndices, const bool canSkipTables,
              bool enableFixedStrat, int fixedStrat,
              bool storePlainList, bool sample, double sampleRate,
              int thresholdSkipTable);
};

#endif /* LOADER_H_ */
