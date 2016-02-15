#ifndef KBB_H_
#define KBB_H_

#include <trident/kb/consts.h>
#include <trident/kb/statistics.h>
#include <trident/kb/dictmgmt.h>
#include <trident/kb/kbconfig.h>
#include <trident/kb/cacheidx.h>
#include <trident/utils/memorymgr.h>

#include <boost/thread.hpp>
#include <boost/filesystem.hpp>

#include <string>

class Leaf;
class Querier;
class Inserter;
class TableStorage;
class Root;
class StringBuffer;
struct FileSegment;
class FileDescriptor;

using namespace std;
namespace fs = boost::filesystem;

class KB {
private:
    const string path;

    const bool readOnly;

    Root *tree;

    Stats stats;
    Stats *statsStrings;

    StringBuffer **stringbuffers;
    //text->number
    Root **dictionaries;
    //number->text
    Root **invDictionaries;
    int dictPartitions;
    bool dictHash;

    long ntables[N_PARTITIONS];
    long nFirstTables[N_PARTITIONS];

    long totalNumberTriples;
    long totalNumberTerms;
    long nextID;

    int nindices;
    bool aggrIndices;
    bool incompleteIndices;

    bool useFixedStrategy;
    char storageFixedStrategy;

    size_t thresholdSkipTable;

    bool dictEnabled;

    double sampleRate;

    //boost::thread *dictLookupThread;
    DictMgmt *dictManager;

    TableStorage *files[N_PARTITIONS];
    MemoryManager<FileDescriptor> *bytesTracker[N_PARTITIONS];

    CacheIdx *pso;
    CacheIdx *osp;

    KB *sampleKB;

    void loadDict(int id, KBConfig *config);

public:
    KB(const char *path, bool readOnly, bool reasoning,
       bool dictEnabled, KBConfig &config);

    Querier *query();

    Inserter *insert();

    DictMgmt *getDictMgmt() {
        return dictManager;
    }

    void closeDict();

    void closeInvDict();

    void closeStringBuffers();

    Stats getStats();

    Stats *getStatsDict();

    string getDictPath(int i);

    string getPath() {
        return path;
    }

    int getNIndices() const {
        return nindices;
    }

    uint64_t getNTerms() const {
        return totalNumberTerms;
    }

    int getNDictionaries() {
        return dictPartitions;
    }

    int getNTablesPerPartition(int idx) {
        return ntables[idx];
    }

    double getSampleRate() {
        return sampleRate;
    }

    long getSize() {
        return totalNumberTriples;
    }

    long getNextID() {
        return nextID;
    }

    TreeItr *getItrTerms();

    ~KB();
};

#endif /* KBB_H_ */
