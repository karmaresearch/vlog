#ifndef KBBC_H_
#define KBBC_H_

#include <trident/kb/consts.h>
#include <trident/utils/propertymap.h>

#include <string>

using namespace std;

typedef enum {
    NINDICES, //Number of indices to use
    AGGRINDICES, //Use aggregated indices or not
    INCOMPLINDICES, //The additional indices (PSO,SOP,OSP) are constructed only if necessary
    DICTPARTITIONS, //Partitions for the dictionary
    DICTHASH, //Whether the hash of the terms should be used to compress the dict. entries

    USEFIXEDSTRAT, //Whether we should always use the same strategy
    FIXEDSTRAT, //If the previous is true, this sets the strategy

    THRESHOLD_SKIP_TABLE, //Define the threshold when a table can be skipped

    TREE_MAXELEMENTSNODE, //Max elements inside a node
    TREE_MAXSIZECACHETREE, //Max size of the cache in bytes
    TREE_MAXNODESINCACHE, //Max number of nodes to keep alive. Then, the elements are being deleted
    TREE_MAXPREALLLEAVESCACHE, //Number of leaves that should be preallocated at startup
    TREE_MAXLEAVESCACHE, //Size internal factory to get new leaves
    TREE_NODEMINBYTES, //Min number of bytes used to store the nodes
    TREE_MAXFILESIZE, //Max size in bytes of the files that store the tree
    TREE_MAXNFILES, //Max number of files opened

    //Used by the factory of arrays of internal lines
    //Used by the factory of internal lines
    TREE_MAXPREALLINTERNALLINES,
    TREE_MAXINTERNALLINES,
    TREE_FACTORYSIZE,
    TREE_ALLOCATEDELEMENTS,

    TREE_NODE_KEYS_FACTORY_SIZE, //Size factory of arrays of longs to be used in the nodes
    TREE_NODE_KEYS_PREALL_FACTORY_SIZE, //Same as before, only the preallocated size

//The following parameters are equalivant to the previous but apply to the dictionary tree
    DICT_MAXELEMENTSNODE,
    DICT_MAXSIZECACHETREE,
    DICT_MAXNODESINCACHE,
    DICT_MAXPREALLLEAVESCACHE,
    DICT_MAXLEAVESCACHE,
    DICT_NODEMINBYTES,
    DICT_MAXFILESIZE,
    DICT_MAXNFILES,
    DICT_NODE_KEYS_FACTORY_SIZE,
    DICT_NODE_KEYS_PREALL_FACTORY_SIZE,

//And these are about the inverse dictionary
    INVDICT_MAXELEMENTSNODE,
    INVDICT_MAXSIZECACHETREE,
    INVDICT_MAXNODESINCACHE,
    INVDICT_MAXPREALLLEAVESCACHE,
    INVDICT_MAXLEAVESCACHE,
    INVDICT_NODEMINBYTES,
    INVDICT_MAXFILESIZE,
    INVDICT_MAXNFILES,
    INVDICT_NODE_KEYS_FACTORY_SIZE,
    INVDICT_NODE_KEYS_PREALL_FACTORY_SIZE,

//Parameters for one storage partition
    STORAGE_CACHE_SIZE,
    STORAGE_MAX_FILE_SIZE,
    STORAGE_MAX_N_FILES,

//Parameters about the string buffer
    SB_COMPRESSDOMAINS,
    SB_PREALLBUFFERS,
    SB_CACHESIZE

} KBParam;

class KBConfig {

private:
    PropertyMap internalMap;

public:
    KBConfig();

    void setParam(KBParam key, string value);

    string getParam(KBParam key);

    void setParamInt(KBParam key, int value);

    int getParamInt(KBParam key);

    void setParamLong(KBParam key, long value);

    long getParamLong(KBParam key);

    void setParamBool(KBParam key, bool value);

    bool getParamBool(KBParam key);

    ~KBConfig() {
    }
};

#endif
