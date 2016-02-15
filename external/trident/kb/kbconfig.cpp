#include <trident/kb/kbconfig.h>
#include <trident/utils/propertymap.h>

#include <map>

KBConfig::KBConfig() {
    //All default values
    internalMap.setInt(DICTPARTITIONS, 4);
    internalMap.setBool(DICTHASH, false);

    internalMap.setInt(NINDICES, 6);
    internalMap.setBool(AGGRINDICES, true);
    internalMap.setBool(INCOMPLINDICES, false);

    internalMap.setBool(USEFIXEDSTRAT, false);
    internalMap.setInt(FIXEDSTRAT, 0);
    internalMap.setInt(THRESHOLD_SKIP_TABLE, 10);

    //Parameters about the main tree
    internalMap.setInt(TREE_MAXELEMENTSNODE, 2048);
    internalMap.setLong(TREE_MAXSIZECACHETREE, 10000000000);
    internalMap.setInt(TREE_MAXNODESINCACHE, 100000);
    internalMap.setInt(TREE_MAXPREALLLEAVESCACHE, 100000);
    internalMap.setInt(TREE_MAXLEAVESCACHE, 100);
    internalMap.setInt(TREE_NODEMINBYTES, 1000);
    internalMap.setInt(TREE_MAXFILESIZE, 64 * 1024 * 1024);
    internalMap.setInt(TREE_MAXNFILES, MAX_N_FILES);

    //Leaves in the tree
    internalMap.setInt(TREE_MAXPREALLINTERNALLINES, 10000000);
    internalMap.setInt(TREE_MAXINTERNALLINES, 100000);
    internalMap.setInt(TREE_FACTORYSIZE, 100);
    internalMap.setInt(TREE_ALLOCATEDELEMENTS, 100000);

    //Nodes in the tree
    internalMap.setInt(TREE_NODE_KEYS_FACTORY_SIZE, 10);
    internalMap.setInt(TREE_NODE_KEYS_PREALL_FACTORY_SIZE, 10000);

    //Dictionary
    internalMap.setInt(DICT_MAXELEMENTSNODE, 2048);
    internalMap.setLong(DICT_MAXSIZECACHETREE, 2000000000);
    internalMap.setInt(DICT_MAXNODESINCACHE, 1000);
    internalMap.setInt(DICT_MAXPREALLLEAVESCACHE, 1000);
    internalMap.setInt(DICT_MAXLEAVESCACHE, 1000);
    internalMap.setInt(DICT_NODEMINBYTES, 2048 * 10);
    internalMap.setInt(DICT_MAXFILESIZE, 64 * 1024 * 1024);
    internalMap.setInt(DICT_MAXNFILES, MAX_N_FILES);
    internalMap.setInt(DICT_NODE_KEYS_FACTORY_SIZE, 1000);
    internalMap.setInt(DICT_NODE_KEYS_PREALL_FACTORY_SIZE, 1000);

    //Inverse dictionary
    internalMap.setInt(INVDICT_MAXELEMENTSNODE, 2048);
    internalMap.setLong(INVDICT_MAXSIZECACHETREE, 10000000000);
    internalMap.setInt(INVDICT_MAXNODESINCACHE, 10000);
    internalMap.setInt(INVDICT_MAXPREALLLEAVESCACHE, 10000);
    internalMap.setInt(INVDICT_MAXLEAVESCACHE, 100);
    internalMap.setInt(INVDICT_NODEMINBYTES, 2048 / 2 * 7);
    internalMap.setInt(INVDICT_MAXFILESIZE, 64 * 1024 * 1024);
    internalMap.setInt(INVDICT_MAXNFILES, MAX_N_FILES);
    internalMap.setInt(INVDICT_NODE_KEYS_FACTORY_SIZE, 100);
    internalMap.setInt(INVDICT_NODE_KEYS_PREALL_FACTORY_SIZE, 100);
    //The minimum size is the half of max elements (2048) times 7 bytes.
    //(2 is the term size while 5 is the position to retrieve the textual term)

    //Storage
    internalMap.setLong(STORAGE_CACHE_SIZE, 5000000000);
    internalMap.setInt(STORAGE_MAX_FILE_SIZE, 256 * 1024 * 1024);
    internalMap.setInt(STORAGE_MAX_N_FILES, MAX_N_FILES);

    //String buffer
    internalMap.setBool(SB_COMPRESSDOMAINS, false);
    internalMap.setInt(SB_PREALLBUFFERS, 1000);
    internalMap.setLong(SB_CACHESIZE, 128 * 1024 * 1024); //128MB
}

void KBConfig::setParam(KBParam key, string value) {
    internalMap.set(key, value);
}

string KBConfig::getParam(KBParam key) {
    return internalMap.get(key);
}

void KBConfig::setParamInt(KBParam key, int value) {
    internalMap.setInt(key, value);
}

int KBConfig::getParamInt(KBParam key) {
    return internalMap.getInt(key);
}

void KBConfig::setParamLong(KBParam key, long value) {
    internalMap.setLong(key, value);
}

long KBConfig::getParamLong(KBParam key) {
    return internalMap.getLong(key);
}

void KBConfig::setParamBool(KBParam key, bool value) {
    internalMap.setBool(key, value);
}

bool KBConfig::getParamBool(KBParam key) {
    return internalMap.getBool(key);
}

