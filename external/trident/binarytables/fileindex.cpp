#include <trident/binarytables/fileindex.h>

#include <tridentcompr/utils/utils.h>

#include <algorithm>

FileIndex::FileIndex() {
    lengthArrays = 0;
    lengthAdditionalArrays = 0;

    keys = NULL;
    positions = NULL;
    files = NULL;
    size = 0;

    additionalKeys = NULL;
    additionalIndices = NULL;
    additionalSize = 0;
}

void FileIndex::unserialize(char* buffer, int *offset) {
    size = Utils::decode_vint2(buffer, offset);
    long *keys = NULL;
    int *positions = NULL;
    short *files = NULL;

    if (size > 0) {
        keys = new long[size];
        positions = new int[size];
        files = new short[size];
    }
    long previousKey = 0;
    int previousPos = 0;

    int oldFile = -1;
    for (int i = 0; i < size; ++i) {
        long tmp_k = Utils::decode_vlong(buffer, offset);
        int tmp_p = Utils::decode_vint2(buffer, offset);

        files[i] = Utils::decode_short((const char*)buffer, *offset);
        if (files[i] != oldFile) {
            keys[i] = tmp_k;
            positions[i] = tmp_p;
        } else {
            keys[i] = tmp_k + previousKey;
            positions[i] = tmp_p + previousPos;
        }

        oldFile = files[i];
        previousPos = positions[i];
        previousKey = keys[i];

        (*offset) += 2;
    }

    if (buffer[(*offset)++] == 1) {
        // Read the additional indices
        additionalSize = Utils::decode_vint2(buffer, offset);

        long *additionalKeys = new long[additionalSize];
        FileIndex **additionalIndices = new FileIndex*[additionalSize];
        long diffKey = 0;
        for (int i = 0; i < additionalSize; ++i) {
            additionalKeys[i] = Utils::decode_vlong2(buffer, offset) + diffKey;
            diffKey = additionalKeys[i];
            additionalIndices[i] = new FileIndex();
            additionalIndices[i]->unserialize(buffer, offset);
        }
        this->additionalKeys = additionalKeys;
        this->additionalIndices = additionalIndices;
    }
    this->keys = keys;
    this->positions = positions;
    this->files = files;
}

short FileIndex::file(int idx) {
    return files[idx];
}

int FileIndex::pos(int idx) {
    return positions[idx];
}

long FileIndex::key(int idx) {
    return keys[idx];
}

int FileIndex::idx(const long key) {
    return idx(0, key);
}

int FileIndex::idx(const int start, const long key) {
    //Little optimization. Maybe key  is less than following index
    if (key <= keys[start]) {
        return start;
    } else if (start < size - 1 && key < keys[start + 1]) {
        return start + 1;
    }

    int low = start;
    int high = size - 1;
    while (low <= high) {
        int mid = (low + high) >> 1;
        long midVal = keys[mid];
        if (midVal < key)
            low = mid + 1;
        else if (midVal > key)
            high = mid - 1;
        else
            return mid; // key found
    }
    return low; // key not found.
}

int FileIndex::sizeIndex() {
    return size;
}

FileIndex *FileIndex::additional_idx(long key) {
    if (additionalIndices == NULL)
        return NULL;

    int idx = 0;
    int low = 0;
    int high = additionalSize - 1;
    while (low <= high) {
        idx = (low + high) >> 1;
        long midVal = additionalKeys[idx];
        if (midVal < key)
            low = idx + 1;
        else if (midVal > key)
            high = idx - 1;
        else
            return additionalIndices[idx];
    }
    return NULL;
}

bool FileIndex::isEmpty() {
    return size == 0 && additionalSize == 0;
}

char* FileIndex::serialize(char *buffer, int &offset, int &maxSize) {
    //First check if the size is large enough to contain a list of keys
    int requiredSize = 9 + size * 14 + additionalSize * 8;
    if (maxSize < requiredSize + offset) {
        //Increase the array
        maxSize = max((int) (maxSize * 2), requiredSize + offset);
        char *newbuffer = new char[maxSize];
        if (offset > 0)
            memcpy(newbuffer, buffer, offset);
        delete[] buffer;
        buffer = newbuffer;
    }

    offset = Utils::encode_vint2(buffer, offset, size);
//  offset += 4;
    long diffKey = 0;
    int diffValue = 0;
    int oldFile = -1;

    for (int i = 0; i < size; ++i) {
        long k;
        int v;
        if (oldFile == files[i]) {
            k = keys[i] - diffKey;
            v = positions[i] - diffValue;
        } else {
            k = keys[i];
            v = positions[i];
        }
        offset = Utils::encode_vlong(buffer, offset, k);
        offset = Utils::encode_vint2(buffer, offset, v);
        Utils::encode_short(buffer, offset, files[i]);
        offset += 2;

        oldFile = files[i];
        diffKey = keys[i];
        diffValue = positions[i];
    }

    // Store additional indices
    if (additionalSize > 0) {
        buffer[offset++] = 1;
        offset = Utils::encode_vint2(buffer, offset, additionalSize);
        diffKey = 0;
        for (int i = 0; i < additionalSize; ++i) {
            offset = Utils::encode_vlong2(buffer, offset,
                                          additionalKeys[i] - diffKey);
            buffer = additionalIndices[i]->serialize(buffer, offset, maxSize);
            diffKey = additionalKeys[i];
        }
    } else {
        buffer[offset++] = 0;
    }

    return buffer;
}

void FileIndex::add(long key, short file, int pos) {
    checkLengthArrays(size);
    keys[size] = key;
    files[size] = file;
    positions[size] = pos;
    size++;
}

void FileIndex::addAdditionalIndex(long key, FileIndex *idx) {
    checkLengthAdditionalArrays(additionalSize);
    additionalKeys[additionalSize] = key;
    additionalIndices[additionalSize++] = idx;
}

FileIndex::~FileIndex() {
    if (keys != NULL) {
        delete[] keys;
    }
    if (files != NULL) {
        delete[] files;
    }

    if (positions != NULL) {
        delete[] positions;
    }
    if (additionalSize > 0) {
        delete[] additionalKeys;
        for (int i = 0; i < additionalSize; ++i) {
            delete additionalIndices[i];
        }
        delete[] additionalIndices;
    }
}
