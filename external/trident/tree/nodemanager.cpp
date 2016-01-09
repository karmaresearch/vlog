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

#include <trident/tree/nodemanager.h>

#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/filesystem.hpp>

#include <iostream>
#include <fstream>

namespace bip = boost::interprocess;
namespace fs = boost::filesystem;
char one[1] = { 1 };
char zero[1] = { 0 };

NodeManager::NodeManager(TreeContext *context, int nodeMinBytes,
                         int fileMaxSize, int maxNFiles, long cacheMaxSize, std::string path) :
    readOnly(context->isReadOnly()), path(path), nodeMinSize(nodeMinBytes) {
    lastNodeInserted = NULL;

    //Init filemanager
    //Calculate the highest file
    fs::path dir(path);
    fs::directory_iterator end_iter;
    lastCreatedFile = 0;
    if (fs::exists(dir) && fs::is_directory(dir)) {
        for (fs::directory_iterator dir_iter(dir); dir_iter != end_iter;
                ++dir_iter) {
            if (!dir_iter->path().has_extension()) {
                short idx = (short) atoi(dir_iter->path().filename().c_str());
                if (lastCreatedFile < idx)
                    lastCreatedFile = idx;
            }
        }
    }
    bytesTracker = new MemoryManager<FileDescriptor>(cacheMaxSize);
    this->manager = new FileManager<FileDescriptor, FileDescriptor>(path,
            context->isReadOnly(), fileMaxSize, maxNFiles, lastCreatedFile,
            bytesTracker, NULL);

    //Init storedNodes and firstElementPerFile
    string file = path + string("/idx");

    if (!readOnly) {
        readOnlyStoredNodes = NULL;
        nodesLoaded = NULL;
        mapping = NULL;
        mapped_rgn = NULL;
        rawInput = NULL;
    } else {
        //Load the nodes in the array
        if (fs::exists(file) && fs::file_size(file) > 0) {
            mapping = new bip::file_mapping(file.c_str(), bip::read_only);
            mapped_rgn = new bip::mapped_region(*mapping, bip::read_only);
            rawInput = static_cast<char*>(mapped_rgn->get_address());
            int nNodes = Utils::decode_int(rawInput, 0);
            nodesLoaded = new bool[nNodes];
            memset(nodesLoaded, 0, nNodes * sizeof(bool));
            readOnlyStoredNodes = new CachedNode[nNodes];
        }
    }
}

void NodeManager::unserializeNodeFrom(CachedNode *node, char *buffer, int pos) {
    long id = Utils::decode_long(buffer, pos);
    pos += 8;

    bool canHaveChildren = buffer[pos++] == 1 ? true : false;
    short fileIndex = (short) Utils::decode_int(buffer, pos);
    pos += 4;
    int fileOffset = Utils::decode_int(buffer, pos);
    pos += 4;
    int nodeSize = Utils::decode_int(buffer, pos);
    pos += 4;
    int availableSize = Utils::decode_int(buffer, pos);

    node->id = id;
    node->nodeSize = nodeSize;
    node->children = canHaveChildren;
    node->fileIndex = fileIndex;
    node->posIndex = fileOffset;
    node->availableSize = availableSize;
}

int NodeManager::serializeTo(CachedNode *node, char *buffer) {
    int pos = 0;
    Utils::encode_long(buffer, pos, node->id);
    pos += 8;
    buffer[pos++] = node->children ? 1 : 0;
    Utils::encode_int(buffer, pos, node->fileIndex);
    pos += 4;
    Utils::encode_int(buffer, pos, node->posIndex);
    pos += 4;
    Utils::encode_int(buffer, pos, node->nodeSize);
    pos += 4;
    Utils::encode_int(buffer, pos, node->availableSize);
    pos += 4;
    return pos;
}

char* NodeManager::get(CachedNode *node) {
    int len = node->nodeSize;
    return manager->getBuffer(node->fileIndex, node->posIndex, &len);
}

CachedNode *NodeManager::getCachedNode(long id) {
    if (readOnly) {
        if (!nodesLoaded[id]) {
            //Load the node from rawInput
            int pos = Utils::decode_int(rawInput, 4 + id * 4);
            unserializeNodeFrom(readOnlyStoredNodes + id, rawInput, pos);
            nodesLoaded[id] = true;
        }
        return readOnlyStoredNodes + id;
    } else {
        boost::unordered_map<long, CachedNode*>::iterator itr =
            storedNodes.find(id);
        if (itr != storedNodes.end())
            return itr->second;
        else
            return NULL;
    }
}

void NodeManager::put(Node *node, char *buffer, int sizeBuffer) {
    //First check if there is already a cachedNode existing
    CachedNode *cn = getCachedNode(node->getId());
    if (cn != NULL) {
        if (sizeBuffer > cn->availableSize) {
            BOOST_LOG_TRIVIAL(debug) << "Node " << cn->id << " is " << (sizeBuffer - cn->availableSize) << "  bytes larger. Must increase file " << cn->fileIndex;
            int diff = sizeBuffer - cn->availableSize;
            //Enlarge the file
            manager->shiftRemainingFile(cn->fileIndex, cn->posIndex, diff);
            // Update positions of all the nodes stored in the same file
            cn->availableSize += diff;
            CachedNode *next = cn->next;
            while (next != NULL) {
                next->posIndex += diff;
                next = cn->next;
            }
        }

        //Replace the content and update the nodeSize
        int len = sizeBuffer;
        char *b = manager->getBuffer(cn->fileIndex, cn->posIndex, &len);
        memcpy(b, buffer, sizeBuffer);
        cn->nodeSize = sizeBuffer;
    } else {
        CachedNode *c = new CachedNode;
        //Fill all the fields
        c->id = node->getId();
        c->children = node->canHaveChildren();
        c->nodeSize = sizeBuffer;
        c->availableSize = max(nodeMinSize, sizeBuffer);

        //Is the file too big?
        if (manager->sizeLastFile() >= manager->getFileMaxSize()) {
            lastCreatedFile = manager->createNewFile();
            lastNodeInserted = NULL;
        }

        // Update the metadata
        c->fileIndex = lastCreatedFile;
        c->posIndex = manager->sizeFile(lastCreatedFile);
        c->previous = NULL;
        c->next = NULL;
        if (lastNodeInserted == NULL) {
            lastNodeInserted = c;
            // this node is the first in the file
            firstElementsPerFile.push_back(c);
        } else {
            c->previous = lastNodeInserted;
            lastNodeInserted->next = c;
            lastNodeInserted = c;
        }
        //Write to disk
        manager->append(buffer, c->availableSize);

        //Put the cached node in the list
        storedNodes.insert(std::make_pair(c->id, c));
    }
}

void NodeManager::compressSpace(string path) {
    //1-- Load all the nodes and sort them by file
    vector<vector<CachedNode> > nodes;
    string sFileIdx = path + string("/idx");
    int totalNumberNodes = 0;
    if (fs::exists(path) && fs::file_size(sFileIdx) > 0) {
        bip::file_mapping *mapping = new bip::file_mapping(sFileIdx.c_str(),
                bip::read_only);
        bip::mapped_region *mapped_rgn = new bip::mapped_region(*mapping,
                bip::read_only);
        long size = mapped_rgn->get_size();
        char *raw_input = static_cast<char*>(mapped_rgn->get_address());
        int nnodes = Utils::decode_int(raw_input, 0);
        long pos = 4 + 4 * nnodes;

        int currentFile = -1;
        while (pos < size) {
            bool isFirst = raw_input[pos++];
            CachedNode node;
            unserializeNodeFrom(&node, raw_input, pos);
            pos += NODE_SIZE;
            if (isFirst) {
                nodes.push_back(vector<CachedNode>());
                currentFile++;
            }
            nodes[currentFile].push_back(node);
            totalNumberNodes++;
        }
        delete mapped_rgn;
        delete mapping;
    }

    //2-- Rewrite each file eliminating the blank spaces
    char *supportBuffer = new char[SIZE_SUPPORT_BUFFER];
    fs::remove(fs::path(sFileIdx));
    ofstream fileIdx(sFileIdx);
    int sizeCoordinates = 4 * totalNumberNodes;
    char *coordinatesSpace = new char[sizeCoordinates];
    Utils::encode_int(coordinatesSpace, 0, totalNumberNodes);
    fileIdx.write(coordinatesSpace, 4);
    //Move the file to the next position
    fileIdx.seekp(4 + sizeCoordinates);

    for (int i = 0; i < nodes.size(); ++i) {
        vector<CachedNode> *fileNodes = &nodes[i];
        //Open the old file and create a new file
        fs::path pOldFile(path + string("/") + to_string(i));
        ifstream sOldfile(pOldFile.string());
        fs::path pNewFile(path + string("/") + to_string(i) + ".new");
        ofstream sNewFile(pNewFile.string());

        //Go through all the nodes and copy the contents from the old node to the new one
        int size = fileNodes->size();
        bool first = true;
        for (int j = 0; j < size; ++j) {
            CachedNode *node = &((*fileNodes)[j]);
            //Move to the correct location
            sOldfile.seekg(node->posIndex);
            node->posIndex = sNewFile.tellp();
            sOldfile.read(supportBuffer, node->nodeSize);
            sNewFile.write(supportBuffer, node->nodeSize);
            node->availableSize = node->nodeSize;

            if (first) {
                fileIdx.write(one, 1);
                first = false;
            } else {
                fileIdx.write(zero, 1);
            }
            int p = serializeTo(node, supportBuffer);
            Utils::encode_int(coordinatesSpace, node->id * 4, fileIdx.tellp());
            fileIdx.write(supportBuffer, p);
        }
        sOldfile.close();
        sNewFile.close();

        //Remove the old file
        long oldSize = fs::file_size(pOldFile);
        fs::remove_all(pOldFile);

        //Rename the new file
        long newSize = fs::file_size(pNewFile);
        fs::rename(pNewFile, pOldFile);

        BOOST_LOG_TRIVIAL(debug) << "Oldsize file " << i << ": " << oldSize << " newsize: " << newSize;
    }
    fileIdx.seekp(4);
    fileIdx.write(coordinatesSpace, sizeCoordinates);
    fileIdx.close();
    delete[] supportBuffer;
    delete[] coordinatesSpace;
}

NodeManager::~NodeManager() {

    delete bytesTracker;

    if (!readOnly) {
        string file = path + string("/idx");
        ofstream out(file);
        char supportBuffer[512];

        //Write at the beginning of the file the number of nodes and the position where the nodes are being stored
        Utils::encode_int(supportBuffer, 0, storedNodes.size());
        out.write(supportBuffer, 4);

        //Reserve some space to write the positions where the nodes are being stored.
        int sizeCoordinates = 4 * storedNodes.size();
        char *coordinatesSpace = new char[sizeCoordinates];
        //Move the file to the next position
        out.seekp(4 + sizeCoordinates);

        long wastedSpace = 0;
        for (int i = 0; i < firstElementsPerFile.size(); ++i) {
            CachedNode *node = firstElementsPerFile[i];
            if (node != NULL) {
                out.write(one, 1);
                int p = serializeTo(node, supportBuffer);
                Utils::encode_int(coordinatesSpace, node->id * 4, out.tellp());
                out.write(supportBuffer, p);
                wastedSpace += node->availableSize - node->nodeSize;
                node = node->next;
                while (node != NULL) {
                    out.write(zero, 1);
                    int p = serializeTo(node, supportBuffer);
                    Utils::encode_int(coordinatesSpace, node->id * 4,
                                      out.tellp());
                    out.write(supportBuffer, p);
                    wastedSpace += node->availableSize - node->nodeSize;
                    node = node->next;
                }
            }
        }

        //Write the coordinatesSpace
        out.seekp(4);
        out.write(coordinatesSpace, sizeCoordinates);
        delete[] coordinatesSpace;

        BOOST_LOG_TRIVIAL(debug) << "Wasted space to store the nodes: " << wastedSpace;
        out.close();

        //Clean the stored nodes
        for (boost::unordered_map<long, CachedNode*>::iterator itr =
                    storedNodes.begin(); itr != storedNodes.end(); ++itr) {
            delete itr->second;
        }
        storedNodes.clear();
    } else {
        delete[] readOnlyStoredNodes;
        delete[] nodesLoaded;
        rawInput = NULL;
        delete mapped_rgn;
        delete mapping;
    }

    delete manager;
}

