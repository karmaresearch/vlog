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

#include <trident/kb/dictmgmt.h>
#include <trident/tree/root.h>
#include <trident/tree/stringbuffer.h>
#include <trident/tree/treeitr.h>

#include <tridentcompr/utils/hashfunctions.h>

#include <lz4.h>
#include <boost/thread.hpp>
#include <boost/log/trivial.hpp>
#include <boost/chrono.hpp>

#include <iostream>
#include <fstream>

using namespace std;
namespace timens = boost::chrono;

void DictMgmt::threadlookupTerms(long *data, int nTuples, int sTuples,
                                 bool printTuples) {
    boost::unique_lock<boost::mutex> lock(mutex1);
    while (!threadIsWaiting) {
        canAddData.wait(lock);
    }

    //Setup the task to execute
    dataToProcess = data;
    this->nTuples = nTuples;
    this->printTuples = printTuples;
    this->sTuples = sTuples;

    waitForData.notify_one();
}

void DictMgmt::lookupTerms(long *data, int nTuples, int sTuples,
                           bool printTuples) {

    //timens::system_clock::time_point start = timens::system_clock::now();

    row.setSize(sTuples);
    int tCounter = 0;
    for (int i = 0; i < nTuples; ++i) {
        for (int j = 0; j < sTuples; ++j) {
            getText(data[tCounter++], row.getRawData(j));
        }
        if (printTuples) {
            row.printRow();
        }
    }


    //boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
    //                                      - start;
    //BOOST_LOG_TRIVIAL(debug) << "Time looking up the elements = " << sec.count() * 1000 << " ms";
}

TreeItr *DictMgmt::getInvDictIterator(int part) {
    return invDicts[part]->itr();
}

bool DictMgmt::getText(nTerm key, char *value) {

    int index = key % dictPartitions;
    long coordinates;
    if (invDicts[index]->get(key, coordinates)) {
        int size = 0;
        stringbuffers[index]->get(coordinates, value, size);
        value[size] = '\0';
        return true;
    }
    return false;
}

void DictMgmt::getTextFromCoordinates(int part, long coordinates, char *output,
                                      int &sizeOutput) {
    stringbuffers[part]->get(coordinates, output, sizeOutput);
    output[sizeOutput] = '\0';
}

int DictMgmt::getDictPartition(const char *key, const int sizeKey) {
    return Utils::getPartition(key, sizeKey, dictPartitions);
}

bool DictMgmt::getNumber(const char *key, const int sizeKey, nTerm *value) {

    int index = getDictPartition(key, sizeKey);
    return dicts[index]->get((tTerm*) key, sizeKey, value);
}

bool DictMgmt::putPair(const char *key, int sizeKey, nTerm &value) {
    return putPair(getDictPartition(key, sizeKey), key, sizeKey, value);
}

bool DictMgmt::putPair(int part, const char *key, int sizeKey, nTerm &value) {
    long coordinates = stringbuffers[part]->getSize();
    if (dicts[part]->insertIfNotExists((tTerm*) key, sizeKey, value)) {
        invDicts[part]->put(value, coordinates);
        insertedNewTerms[part]++;
        return true;
    }
    return false;
}

bool DictMgmt::putDict(int part, const char *key, int sizeKey, nTerm &value) {
    if (dicts[part]->insertIfNotExists((tTerm*) key, sizeKey, value)) {
        insertedNewTerms[part]++;
        return true;
    }
    return false;
}

bool DictMgmt::putDict(int part, const char *key, int sizeKey, nTerm &value,
                       long &coordinates) {
    coordinates = stringbuffers[part]->getSize();
    if (dicts[part]->insertIfNotExists((tTerm*) key, sizeKey, value)) {
        insertedNewTerms[part]++;
        return true;
    }
    return false;
}

bool DictMgmt::putInvDict(int part, const char *key, int sizeKey, nTerm &value) {
    long coordinates = stringbuffers[part]->getSize();
    stringbuffers[part]->append((char*) key, sizeKey);
    invDicts[part]->put(value, coordinates);
    return true;
}

bool DictMgmt::putInvDict(int part, const nTerm key, const long coordinates) {
    invDicts[part]->put(key, coordinates);
    return true;

}

//void DictMgmt::loadStandardTerms() {
//  standardTerms.insert(std::make_pair((char*) S_RDF_NIL, (nTerm) RDF_NIL));
//  standardTerms.insert(std::make_pair((char*) S_RDF_LIST, (nTerm) RDF_LIST));
//  standardTerms.insert(
//          std::make_pair((char*) S_RDF_FIRST, (nTerm) RDF_FIRST));
//  standardTerms.insert(std::make_pair((char*) S_RDF_REST, (nTerm) RDF_REST));
//  standardTerms.insert(std::make_pair((char*) S_RDF_TYPE, (nTerm) RDF_TYPE));
//  standardTerms.insert(
//          std::make_pair((char*) S_RDF_PROPERTY, (nTerm) RDF_PROPERTY));
//  standardTerms.insert(
//          std::make_pair((char*) S_RDFS_RANGE, (nTerm) RDFS_RANGE));
//  standardTerms.insert(
//          std::make_pair((char*) S_RDFS_DOMAIN, (nTerm) RDFS_DOMAIN));
//  standardTerms.insert(
//          std::make_pair((char*) S_RDFS_SUBPROPERTY,
//                  (nTerm) RDFS_SUBPROPERTY));
//  standardTerms.insert(
//          std::make_pair((char*) S_RDFS_SUBCLASS, (nTerm) RDFS_SUBCLASS));
//  standardTerms.insert(
//          std::make_pair((char*) S_RDFS_MEMBER, (nTerm) RDFS_MEMBER));
//  standardTerms.insert(
//          std::make_pair((char*) S_RDFS_LITERAL, (nTerm) RDFS_LITERAL));
//  standardTerms.insert(
//          std::make_pair((char*) S_RDFS_CONTAINER_MEMBERSHIP_PROPERTY,
//                  (nTerm) RDFS_CONTAINER_MEMBERSHIP_PROPERTY));
//  standardTerms.insert(
//          std::make_pair((char*) S_RDFS_DATATYPE, (nTerm) RDFS_DATATYPE));
//  standardTerms.insert(
//          std::make_pair((char*) S_RDFS_CLASS, (nTerm) RDFS_CLASS));
//  standardTerms.insert(
//          std::make_pair((char*) S_RDFS_RESOURCE, (nTerm) RDFS_RESOURCE));
//  standardTerms.insert(
//          std::make_pair((char*) S_OWL_CLASS, (nTerm) OWL_CLASS));
//  standardTerms.insert(
//          std::make_pair((char*) S_OWL_FUNCTIONAL_PROPERTY,
//                  (nTerm) OWL_FUNCTIONAL_PROPERTY));
//  standardTerms.insert(
//          std::make_pair((char*) S_OWL_INVERSE_FUNCTIONAL_PROPERTY,
//                  (nTerm) OWL_INVERSE_FUNCTIONAL_PROPERTY));
//  standardTerms.insert(
//          std::make_pair((char*) S_OWL_SYMMETRIC_PROPERTY,
//                  (nTerm) OWL_SYMMETRIC_PROPERTY));
//  standardTerms.insert(
//          std::make_pair((char*) S_OWL_TRANSITIVE_PROPERTY,
//                  (nTerm) OWL_TRANSITIVE_PROPERTY));
//  standardTerms.insert(
//          std::make_pair((char*) S_OWL_SAME_AS, (nTerm) OWL_SAME_AS));
//  standardTerms.insert(
//          std::make_pair((char*) S_OWL_INVERSE_OF, (nTerm) OWL_INVERSE_OF));
//  standardTerms.insert(
//          std::make_pair((char*) S_OWL_EQUIVALENT_CLASS,
//                  (nTerm) OWL_EQUIVALENT_CLASS));
//  standardTerms.insert(
//          std::make_pair((char*) S_OWL_EQUIVALENT_PROPERTY,
//                  (nTerm) OWL_EQUIVALENT_PROPERTY));
//  standardTerms.insert(
//          std::make_pair((char*) S_OWL_HAS_VALUE, (nTerm) OWL_HAS_VALUE));
//  standardTerms.insert(
//          std::make_pair((char*) S_OWL_ON_PROPERTY, (nTerm) OWL_ON_PROPERTY));
//  standardTerms.insert(
//          std::make_pair((char*) S_OWL_SOME_VALUES_FROM,
//                  (nTerm) OWL_SOME_VALUES_FROM));
//  standardTerms.insert(
//          std::make_pair((char*) S_OWL_ALL_VALUES_FROM,
//                  (nTerm) OWL_ALL_VALUES_FROM));
//  standardTerms.insert(
//          std::make_pair((char*) S_OWL2_PROPERTY_CHAIN_AXIOM,
//                  (nTerm) OWL2_PROPERTY_CHAIN_AXIOM));
//  standardTerms.insert(
//          std::make_pair((char*) S_OWL2_HAS_KEY, (nTerm) OWL2_HAS_KEY));
//  standardTerms.insert(
//          std::make_pair((char*) S_OWL2_INTERSECTION_OF,
//                  (nTerm) OWL2_INTERSECTION_OF));
//  standardTerms.insert(
//          std::make_pair((char*) S_OWL2_UNION_OF, (nTerm) OWL2_UNION_OF));
//  standardTerms.insert(
//          std::make_pair((char*) S_OWL2_ONE_OF, (nTerm) OWL2_ONE_OF));
//  standardTerms.insert(
//          std::make_pair((char*) S_OWL2_THING, (nTerm) OWL2_THING));
//  standardTerms.insert(std::make_pair((char*) S_OWL2_1, (nTerm) OWL2_1));
//  standardTerms.insert(
//          std::make_pair((char*) S_OWL2_MAX_CARD, (nTerm) OWL2_MAX_CARD));
//  standardTerms.insert(
//          std::make_pair((char*) S_OWL2_MAX_Q_CARD, (nTerm) OWL2_MAX_Q_CARD));
//  standardTerms.insert(
//          std::make_pair((char*) S_OWL2_ON_CLASS, (nTerm) OWL2_ON_CLASS));
//  standardTerms.insert(
//          std::make_pair((char*) S_OWL2_NOTHING, (nTerm) OWL2_NOTHING));
//  standardTerms.insert(
//          std::make_pair((char*) S_OWL2_DATATYPE_PROP,
//                  (nTerm) OWL2_DATATYPE_PROP));
//  standardTerms.insert(
//          std::make_pair((char*) S_OWL2_OBJECT_PROP,
//                  (nTerm) OWL2_OBJECT_PROP));
//}

/*void DictMgmt::run() {
    while (true) {
        boost::unique_lock<boost::mutex> lock(mutex2);
        while (dataToProcess == NULL) {
            waitForData.wait(lock);
        }
        threadIsWaiting = false;
        lookupTerms(dataToProcess, nTuples, sTuples, printTuples);
        dataToProcess = NULL;
        threadIsWaiting = true;
        canAddData.notify_all();
    }
}*/

void DictMgmt::waitUntilFinish() {
    boost::unique_lock<boost::mutex> lock(mutex1);
    while (dataToProcess != NULL) {
        canAddData.wait(lock);
    }
}

int uncompressBuffer(ifstream &is, char *compressedBuffer,
                     char *uncompressedBuffer) {
    //The total header size is 21 bytes
    char header[21];
    is.read(header, 21);
    //First 8 bytes is a fixed string (LZ4Block). Then there is one token byte.
    int compressedLen = Utils::decode_intLE(header, 9);
    int uncompressedLen = Utils::decode_intLE(header, 13);
    is.read(compressedBuffer, compressedLen);

    if (!LZ4_decompress_fast(compressedBuffer, uncompressedBuffer,
                             uncompressedLen)) {
        BOOST_LOG_TRIVIAL(error) << "Error in the decompression.";
    }
    return uncompressedLen;
}

void DictMgmt::parseDictFile(string path) {
    std::ifstream is(path.c_str());
    char compressedBuffer[70 * 1024]; //Temporary buffer. A bit bigger than 32K
    char uncompressedBuffer[65 * 1024];
    char textualTerm[4096];
    int uncompressedBufferLen = 0;
    int currentOffset = 0;
    long counter = 0;

    while (!is.eof() || currentOffset < uncompressedBufferLen - 1) {
        ++counter;

        /*** READ THE LONG NUMBER ***/
        long numericTerm = 0;
        if (currentOffset + 7 < uncompressedBufferLen) {
            //Parse it normally
            numericTerm = Utils::decode_long(uncompressedBuffer, currentOffset);
            currentOffset += 8;
        } else {
            //Need to parse the next buffer
            int numBytes = 7;
            for (; currentOffset < uncompressedBufferLen; numBytes--) {
                numericTerm += (uncompressedBuffer[currentOffset++] & 0xFF)
                               << (numBytes * 8);
            }

            //Get the new buffer
            uncompressedBufferLen = uncompressBuffer(is, compressedBuffer,
                                    uncompressedBuffer);
            if (uncompressedBufferLen == 0) {
                break; //Finished reading
            }
            currentOffset = 0;

            //Read the remaining bytes
            for (; numBytes >= 0; numBytes--) {
                numericTerm += (uncompressedBuffer[currentOffset++] & 0xFF)
                               << (numBytes * 8);
            }
        }

        /*** READ THE STRING ***/
        //First we must read the size of the string, which is stored in a 2-bytes number
        int sizeString = 0;
        if (currentOffset + 1 < uncompressedBufferLen) {
            sizeString = Utils::decode_short((const char*)uncompressedBuffer, currentOffset);
            currentOffset += 2;
        } else {
            int numBytes = 1;
            for (; currentOffset < uncompressedBufferLen; numBytes--) {
                sizeString += (uncompressedBuffer[currentOffset++] & 0xFF)
                              << (numBytes * 8);
            }

            //Get the new buffer
            uncompressedBufferLen = uncompressBuffer(is, compressedBuffer,
                                    uncompressedBuffer);
            currentOffset = 0;

            //Read the remaining bytes
            for (; numBytes >= 0; numBytes--) {
                sizeString += (uncompressedBuffer[currentOffset++] & 0xFF)
                              << (numBytes * 8);
            }
        }

        //Now we can read the entire string
        if (currentOffset + sizeString <= uncompressedBufferLen) {
            memcpy(textualTerm, uncompressedBuffer + currentOffset, sizeString);
            currentOffset += sizeString;
        } else {
            int diff = uncompressedBufferLen - currentOffset;
            memcpy(textualTerm, uncompressedBuffer + currentOffset, diff);
            uncompressedBufferLen = uncompressBuffer(is, compressedBuffer,
                                    uncompressedBuffer);
            currentOffset = sizeString - diff;
            memcpy(textualTerm + diff, uncompressedBuffer, currentOffset);
        }
        textualTerm[sizeString] = 0;

        /*** ADD THE PAIR TO THE DICTIONARIES ***/
//      putPair((const char *)textualTerm, sizeString, (nTerm)numericTerm);
    }

    BOOST_LOG_TRIVIAL(info) << "Inserted " << counter << " entries ";
}

void DictMgmt::loadDict(string dirFiles) {
    //1- load the common terms.
    vector<string> files = Utils::getFilesWithPrefix(dirFiles, "c-");

    //2- Sort them
    sort(files.begin(), files.end());

    //3- Load the entries in the trees
    for (vector<string>::iterator it = files.begin(); it != files.end(); ++it) {
        string fileName = dirFiles + string("/") + *it;
        parseDictFile(fileName);
    }

    //4- Load all the other entries in the trees.
    files = Utils::getFilesWithPrefix(dirFiles, "n-");
    if (files.size() != dictPartitions) {
        BOOST_LOG_TRIVIAL(error) << "The number of files is not the same as the number of partitions";
    }

    boost::thread **threads = new boost::thread*[dictPartitions];

    int i = 0;
    for (vector<string>::iterator it = files.begin(); it != files.end(); ++it) {
        string fileName = dirFiles + string("/") + *it;
//  string fileName = dirFiles + string("/") + files[0];
        threads[i++] = new boost::thread(
            boost::bind(&DictMgmt::parseDictFile, this, fileName));
    }

    //5- Wait until the threads are finished...
    for (int j = 0; j < dictPartitions; ++j) {
        threads[j]->join();
    }

    //6- Deallocate the threads
    for (int j = 0; j < dictPartitions; ++j) {
        delete threads[j];
    }
    delete[] threads;
}
