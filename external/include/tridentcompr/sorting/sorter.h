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

#ifndef SORTER_H_
#define SORTER_H_

#include "../utils/utils.h"
#include "../utils/lz4io.h"

#include <string>
#include <vector>

using namespace std;

class Sorter {
private:
    static void sort(vector<string> &inputFiles, int filesPerMerge,
                     string prefixOutputFiles);

    static void sortUnsortedFiles(vector<string> &inputFiles, string dir,
                                  string prefixOutputFiles, int fileSize);

public:
    static void sortBufferAndWriteToFile(vector<Triple> &vector,
                                         string fileOutput);

    static void mergeSort(string inputDir, int nThreads, bool initialSorting,
                          int recordsInitialMemorySort, int filesPerMerge);

    template<class K>
    static vector<string> sortFiles(vector<string> inputFiles,
                                    string prefixOutputFile) {
        long maxSizeToSort = max((long) (BLOCK_SUPPORT_BUFFER_COMPR * 2),
                                 (long) (Utils::getSystemMemory() * 0.70));
        int sizeEl = sizeof(K);
        long currentSize = 0;
        int idxFile = 0;

        vector<K> inmemoryContainer;
        vector<string> output;
        for (vector<string>::iterator itr = inputFiles.begin(); itr != inputFiles.end();
                itr++) {
            LZ4Reader reader(*itr);
            while (!reader.isEof()) {
                K el;
                el.readFrom(&reader);
                if (currentSize + sizeEl > maxSizeToSort) {
                    std::sort(inmemoryContainer.begin(), inmemoryContainer.end(), K::less);
                    string outputFile = prefixOutputFile + "." + to_string(idxFile++);
                    LZ4Writer writer(outputFile);
                    for (typename vector<K>::iterator itr = inmemoryContainer.begin(); itr !=
                            inmemoryContainer.end(); ++itr) {
                        itr->writeTo(&writer);
                    }
                    currentSize = 0;
                    inmemoryContainer.clear();
                    output.push_back(outputFile);
                }

                inmemoryContainer.push_back(el);
                currentSize += sizeEl;
            }
        }

        if (inmemoryContainer.size() > 0) {
            std::sort(inmemoryContainer.begin(), inmemoryContainer.end(), K::less);
            string outputFile = prefixOutputFile + "." + to_string(idxFile++);
            LZ4Writer writer(outputFile);
            for (typename vector<K>::iterator itr = inmemoryContainer.begin(); itr !=
                    inmemoryContainer.end(); ++itr) {
                itr->writeTo(&writer);
            }
            inmemoryContainer.clear();
            output.push_back(outputFile);
        }

        return output;
    }

};

#endif /* SORTER_H_ */


