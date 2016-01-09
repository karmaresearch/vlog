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

#include <tridentcompr/sorting/sorter.h>
#include <tridentcompr/sorting/filemerger.h>
#include <tridentcompr/utils/utils.h>
#include <tridentcompr/utils/lz4io.h>
#include <tridentcompr/utils/triplewriters.h>

#include <boost/filesystem.hpp>
#include <boost/thread.hpp>
#include <vector>
#include <algorithm>

namespace fs = boost::filesystem;

void Sorter::sortUnsortedFiles(vector<string> &inputFiles, string dir,
                               string prefixOutputFiles, int fileSize) {
    SortedTripleWriter writer(dir, prefixOutputFiles, fileSize);
    for (vector<string>::iterator itr = inputFiles.begin();
            itr != inputFiles.end(); ++itr) {
        LZ4Reader reader(*itr);
        const bool quad = reader.parseByte() != 0;
        while (!reader.isEof()) {
            long t1 = reader.parseLong();
            long t2 = reader.parseLong();
            long t3 = reader.parseLong();
            if (quad) {
                long count = reader.parseLong();
                writer.write(t1, t2, t3, count);
            } else {
                writer.write(t1, t2, t3);
            }
        }
        fs::remove(*itr);
    }
}

void Sorter::sort(vector<string> &inputFiles, int filesPerMerge,
                  string prefixOutputFiles) {
    int segment = 0;
    while (inputFiles.size() > 0) {
        //Take out the filesPerMerge lastFiles
        vector<string> inputForSorting;
        for (int i = 0; i < filesPerMerge && inputFiles.size() > 0; ++i) {
            inputForSorting.push_back(inputFiles.back());
            inputFiles.pop_back();
        }

        //Sort them and write a new file
        FileMerger<Triple> merger(inputForSorting);
        string fileOutput = prefixOutputFiles + string("-")
                            + to_string(segment);
        LZ4Writer writer(fileOutput);
        while (!merger.isEmpty()) {
            Triple t = merger.get();
            t.writeTo(&writer);
        }

        //Delete the old files
        for (vector<string>::iterator itr = inputForSorting.begin();
                itr != inputForSorting.end(); ++itr) {
            fs::remove(fs::path(*itr));
        }
        segment++;
    }
}

bool TripleCmp(const Triple &t1, const Triple &t2) {
    if (t1.s < t2.s) {
        return true;
    } else if (t1.s == t2.s) {
        if (t1.p < t2.p) {
            return true;
        } else if (t1.p == t2.p) {
            return t1.o < t2.o;
        }
    }
    return false;
}

void Sorter::sortBufferAndWriteToFile(vector<Triple> &v, string fileOutput) {
    std::sort(v.begin(), v.end(), TripleCmp);
    LZ4Writer writer(fileOutput);
    for (vector<Triple>::iterator itr = v.begin(); itr != v.end(); ++itr) {
        itr->writeTo(&writer);
    }
}

void Sorter::mergeSort(string inputDir, int nThreads, bool initialSorting,
                       int fileSize, int filesPerMerge) {
    int filesInDir = 0;
    int iteration = 0;

    /*** SORT THE ORIGINAL FILES IN BLOCKS OF N RECORDS ***/
    if (initialSorting) {
        vector<string> unsortedFiles = Utils::getFiles(inputDir);
        vector<string> *splits = new vector<string> [nThreads];
        //Give each file to a different split
        int currentSplit = 0;
        for (vector<string>::iterator itr = unsortedFiles.begin();
                itr != unsortedFiles.end(); ++itr) {
            splits[currentSplit].push_back(*itr);
            currentSplit = (currentSplit + 1) % nThreads;
        }
        //Sort the files
        boost::thread *threads = new boost::thread[nThreads - 1];
        for (int i = 1; i < nThreads; ++i) {
            string prefixOutputFile = string("/sorted-inputfile-")
                                      + to_string(i);
            threads[i - 1] = boost::thread(
                                 boost::bind(&Sorter::sortUnsortedFiles, splits[i], inputDir,
                                             prefixOutputFile, fileSize));
        }
        string prefixOutputFile = string("/sorted-inputfile-0");
        sortUnsortedFiles(splits[0], inputDir, prefixOutputFile, fileSize);
        for (int i = 1; i < nThreads; ++i) {
            threads[i - 1].join();
        }
        delete[] threads;
        delete[] splits;
    }

    /*** MERGE SORT ***/
    BOOST_LOG_TRIVIAL(debug) << "Start merge sorting procedure";
    do {
        //Read all the files and store them in a vector
        vector<string> files = Utils::getFiles(inputDir);
        filesInDir = files.size();
        if (files.size() <= nThreads) {
            return; //No need to do sorting
        }

        BOOST_LOG_TRIVIAL(debug) << "(Sorted) files to merge: " << files.size() << " maxLimit: " << nThreads;

        //Split the files in nThreads splits
        vector<string> *splits = new vector<string> [nThreads];
        int currentSplit = 0;
        for (vector<string>::iterator itr = files.begin(); itr != files.end();
                ++itr) {
            splits[currentSplit].push_back(*itr);
            currentSplit = (currentSplit + 1) % nThreads;
        }

        //Start the threads and wait until they are finished
        boost::thread *threads = new boost::thread[nThreads - 1];
        for (int i = 1; i < nThreads; ++i) {
            string prefixOutputFile = inputDir + string("/merged-file-")
                                      + to_string(i) + string("-") + to_string(iteration);
            threads[i - 1] = boost::thread(
                                 boost::bind(&Sorter::sort, splits[i], filesPerMerge,
                                             prefixOutputFile));
        }
        string prefixOutputFile = inputDir + string("/merged-file-0-")
                                  + to_string(iteration);
        sort(splits[0], filesPerMerge, prefixOutputFile);
        for (int i = 1; i < nThreads; ++i) {
            threads[i - 1].join();
        }

        delete[] threads;
        delete[] splits;
        iteration++;
    } while (filesInDir / filesPerMerge > nThreads);
}
