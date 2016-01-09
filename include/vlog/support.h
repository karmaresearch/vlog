/*
   Copyright (C) 2015 Jacopo Urbani.

   This file is part of Vlog.

   Vlog is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 2 of the License, or
   (at your option) any later version.

   Vlog is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with Vlog.  If not, see <http://www.gnu.org/licenses/>.
*/

#ifndef SUPPORT_H
#define SUPPORT_H

#include <google/dense_hash_map>
#include <string>
#include <functional>
#include <vector>

#include "term.h"

struct eqstr {
    bool operator()(const std::string &v1, const std::string &v2) const {
        return v1.compare(v2) == 0;
    }
};
typedef google::dense_hash_map<const std::string, Term_t, std::hash<std::string>, eqstr> SimpleHashmap;
typedef google::dense_hash_map<Term_t, const std::string> SimpleInverseHashMap;

class Dictionary {
private:
    SimpleHashmap map;
    SimpleInverseHashMap inverseMap;
    uint64_t counter;
public:
    Dictionary() : Dictionary(1) {
    }

    Dictionary(uint64_t startingCounter) : counter(startingCounter) {
        map.set_empty_key("");
        inverseMap.set_empty_key(0);
    }

    Term_t getOrAdd(std::string &rawValue) {
        SimpleHashmap::iterator itr = map.find(rawValue);
        if (itr == map.end()) {
            //Add value
            map.insert(std::make_pair(rawValue, (Term_t) counter));
            inverseMap.insert(std::make_pair((Term_t) counter, rawValue));
            return (Term_t) counter++;
        } else {
            return itr->second;
        }
    }

    std::string getRawValue(const Term_t id) {
        SimpleInverseHashMap::iterator itr = inverseMap.find(id);
        return itr->second;
    }

    std::string tostring() {
        std::string output = "";
        for (SimpleHashmap::iterator itr = map.begin(); itr != map.end(); ++itr) {
            output += itr->first + std::string(" ") + std::to_string(itr->second) + std::string(" ");
        }
        return output;
    }

    size_t size() {
        return map.size();
    }
};

class ReasoningUtils {
public:
    static int cmp(const Term_t *r1, const Term_t *r2, const size_t s) {
        for (size_t i = 0; i < s; ++i) {
            if (r1[i] < r2[i])
                return -1;
            else if (r1[i] > r2[i])
                return 1;
        }
        return 0;
    }

    static void copyArray(std::vector<Term_t> &dest, const Term_t *row, const uint8_t nfields) {
        for (size_t i = 0; i < nfields; ++i) {
            dest.push_back(row[i]);
        }
    }

    static void readArray(Term_t *dest, std::vector<Term_t>::const_iterator &itr, const uint8_t nfields) {
        for (size_t i = 0; i < nfields; ++i) {
            dest[i] = *itr;
            itr++;
        }
    }
};
#endif
