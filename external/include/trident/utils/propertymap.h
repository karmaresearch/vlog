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

#ifndef PM_H_
#define PM_H_

#include <boost/log/trivial.hpp>

#include <string>
#include <map>
#include <cstring>

using namespace std;

class PropertyMap {
private:
    map<int, char *> privateMap;

public:
    void setInt(int key, int value) {
        string svalue = to_string(value);
        set(key, svalue);
    }

    int getInt(int key) {
        map<int, char *>::iterator itr = privateMap.find(key);
        if (itr != privateMap.end()) {
            return stoi(itr->second);
        }
        BOOST_LOG_TRIVIAL(error) << "Not found";
        return 0;
    }

    void setLong(int key, long value) {
        string svalue = to_string(value);
        set(key, svalue);
    }

    long getLong(int key) {
        map<int, char *>::iterator itr = privateMap.find(key);
        if (itr != privateMap.end()) {
            return stol(itr->second);
        }
        BOOST_LOG_TRIVIAL(error) << "Not found";
        return 0;
    }

    void setBool(int key, bool value) {
        string svalue = to_string(value);
        set(key, svalue);
    }

    bool getBool(int key) {
        map<int, char *>::iterator itr = privateMap.find(key);
        if (itr != privateMap.end()) {
            return stoi(itr->second) != 0;
        }
        BOOST_LOG_TRIVIAL(error) << "Not found";
        return 0;
    }

    void set(int key, string value) {
        map<int, char*>::iterator itr = privateMap.find(key);
        if (itr != privateMap.end()) {
            delete[] itr->second;
            char *newel = new char[value.size() + 1];
            strcpy(newel, value.c_str());
            itr->second = newel;
        } else {
            char *newel = new char[value.size() + 1];
            strcpy(newel, value.c_str());
            privateMap.insert(make_pair(key, newel));
        }
    }

    string get(int key) {
        map<int, char *>::iterator itr = privateMap.find(key);
        if (itr != privateMap.end()) {
            return itr->second;
        }
        BOOST_LOG_TRIVIAL(error) << "Not found";
        return "";
    }

    ~PropertyMap() {
        for (map<int, char *>::iterator itr = privateMap.begin(); itr != privateMap.end(); ++itr) {
            delete[] itr->second;
        }
        privateMap.clear();
    }
};

#endif
