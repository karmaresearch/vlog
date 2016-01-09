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

#ifndef _STATS_H
#define _STATS_H


class Stats {
private:
    long readIndexBlocks;
    long readIndexBytes;
public:

    Stats() : readIndexBlocks(0), readIndexBytes(0) {}

    void incrNReadIndexBlocks() {
        readIndexBlocks++;
    }

    void addNReadIndexBytes(const unsigned long bytes) {
        readIndexBytes += bytes;
    }

    unsigned long getNReadIndexBlocks() const {
        return readIndexBlocks;
    }

    unsigned long getNReadIndexBytes() const {
        return readIndexBytes;
    }
};

#endif
