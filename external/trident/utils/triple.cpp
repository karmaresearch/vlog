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

#include <tridentcompr/utils/triple.h>
#include <tridentcompr/utils/lz4io.h>

void Triple::readFrom(LZ4Reader *reader) {
	s = reader->parseVLong();
	p = reader->parseVLong();
	o = reader->parseVLong();
    count = reader->parseVLong();
}

void Triple::writeTo(LZ4Writer *writer) {
    writer->writeVLong(s);
    writer->writeVLong(p);
    writer->writeVLong(o);
    writer->writeVLong(count);
}
