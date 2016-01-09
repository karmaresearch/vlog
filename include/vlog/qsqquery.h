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

#ifndef QUERY_H
#define QUERY_H

#include <vlog/concepts.h>

class QSQQuery {
private:
    const Literal literal;
    uint8_t nPosToCopy;
    uint8_t posToCopy[SIZETUPLE];
    uint8_t nRepeatedVars;
    std::pair<uint8_t, uint8_t> repeatedVars[SIZETUPLE - 1];

public:
    QSQQuery(const Literal literal);

    const Literal *getLiteral() const {
        return &literal;
    }

    uint8_t getNPosToCopy() const {
        return nPosToCopy;
    }

    uint8_t getNRepeatedVars() const {
        return nRepeatedVars;
    }

    std::pair<uint8_t, uint8_t> getRepeatedVar(const uint8_t idx) const {
        return repeatedVars[idx];
    }

    uint8_t *getPosToCopy() {
        return posToCopy;
    }

    std::string tostring();
};

#endif
