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

#ifndef MATERIALIZATION_H
#define MATERIALIZATION_H

#include <vlog/concepts.h>
#include <vlog/edb.h>

#include <vector>

class DictMgmt;
class QSQR;
class Materialization {

private:
    bool repeatPrematerialization;
    std::vector<Literal> prematerializedLiterals;
    std::vector<Literal> edbLiterals;

    static bool evaluateQueryThreadedVersion(EDBLayer *kb,
            Program *p,
            DictMgmt *dict,
            QSQQuery *q,
            TupleTable **output,
            long timeoutSeconds);

    bool execMatQuery(Literal &l, bool timeout, EDBLayer &kb,
                      Program &p, DictMgmt *dict, int &predIdx,
                      long timeoutSeconds);

    bool cardIsTooLarge(const Literal &lit, Program &p, EDBLayer &layer);

public:

    Materialization() : repeatPrematerialization(false) {}

    void loadLiteralsFromFile(Program &p, std::string filePath);

    void guessLiteralsFromRules(Program &p, DictMgmt *dict, EDBLayer &layer);

    void getAndStorePrematerialization(EDBLayer &layer, Program &p,
                                       DictMgmt *dict, bool timeout,
                                       long timeoutSeconds);

    void rewriteLiteralInProgram(Literal &prematLiteral,
                                 Literal &rewrittenLiteral, EDBLayer &kb,
                                 Program &p);
};

#endif
