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

#ifndef REASONER_H
#define REASONER_H

#include <vlog/concepts.h>
#include <vlog/edb.h>
#include <vlog/fctable.h>
#include <vlog/seminaiver.h>

#include <trident/kb/kb.h>
#include <trident/kb/querier.h>

#include <trident/sparql/query.h>

#define QUERY_MAT 0
#define QUERY_ONDEM 1

typedef enum {TOPDOWN, MAGIC} ReasoningMode;

class Reasoner {
private:

    const uint64_t threshold;

    void cleanBindings(std::vector<Term_t> &bindings, std::vector<uint8_t> * posJoins,
                       TupleTable *input);

    TupleTable *getVerifiedBindings(QSQQuery &query,
                                    std::vector<uint8_t> * posJoins,
                                    std::vector<Term_t> *possibleValuesJoins,
                                    EDBLayer &layer, Program &program, DictMgmt *dict,
                                    bool returnOnlyVars);

    FCBlock getBlockFromQuery(Literal constantsQuery, Literal &boundQuery, std::vector<uint8_t> *posJoins,
                              std::vector<Term_t> *possibleValuesJoins);
public:

    Reasoner(const uint64_t threshold) : threshold(threshold) {}

    ReasoningMode chooseMostEfficientAlgo(Pattern *pattern,
                                          EDBLayer &layer, Program &program,
                                          DictMgmt *dict,
                                          std::vector<uint8_t> *posBindings,
                                          std::vector<Term_t> *valueBindings);

    TupleIterator *getTopDownIterator(Pattern *pattern,
                                      std::vector<uint8_t> * posJoins,
                                      std::vector<Term_t> *possibleValuesJoins,
                                      EDBLayer &layer, Program &program,
                                      DictMgmt *dict,
                                      bool returnOnlyVars);

    TupleIterator *getMagicIterator(Pattern *pattern,
                                    std::vector<uint8_t> * posJoins,
                                    std::vector<Term_t> *possibleValuesJoins,
                                    EDBLayer &layer, Program &program,
                                    DictMgmt *dict,
                                    bool returnOnlyVars);

    /*
    TupleIterator *getIncrReasoningIterator(Pattern *pattern,
                                            std::vector<uint8_t> * posJoins,
                                            std::vector<Term_t> *possibleValuesJoins,
                                            EDBLayer &layer, Program &program,
                                            DictMgmt *dict,
                                            bool returnOnlyVars);
    */

    size_t estimate(Pattern *pattern, std::vector<uint8_t> *posBindings,
                  std::vector<Term_t> *valueBindings, EDBLayer &layer,
                  Program &, DictMgmt *dict);

    static std::shared_ptr<SemiNaiver> fullMaterialization(KB *kb, EDBLayer &layer,
            Program *p, bool opt_intersect, bool opt_filtering);

    //static int materializationOrOnDemand(const uint64_t matThreshold, std::vector<std::shared_ptr<SPARQLOperator>> &patterns);

    ~Reasoner() {
    }
};
#endif
