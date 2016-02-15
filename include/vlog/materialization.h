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
            QSQQuery *q,
            TupleTable **output,
            long timeoutSeconds);

    bool execMatQuery(Literal &l, bool timeout, EDBLayer &kb,
                      Program &p, int &predIdx, long timeoutSeconds);

    bool cardIsTooLarge(const Literal &lit, Program &p, EDBLayer &layer);

public:

    Materialization() : repeatPrematerialization(false) {}

    void loadLiteralsFromFile(Program &p, std::string filePath);

    void guessLiteralsFromRules(Program &p, EDBLayer &layer);

    void getAndStorePrematerialization(EDBLayer &layer, Program &p,
                                        bool timeout,
                                       long timeoutSeconds);

    void rewriteLiteralInProgram(Literal &prematLiteral,
                                 Literal &rewrittenLiteral, EDBLayer &kb,
                                 Program &p);
};

#endif
