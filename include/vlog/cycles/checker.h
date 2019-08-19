#ifndef _CHECKER_H
#define _CHECKER_H

#include <vlog/edb.h>
#include <string>
#include <list>

class Checker {
    private:
        static bool JA(Program &p, bool restricted);

        static bool MFA(Program &p, std::string sameasAlgo = "");

        static bool MSA(Program &p);

        static bool EMFA(Program &p);

        static bool RMFA(Program &p);

        static bool RMSA(Program &p);

        static bool MFC(Program &p, bool restricted = false);

        static void createCriticalInstance(Program &newProgram,
                Program &p,
                EDBLayer *db,
                EDBLayer &layer);

        static void addBlockCheckTargets(Program &p, PredId_t ignorePred = -1);

        static Program *getProgramForBlockingCheckRMFC(Program &p);

    public:
        VLIBEXP static int check(Program &p, std::string alg,
                std::string sameasAlgo, EDBLayer &db);

        VLIBEXP static int checkFromFile(std::string ruleFile,
                std::string alg, std::string sameasAlgo,  EDBLayer &db,
                bool rewriteMultihead = false);

        VLIBEXP static int checkFromString(std::string rulesString,
                std::string alg, std::string sameasAlgo,  EDBLayer &db,
                bool rewriteMultihead = false);

};
#endif
