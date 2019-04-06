#ifndef _CHECKER_H
#define _CHECKER_H

#include <vlog/edb.h>
#include <string>
#include <list>

class Checker {
    private:
        static bool JA(Program &p, bool restricted);

        static bool MFA(Program &p);

        static bool RMFA(Program &p);

        static bool MFC(Program &p);

    public:
        static int check(Program &p, std::string alg, EDBLayer &db);

        static int checkFromFile(std::string ruleFile, std::string alg, EDBLayer &db);

        static int checkFromString(std::string rulesString, std::string alg, EDBLayer &db);

};
#endif
