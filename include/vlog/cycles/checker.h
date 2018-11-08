#ifndef _CHECKER_H
#define _CHECKER_H

#include <vlog/edb.h>

#include <string>

class Checker {
    public:
        static int check(std::string ruleFile, std::string alg, EDBLayer &db);

	static bool JA(Program &p);

	static void closure(Program &p, std::vector<std::pair<PredId_t, uint8_t>> &input);
};

#endif
