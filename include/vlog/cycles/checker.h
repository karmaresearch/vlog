#ifndef _CHECKER_H
#define _CHECKER_H

#include <vlog/edb.h>

#include <string>

class Checker {
    public:
        static int check(std::string ruleFile, EDBLayer &db);
};

#endif
