#ifndef _EDB_CONF_H
#define _EDB_CONF_H

#include <vlog/consts.h>

#include <string>
#include <vector>

using namespace std;

class EDBConf {
public:

    struct Table {
        string predname;
        string type;
        std::vector<string> params;
        bool encoded;
        Table() : encoded(true) {}
        Table(bool encoded) : encoded(encoded) {}
    };

protected:
    std::vector<Table> tables;

    void parse(string f);

public:
    VLIBEXP EDBConf(string rawcontent, bool isFile);

    EDBConf(string rawcontent) : EDBConf(rawcontent, true) {}

    const std::vector<Table> &getTables() {
        return tables;
    }
};

#endif
