#ifndef _EDB_CONF_H
#define _EDB_CONF_H

#include <string>
#include <vector>

using namespace std;

class EDBConf {
public:

    struct Table {
        string predname;
        string type;
        std::vector<string> params;
    };

private:
    std::vector<Table> tables;

    void parse(string f);

public:
    EDBConf(string rawcontent, bool isFile);

    EDBConf(string rawcontent) : EDBConf(rawcontent, true) {}

    const std::vector<Table> &getTables() {
        return tables;
    }
};

#endif
