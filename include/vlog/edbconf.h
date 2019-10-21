#ifndef _EDB_CONF_H
#define _EDB_CONF_H

#include <vector>
#include <string>

#include <vlog/consts.h>


class EDBConf {
    public:

        struct Table {
            std::string predname;
            std::string type;
            std::vector<std::string> params;
            bool encoded;
            Table() : encoded(true) {}
            Table(bool encoded) : encoded(encoded) {}
        };

    protected:
        std::vector<Table> tables;
        std::string rootPath;

        void parse(std::string f);

    public:
        VLIBEXP EDBConf(std::string rawcontent, bool isFile);

        EDBConf(std::string rawcontent) : EDBConf(rawcontent, true) {}

        void setRootPath(std::string path);

        std::string getRootPath() const;

        const std::vector<Table> &getTables() const {
            return tables;
        }
};

#endif
