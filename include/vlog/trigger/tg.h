#ifndef _TG_H
#define _TG_H

#include <vlog/concepts.h>
#include <vlog/edb.h>

#include <string>
#include <vector>

class TriggerGraph {
    private:
        struct Node {
            int ruleID;
            std::string label;
            std::vector<Node*> incoming;
            std::vector<Node*> outgoing;
        };

        std::vector<Node> nodes;

        void processNode(const Node &n, std::ostream &out);

    public:
        TriggerGraph();

        void createLinear(EDBLayer &db, Program &p);

        void createKBound(EDBLayer &db, Program &p);

        void saveAllPaths(std::ostream &out);
};

#endif
