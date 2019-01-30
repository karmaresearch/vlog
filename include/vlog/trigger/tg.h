#ifndef _TG_H
#define _TG_H

#include <vlog/concepts.h>
#include <vlog/edb.h>

#include <string>
#include <vector>
#include <unordered_set>

class TriggerGraph {
    private:
        struct Node {
            int ruleID;
            std::string label;
            std::vector<std::shared_ptr<Node>> incoming;
            std::vector<std::shared_ptr<Node>> outgoing;
            std::unique_ptr<Literal> literal;
        };
        std::vector<std::shared_ptr<Node>> nodes;

        void removeNode(Node *n);

        void processNode(const Node &n, std::ostream &out);


        void computeNonIsomorphisms(std::vector<VTuple> &out,
                std::vector<uint64_t> &tuple,
                std::vector<bool> &mask,
                int sizeLeft,
                uint64_t freshID);

        std::vector<VTuple> linearGetNonIsomorphicTuples(int arity);

        void linearGetAllNodesRootedAt(std::shared_ptr<Node> n,
                std::vector<std::shared_ptr<Node>> &out);

        void linearGetAllGraphNodes(
                std::vector<std::shared_ptr<Node>> &out);

        void linearChase(Program &program,
                Node *node, const std::vector<Literal> &db,
                std::unordered_set<std::string> &out);

        void linearRecursiveConstruction(Program &p, const Literal &l,
                std::shared_ptr<Node> parentNode);

    public:
        TriggerGraph();

        void createLinear(EDBLayer &db, Program &p);

        void createKBound(EDBLayer &db, Program &p);

        void saveAllPaths(std::ostream &out);
};

#endif
