#ifndef _TG_H
#define _TG_H

#include <vlog/concepts.h>
#include <vlog/edb.h>

#include <string>
#include <vector>
#include <unordered_set>
#include <set>
#include <fstream>

class TriggerGraph {
    private:
        struct NodeFactSet {
            std::string id;
            std::vector<Literal> facts;
        };

        struct Node {
            private:
                uint64_t id;
            public:
                Node(uint64_t id) : id(id) {}
                int ruleID;
                int plotID;
                std::string label;
                std::vector<std::shared_ptr<Node>> incoming;
                std::vector<std::shared_ptr<Node>> outgoing;
                std::unique_ptr<Literal> literal;
                std::vector<NodeFactSet> facts;
                uint64_t getID() { return id; }
        };
        std::vector<std::shared_ptr<Node>> nodes;
        uint64_t nodecounter;

        void prune(EDBLayer &db, Program &program, std::vector<Literal> &database,
                std::shared_ptr<Node> u);

        void removeNode(std::shared_ptr<Node> n);

        void dumpGraphToFile(std::ofstream &edges, std::ofstream &nodes, EDBLayer &db, Program &p);

        void processNode(const Node &n, std::ostream &out);

        void linearAux2(std::shared_ptr<Node> u, std::shared_ptr<Node> v);

        void computeNonIsomorphisms(std::vector<VTuple> &out,
                std::vector<uint64_t> &tuple,
                std::vector<bool> &mask,
                int sizeLeft,
                uint64_t freshID);

        std::vector<VTuple> linearGetNonIsomorphicTuples(int arity);

        uint64_t getNNodes(Node *n = NULL, std::set<string> *cache = NULL);

        void linearGetAllNodesRootedAt(std::shared_ptr<Node> n,
                std::vector<std::shared_ptr<Node>> &out);

        void linearGetAllGraphNodes(
                std::vector<std::shared_ptr<Node>> &out);

        /*void linearComputeNodeOutput(Program &program,
          Node *node, const std::vector<Literal> &db,
          std::unordered_set<std::string> &out);*/

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
