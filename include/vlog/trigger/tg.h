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

        struct CardSorter {
            EDBLayer &e;

            CardSorter(EDBLayer &edb) : e(edb) {}

            bool operator ()(const std::shared_ptr<TriggerGraph::Node> &a,
                    const std::shared_ptr<TriggerGraph::Node> &b) const;
        };



        std::vector<std::shared_ptr<Node>> nodes;
        uint64_t nodecounter;
        std::map<PredId_t, std::vector<size_t>> pred2bodyrules;
        std::map<uint64_t, std::shared_ptr<Node>> allnodes;
        size_t freshIndividualCounter;

        void remove(EDBLayer &db, Program &program,
                std::vector<Literal> &database,
                std::shared_ptr<Node> u,
                std::unordered_map<size_t,
                    std::vector<std::shared_ptr<Node>>> &headPred2nodes);

        void removeNode(std::shared_ptr<Node> n,
                std::unordered_map<size_t,
                    std::vector<std::shared_ptr<Node>>> &headPred2nodes);

        void dumpGraphToFile(std::ofstream &edges, std::ofstream &nodes, EDBLayer &db, Program &p);

        void processNode(const Node &n, std::ostream &out);

        void prune(Program &program,
                std::shared_ptr<Node> u,
                std::shared_ptr<Node> v,
                std::vector<Literal> &database);

        void computeNonIsomorphisms(std::vector<VTuple> &out,
                std::vector<uint64_t> &tuple,
                std::vector<bool> &mask,
                int sizeLeft,
                uint64_t freshID);

        std::vector<VTuple> linearGetNonIsomorphicTuples(int start = 0, int arity = 1);

        uint64_t getNNodes(Node *n = NULL, std::set<std::string> *cache = NULL);

        void applyRule(const Rule &r,
                std::vector<Literal> &out,
                const std::vector<Literal> &bodyAtoms);

        bool isWitness(Program &program,
                std::shared_ptr<Node> u,
                std::shared_ptr<Node> v,
                std::vector<Literal> &database);

        void linearGetAllNodesRootedAt(std::shared_ptr<Node> n,
                std::vector<std::shared_ptr<Node>> &out);

        void linearGetAllGraphNodes(
                std::vector<std::shared_ptr<Node>> &out);

        void linearChase(Program &program,
                Node *node, const std::vector<Literal> &db,
                std::unordered_set<std::string> &out);

        void linearChase(Program &program,
                Node *node, const std::vector<Literal> &db,
                std::vector<Literal> &out);

        void linearChase(Program &program,
                Node *node, const std::vector<Literal> &db,
                const std::vector<Literal> **out);

        void linearBuild_process(Program &program,
                std::vector<Rule> &rules,
                std::shared_ptr<Node> node,
                std::vector<Literal> &chase,
                std::vector<std::shared_ptr<Node>> &children);

        void linearBuild(Program &p,
                std::vector<Rule> &rules,
                const Literal &l,
                std::shared_ptr<Node> parentNode);

        void sortByCardinalities(
                std::vector<std::shared_ptr<Node>> &atoms,
                EDBLayer &edb);

    public:
        TriggerGraph();

        void createLinear(EDBLayer &db, Program &p);

        void createKBound(EDBLayer &db, Program &p);

        void saveAllPaths(EDBLayer &db, std::ostream &out);
};

#endif
