#ifndef _GRAPH_H
#define _GRAPH_H

#include <cstdint>
#include <set>
#include <cstdlib>

class Graph
{
    private:
        size_t V; // No. of vertices
        std::set<uint64_t> *adj; // Pointer to an array containing adjacency lists
        bool isCyclicUtil(uint64_t v, bool visited[], bool *rs); // used by isCyclic()
        bool reachable(uint64_t destNode, uint64_t fromNode, bool *visited);

    public:

        Graph(size_t V); // Constructor
        void addEdge(uint64_t v, uint64_t w); // to add an edge to graph
        void removeNodes(std::set<uint64_t> &v);    // completely removes the specified nodes from the graph.
        bool isCyclic(); // returns true if there is a cycle in this graph
        bool isCyclicNode(uint64_t node); // returns true if node is part of a cycle in this graph
        bool reachable(uint64_t destNode, uint64_t fromNode); // returns true if destNode is reachable from fromNode
        std::set<uint64_t> *getDestinations(uint64_t v);
		void getRecursiveDestinations(uint64_t v, std::set<uint64_t> &result);
        ~Graph();
};

#endif
