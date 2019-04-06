#ifndef _GRAPH_H
#define _GRAPH_H

#include <cstdint>
#include <list>
#include <cstdlib>

class Graph
{
    private:
        size_t V; // No. of vertices
        std::list<uint64_t> *adj; // Pointer to an array containing adjacency lists
        bool isCyclicUtil(uint64_t v, bool visited[], bool *rs); // used by isCyclic()

    public:

        Graph(size_t V); // Constructor
        void addEdge(uint64_t v, uint64_t w); // to add an edge to graph
        bool isCyclic(); // returns true if there is a cycle in this graph
};

#endif
