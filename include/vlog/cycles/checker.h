#ifndef _CHECKER_H
#define _CHECKER_H

#include <vlog/edb.h>

#include <string>
#include <list>

class Checker {
    public:
        static int check(std::string ruleFile, std::string alg, EDBLayer &db);

	static bool JA(Program &p, bool restricted);

	static bool MFA(Program &p, bool restricted);

	static void closure(Program &p, std::vector<std::pair<PredId_t, uint8_t>> &input);
};


class Graph
{
	int V; // No. of vertices
	list<int> *adj; // Pointer to an array containing adjacency lists
	bool isCyclicUtil(int v, bool visited[], bool *rs); // used by isCyclic()
public:
	Graph(int V); // Constructor
	void addEdge(int v, int w); // to add an edge to graph
	bool isCyclic(); // returns true if there is a cycle in this graph
};

#endif
