#include <vlog/graph.h>
#include <kognac/logs.h>

Graph::Graph(size_t V)
{
    this->V = V;
    adj = new std::set<uint64_t>[V];
}

void Graph::addEdge(uint64_t v, uint64_t w)
{
    LOG(TRACEL) << "Adding edge from " << v << " to " << w;
    adj[v].insert(w); // Add w to v’s list.
}

// This function is a variation of DFSUytil() in https://www.geeksforgeeks.org/archives/18212
bool Graph::isCyclicUtil(uint64_t v, bool visited[], bool *recStack) {
    if(visited[v] == false) {
        // Mark the current node as visited and part of recursion stack
        visited[v] = true;
        recStack[v] = true;

        // Recur for all the vertices adjacent to this vertex
        for(auto i = adj[v].begin(); i != adj[v].end(); ++i) {
            if ( !visited[*i] && isCyclicUtil(*i, visited, recStack) )
                return true;
            else if (recStack[*i])
                return true;
        }
    }
    recStack[v] = false; // remove the vertex from recursion stack
    return false;
}

bool Graph::reachable(uint64_t destNode, uint64_t fromNode, bool *visited) {
    for(auto i = adj[fromNode].begin(); i != adj[fromNode].end(); ++i) {
        if (visited[*i]) {
            continue;
        }
        visited[*i] = true;
        if (*i == destNode) {
            return true;
        }
        if (reachable(destNode, *i, visited)) {
            return true;
        }
    }
    return false;
}

// Returns true if destNode can be reached from fromNode.
bool Graph::reachable(uint64_t destNode, uint64_t fromNode) {
    bool *visited = new bool[V];
    bool retval = reachable(destNode, fromNode, visited);
    delete[] visited;
    return retval;
}

// Returns true if the node is part of a cycle in the graph, else false;
bool Graph::isCyclicNode(uint64_t node) {
    bool *visited = new bool[V];
    bool *recStack = new bool[V];
    for(uint64_t i = 0; i < V; i++) {
        visited[i] = false;
        recStack[i] = false;
    }
    bool retval = false;
    if (isCyclicUtil(node, visited, recStack)) {
        retval = true;
    }

    delete[] visited;
    delete[] recStack;
    return retval;
}

// Returns true if the graph contains a cycle, else false.
// This function is a variation of DFS() in https://www.geeksforgeeks.org/archives/18212
bool Graph::isCyclic() {
    // Mark all the vertices as not visited and not part of recursion
    // stack
    bool *visited = new bool[V];
    bool *recStack = new bool[V];
    for(uint64_t i = 0; i < V; i++) {
        visited[i] = false;
        recStack[i] = false;
    }

    // Call the recursive helper function to detect cycle in different
    // DFS trees
    bool retval = false;
    for(uint64_t i = 0; i < V; i++) {
        if (isCyclicUtil(i, visited, recStack)) {
            retval = true;
            break;
        }
    }

    delete[] visited;
    delete[] recStack;
    return retval;
}

std::set<uint64_t> *Graph::getDestinations(uint64_t v) {
    if (v >= V) {
        return NULL;
    }
    return &adj[v];
}

Graph::~Graph() {
    delete[] adj;
}
