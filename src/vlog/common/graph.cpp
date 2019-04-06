#include <vlog/graph.h>
#include <kognac/logs.h>

Graph::Graph(size_t V)
{
    this->V = V;
    adj = new std::list<uint64_t>[V];
}

void Graph::addEdge(uint64_t v, uint64_t w)
{
    LOG(TRACEL) << "Adding edge from " << v << " to " << w;
    adj[v].push_back(w); // Add w to v’s list.
}

// This function is a variation of DFSUytil() in https://www.geeksforgeeks.org/archives/18212
bool Graph::isCyclicUtil(uint64_t v, bool visited[], bool *recStack) {
    if(visited[v] == false) {
        // Mark the current node as visited and part of recursion stack
        visited[v] = true;
        recStack[v] = true;

        // Recur for all the vertices adjacent to this vertex
        std::list<uint64_t>::iterator i;
        for(i = adj[v].begin(); i != adj[v].end(); ++i) {
            if ( !visited[*i] && isCyclicUtil(*i, visited, recStack) )
                return true;
            else if (recStack[*i])
                return true;
        }
    }
    recStack[v] = false; // remove the vertex from recursion stack
    return false;
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
