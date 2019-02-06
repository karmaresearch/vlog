#include <vlog/trigger/tg.h>
#include <kognac/logs.h>

#include <inttypes.h>
#include <unordered_set>
#include <chrono>

TriggerGraph::TriggerGraph() {
}

void computeAllPermutations(std::vector<std::vector<int>> &out,
        std::vector<int> &in, int n, int k) {
    do {
        std::vector<int> perm;
        for (int i = 0; i < k; i++) {
            perm.push_back(in[i]);
        }
        out.push_back(perm);
        std::reverse(in.begin()+k,in.end());
    } while (std::next_permutation(in.begin(), in.end()));
}

void TriggerGraph::computeNonIsomorphisms(std::vector<VTuple> &out,
        std::vector<uint64_t> &tuple,
        std::vector<bool> &mask,
        int sizeLeft,
        uint64_t freshID) {

    if (sizeLeft == 0) {
        //The tuple has been fully created. Add it to out
        VTuple t(tuple.size());
        for(int i = 0; i < tuple.size(); ++i) {
            t.set(VTerm(0, tuple[i]), i);
        }
        out.push_back(t);
    } else {
        //I need to fill sizeLeft elements
        for(int positionsToCover = 1;
                positionsToCover <= sizeLeft;
                ++positionsToCover) {
            //Get all available indices
            std::vector<int> d;
            for(int i = 0; i < mask.size(); ++i) {
                if (mask[i])
                    d.push_back(i);
            }
            assert(d.size() == sizeLeft);

            std::vector<std::vector<int>> permutations;
            computeAllPermutations(permutations, d, sizeLeft, positionsToCover);

            for(const auto &perm : permutations) {
                std::vector<bool> newmask = mask;
                std::vector<uint64_t> newtuple = tuple;
                //Fill the data
                for(const auto &p : perm) {
                    newmask[p] = false;
                    newtuple[p] = freshID;
                }

                //Recursive call
                computeNonIsomorphisms(out,
                        newtuple,
                        newmask,
                        sizeLeft - positionsToCover,
                        freshID + 1);
            }
        }
    }
}

std::vector<VTuple> TriggerGraph::linearGetNonIsomorphicTuples(int start, int arity) {
    //Need to compute all non isomorphic tuples
    std::vector<VTuple> out;
    std::vector<uint64_t> tuple(arity);
    //std::vector<bool> mask(arity, true);
    //computeNonIsomorphisms(out, tuple, mask, arity, 0);
    if (arity == 1) {
        VTuple t(1);
        t.set(VTerm(0, start), 0);
        out.push_back(t);
    } else if (arity == 2) {
        VTuple t1(2);
        t1.set(VTerm(0, start), 0);
        t1.set(VTerm(0, start + 1), 1);
        out.push_back(t1);
        VTuple t2(2);
        t2.set(VTerm(0, start + 2), 0);
        t2.set(VTerm(0, start + 2), 1);
        out.push_back(t2);
    } else {
        LOG(ERRORL) << "arity not supported";
    }
    return out;
}

void TriggerGraph::linearChase(Program &program,
        Node *node, const std::vector<Literal> &db,
        std::unordered_set<std::string> &out) {

    std::string strPointer = std::to_string((uint64_t)&db);

    NodeFactSet localDerivations;
    localDerivations.id = strPointer;
    const std::vector<Literal> *outNode = NULL;

    //Search if I've already computed the chase before
    bool found = false;
    for(const auto &p : node->facts) {
        if (p.id == strPointer) {
            found = true;
            outNode = &(p.facts);
            break;
        }
    }

    if (!found) {
        const std::vector<Literal> *pointer = NULL;
        if (node->incoming.size() > 0) {
            std::unordered_set<std::string> lout;
            Node *parentNode = node->incoming[0].get();
            linearChase(program, parentNode, db, lout);
            bool found = false;
            for (const auto &set : parentNode->facts) {
                if (set.id == strPointer) {
                    pointer = &(set.facts);
                    found = true;
                    break;
                }
            }
            assert(found);
        } else {
            //Apply the rules on database
            pointer = &db;
        }

        //Apply the rule
        if (node->ruleID != -1) {
            const auto &rule = program.getRule(node->ruleID);
            const auto &body = rule.getBody();
            const auto &bodyAtom = body[0];
            const auto &heads = rule.getHeads();
            assert(heads.size() == 1);
            Literal head = heads[0];
            std::vector<Substitution> subs;
            for(const auto &inputLinear : *pointer) {
                int nsubs = Literal::getSubstitutionsA2B(subs, bodyAtom, inputLinear);
                if (nsubs != -1) {
                    Literal groundHead = head.substitutes(subs);
                    localDerivations.facts.push_back(groundHead);
                }
            }
            //Annotate the node with the dataset so that we do not recompute it
            node->facts.push_back(localDerivations);
            outNode = &(localDerivations.facts);
        } else {
            for(const auto &inputLinear : *pointer) {
                if (inputLinear.getPredicate().getId() == node->literal->getPredicate().getId()) {
                    localDerivations.facts.push_back(inputLinear);
                }
            }
            //Annotate the node with the dataset so that we do not recompute it
            node->facts.push_back(localDerivations);
            outNode = &(localDerivations.facts);
        }
    }

    out.clear();
    for (const auto &l : *outNode) {
        out.insert(l.tostring(NULL, NULL));
    }
}

/*void TriggerGraph::linearChase(Program &program,
  Node *node, const std::vector<Literal> &db,
  std::unordered_set<std::string> &out) {
  std::vector<Literal> outNode;
//Apply the rule on db and get all fact in outNode
const auto &rule = program.getRule(node->ruleID);
const auto &body = rule.getBody();
const auto &bodyAtom = body[0];
const auto &heads = rule.getHeads();
assert(heads.size() == 1);
Literal head = heads[0];
std::vector<Substitution> subs;

for(const auto &inputLinear : db) {
int nsubs = Literal::getSubstitutionsA2B(subs, bodyAtom, inputLinear);
if (nsubs != -1) {
Literal groundHead = head.substitutes(subs);
outNode.push_back(groundHead);
}
}

//Add these facts to out
for(const auto &l : outNode) {
out.insert(l.tostring(NULL, NULL));
}
//Recursive call
for(const auto &child : node->outgoing) {
linearChase(program, child.get(), outNode, out);
}
}*/


/*void TriggerGraph::linearComputeNodeOutput(Program &program,
  Node *node, const std::vector<Literal> &db,
  std::unordered_set<std::string> &out) {
//TODO
}*/

void TriggerGraph::linearRecursiveConstruction(Program &program,
        const Literal &literal,
        std::shared_ptr<Node> parentNode) {
    //If there is a rule with body compatible with the literal,
    //then create a node
    const auto &rules = program.getAllRules();
    std::vector<Substitution> subs;

    for(const auto &rule : rules) {
        const auto &body = rule.getBody();
        assert(body.size() == 1);
        const auto &bodyAtom = body[0];
        int nsubs = Literal::getSubstitutionsA2B(subs, bodyAtom, literal);
        if (nsubs != -1) {
            const auto &heads = rule.getHeads();
            assert(heads.size() == 1);
            Literal head = heads[0];
            Literal groundHead = head.substitutes(subs);
            bool found = false;
            assert(parentNode != NULL);

            //Check if the head exists in the previous nodes
            std::vector<Node*> toCheck;
            toCheck.push_back(parentNode.get());
            while (!toCheck.empty()) {
                Node *n = toCheck.back();
                toCheck.pop_back();
                std::vector<Substitution> s;
                int nsubs = Literal::subsumes(s, *n->literal.get(), groundHead);
                if (nsubs != -1) {
                    found = true;
                    break;
                }
                //Explore the incoming nodes
                for(const auto &child : n->incoming) {
                    toCheck.push_back(child.get());
                }
            }

            //Add a new node
            if (!found) {
                std::shared_ptr<Node> n = std::shared_ptr<Node>(new Node(nodecounter));
                nodecounter += 1;
                n->ruleID = rule.getId();
                assert(n->ruleID != -1);
                n->label = "node-" + std::to_string(n->getID());
                n->literal = std::unique_ptr<Literal>(new Literal(groundHead));
                n->incoming.push_back(parentNode);
                //Add the node to the parent
                parentNode->outgoing.push_back(n);
                //std::cout << "Node with rule " << rule.getId() << " and id " << n->getID() << " has parent " << parentNode->getID() << std::endl;

                //Recursive call
                linearRecursiveConstruction(program, groundHead, n);
            }
        } else {
            continue;
        }
    }
}

void TriggerGraph::linearGetAllNodesRootedAt(std::shared_ptr<Node> n,
        std::vector<std::shared_ptr<Node>> &out) {
    std::vector<std::shared_ptr<Node>> tovisit;
    tovisit.push_back(n);
    while (!tovisit.empty()) {
        auto n = tovisit.back();
        tovisit.pop_back();
        out.push_back(n);
        //Add all the children
        for(const auto &child : n->outgoing) {
            tovisit.push_back(child);
        }
    }
}

void TriggerGraph::linearGetAllGraphNodes(
        std::vector<std::shared_ptr<Node>> &out) {
    for(const auto &n : nodes) {
        linearGetAllNodesRootedAt(n, out);
    }
}

void TriggerGraph::dumpGraphToFile(std::ofstream &fedges, std::ofstream &fnodes,
        EDBLayer &edb, Program &p) {
    std::vector<std::shared_ptr<Node>> toprocess;
    for(const auto &n : nodes) {
        toprocess.push_back(n);
    }

    //Get all nodes
    std::vector<std::shared_ptr<Node>> allnodes;
    linearGetAllGraphNodes(allnodes);
    std::string header = "#plotID\tnodeID\truleID\tliteral\n";
    fnodes.write(header.c_str(), header.size());
    int id = 0;
    for (const auto &n : allnodes) {
        std::string line = std::to_string(id) + "\t" + std::to_string(n->getID()) + "\t" + std::to_string(n->ruleID) + "\t" + n->literal->toprettystring(&p, &edb, true);
        if (n->ruleID != -1) {
            const auto &rule = p.getRule(n->ruleID);
            line += "\t" + rule.toprettystring(&p, &edb, true);
        } else {
            line += "\tEDB";
        }
        line += "\n";
        n->plotID = id;
        id += 1;
        fnodes.write(line.c_str(), line.size());
    }

    while (!toprocess.empty()) {
        std::shared_ptr<Node> n = toprocess.back();
        toprocess.pop_back();
        //Print to file the edge list
        for (const auto &next : n->outgoing) {
            std::string line =  std::to_string(n->plotID) + " " + std::to_string(next->plotID) + "\n";
            fedges.write(line.c_str(), line.size());
            //Process the children
            toprocess.push_back(next);
        }
    }

}

uint64_t TriggerGraph::getNNodes(Node *n, std::set<string> *cache) {
    uint64_t out = 0;
    if (n == NULL) {
        out = nodes.size();
        std::set<string> cache;
        for (const auto &child : nodes) {
            out += getNNodes(child.get(), &cache);
        }
    } else {
        for (const auto &child : n->outgoing) {
            std::string sign = std::to_string(child->getID());
            if (!cache->count(sign)) {
                out += 1;
                cache->insert(sign);
            }
            out += getNNodes(child.get(), cache);
        }
    }
    return out;
}

void TriggerGraph::prune(EDBLayer &db, Program &program,
        std::vector<Literal> &database,
        std::shared_ptr<Node> u) {
    //First process the children of u
    auto children = u->outgoing;
    for (auto child : children) {
        prune(db, program, database, child);
    }

    bool toBeRemoved = false;

    //Then get the list of all nodes descendant from u
    std::vector<std::shared_ptr<Node>> childrenU;
    linearGetAllNodesRootedAt(u, childrenU);
    std::unordered_set<uint64_t> pointersChildrenU;
    for(const auto &cu : childrenU) {
        pointersChildrenU.insert((uint64_t)cu.get());
    }

    //Compute the set of all facts we can get to u
    std::unordered_set<std::string> dbToU;
    linearChase(program, u.get(), database, dbToU);

    std::vector<std::shared_ptr<Node>> allNodes;
    linearGetAllGraphNodes(allNodes);

    for(const auto v : allNodes) {
        if (v.get() == u.get()) {
            continue;
        }

        if (!pointersChildrenU.count((uint64_t)v.get())) {
            //Check whether v is redundant to u w.r.t. the database
            std::unordered_set<std::string> dbToV;
            linearChase(program, v.get(), database, dbToV);
            //If every fact in dbFromU is in dbFromV, then we can remove u
            bool subsumed = true;


            /*std::cout << "*** BEGIN ***" << std::endl;
              std::cout << "Checking u " << u->literal->toprettystring(&program, &db, true) << " vs. v " << v->literal->toprettystring(&program, &db, true) << std::endl;
              std::cout << "Rule u " << u->ruleID << " v " << v->ruleID << std::endl;
              std::cout << "DB_V " << std::endl;
              for (const auto &el : dbToV) {
              std::cout << "  " << el << std::endl;
              }
              std::cout << "DB_U " << std::endl;
              for (const auto &el : dbToU) {
              std::cout << "  " << el << std::endl;
              }*/
            for(const auto &f : dbToU) {
                if (!dbToV.count(f)) {
                    subsumed = false;
                    break;
                }
            }
            /*std::cout << "subsumed=" << subsumed << std::endl;
              std::cout << "*** END ***" << std::endl;*/

            //if subsumed = true, then u is redundant w.r.t. v
            if (subsumed) {
                //aux2 on v. This procedure will move away all good children of u
                linearAux2(u, v);
                toBeRemoved = true;
                break;
            }
        }
    }

    if (toBeRemoved) {
        removeNode(u);
    }

}

void TriggerGraph::createLinear(EDBLayer &db, Program &program) {
    //For each extensional predicate, create all non-isomorphic tuples and
    //chase over them
    std::vector<Literal> database;

#ifdef DEBUG
    std::ofstream fedges;
    fedges.open("graph.before.edges");
    std::ofstream fnodes;
    fnodes.open("graph.before.nodes");
#endif

    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();

    int startCounter = 0;
    for(const PredId_t p : db.getAllEDBPredicates()) {
        //Get arity
        int arity = db.getPredArity(p);
        const auto tuples = linearGetNonIsomorphicTuples(startCounter, arity);
        startCounter += 10; //number large enough to make sure there are no conflicts
        for (const VTuple &t : tuples) {
            std::shared_ptr<Node> n = std::shared_ptr<Node>(new Node(nodecounter));
            nodecounter += 1;
            n->ruleID = -1;
            n->label = "EDB-"+ std::to_string(p);
            Literal l(program.getPredicate(p), t);
            database.push_back(l);
            n->literal = std::unique_ptr<Literal>(new Literal(l));
            linearRecursiveConstruction(program, l, n);
            if (!n->outgoing.empty()) {
                nodes.push_back(n);
            }
        }
    }

    std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    LOG(INFOL) << "Time creating execution graph (msec): " << sec.count() * 1000;

    int allnodes = getNNodes();
    LOG(INFOL) << "Before pruning the graph contains nodes " << allnodes;

#ifdef DEBUG
    dumpGraphToFile(fedges, fnodes, db, program);
    fedges.close();
    fnodes.close();
#endif

#ifdef DEBUG
    fedges.open("graph.after.edges");
    fnodes.open("graph.after.nodes");
#endif

    //*** Prune the graph ***
    start = std::chrono::system_clock::now();
    auto nodesToProcess = nodes;
    for (auto n : nodesToProcess) {
        prune(db, program, database, n);
    }
    sec = std::chrono::system_clock::now() - start;
    LOG(INFOL) << "Time pruning (msec): " << sec.count() * 1000;

#ifdef DEBUG
    dumpGraphToFile(fedges, fnodes, db, program);
    fedges.close();
    fnodes.close();
#endif

    allnodes = getNNodes();
    LOG(INFOL) << "After pruning the graph contains nodes " << allnodes;
}

void TriggerGraph::linearAux2(std::shared_ptr<Node> u, std::shared_ptr<Node> v) {
    //TODO: Do not check for witness
    for(const auto &u_prime : u->outgoing) {
        //Move u_prime under v and remove it from u
        u_prime->incoming.clear();
        u_prime->incoming.push_back(v);
        v->outgoing.push_back(u_prime);
    }
    u->outgoing.clear();
}

void TriggerGraph::removeNode(std::shared_ptr<Node> n) {
    //First remove n from any parent
    for(const auto &parent : n->incoming) {
        int idx = 0;
        for(const auto &cp : parent->outgoing) {
            if (cp->getID() == n->getID()) {
                break;
            }
            idx++;
        }
        if (idx < parent->outgoing.size()) {
            parent->outgoing.erase(parent->outgoing.begin() + idx);
        }
    }

    //Remove it also from the root array
    int idx = 0;
    for(const auto &rootNode : nodes) {
        if (rootNode.get() == n.get()) {
            break;
        }
        idx++;
    }
    if (idx < nodes.size()) {
        nodes.erase(nodes.begin() + idx);
    }
}

void TriggerGraph::createKBound(EDBLayer &db, Program &p) {
    LOG(ERRORL) << "Not yet implemented";
}

void TriggerGraph::processNode(const Node &n, std::ostream &out) {
    for (const auto child : n.incoming) {
        processNode(*child, out);
    }
    //Write a line
    if (n.ruleID != -1) {
        out << std::to_string(n.ruleID) << "\t";
        for (const auto child : n.incoming) {
            out << child->label << "\t";
        }
        out << n.label << std::endl;
    }
}

void TriggerGraph::saveAllPaths(std::ostream &out) {
    std::vector<std::shared_ptr<Node>> nns;
    linearGetAllGraphNodes(nns);
    for(const auto &n : nns) {
        if (n->outgoing.size() == 0) {
            if (n->incoming.size() != 0) {
                processNode(*n.get(), out);
            }
        }
    }
}
