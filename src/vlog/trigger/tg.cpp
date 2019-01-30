#include <vlog/trigger/tg.h>
#include <kognac/logs.h>

#include <inttypes.h>
#include <unordered_set>

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

std::vector<VTuple> TriggerGraph::linearGetNonIsomorphicTuples(int arity) {
    //Need to compute all non isomorphic tuples
    std::vector<VTuple> out;
    std::vector<uint64_t> tuple(arity);
    std::vector<bool> mask(arity, true);
    computeNonIsomorphisms(out, tuple, mask, arity, 0);
    return out;
}

void TriggerGraph::linearChase(Program &program,
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
}

void TriggerGraph::linearRecursiveConstruction(Program &program,
        const Literal &literal,
        std::shared_ptr<Node> parentNode) {
    //If there is a rule with body compatible with the literal,
    //then create a node
    const auto &rules = program.getAllRules();
    std::vector<Substitution> subs;
    long counter = 0;

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
                std::shared_ptr<Node> n = std::shared_ptr<Node>(new Node());
                n->ruleID = rule.getId();
                n->label = "node-" + std::to_string(counter++);
                n->literal = std::unique_ptr<Literal>(new Literal(groundHead));
                n->incoming.push_back(parentNode);
                //Add the node to the parent
                parentNode->outgoing.push_back(n);

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

void TriggerGraph::createLinear(EDBLayer &db, Program &program) {
    //For each extensional predicate, create all non-isomorphic tuples and
    //chase over them
    std::vector<Literal> database;

    for(const PredId_t p : db.getAllEDBPredicates()) {
        //Get arity
        int arity = db.getPredArity(p);
        const auto &tuples = linearGetNonIsomorphicTuples(arity);
        for (const VTuple &t : tuples) {
            std::shared_ptr<Node> n = std::shared_ptr<Node>(new Node());
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

    //*** Prune the graph ***
    std::vector<std::shared_ptr<Node>> allNodes;
    linearGetAllGraphNodes(allNodes);
    for(const auto u : allNodes) {
        //First get the list of all nodes descendant from u
        std::vector<std::shared_ptr<Node>> childrenU;
        linearGetAllNodesRootedAt(u, childrenU);
        std::unordered_set<uint64_t> pointersChildrenU;
        for(const auto &cu : childrenU) {
            pointersChildrenU.insert((uint64_t)cu.get());
        }

        //Compute the set of all facts we can get from u
        std::unordered_set<std::string> dbFromU;
        linearChase(program, u.get(), database, dbFromU);

        for(const auto v : allNodes) {
            if (v.get() == u.get()) {
                continue;
            }
            if (!pointersChildrenU.count((uint64_t)v.get())) {
                //Check whether v is redundant to u w.r.t. the database
                std::unordered_set<std::string> dbFromV;
                linearChase(program, u.get(), database, dbFromU);
                //If every fact in dbFromV is in dbFromU, then we remove v
                bool subsumed = true;
                for(const auto &f : dbFromV) {
                    if (!dbFromU.count(f)) {
                        subsumed = false;
                        break;
                    }
                }
                if (!subsumed) {
                    //TODO: implement aux2 on v

                    //Remove any v and descendant that is not in u
                    std::vector<std::shared_ptr<Node>> childrenU2;
                    linearGetAllNodesRootedAt(u, childrenU2);
                    std::unordered_set<uint64_t> pointersChildrenU2;
                    for(const auto &cu : childrenU2) {
                        pointersChildrenU2.insert((uint64_t)cu.get());
                    }

                    std::vector<std::shared_ptr<Node>> childrenV;
                    linearGetAllNodesRootedAt(v, childrenV);
                    for(const auto &childV : childrenV) {
                        removeNode(childV.get());
                    }
                }
            }
        }
    }
}

void TriggerGraph::removeNode(Node *n) {
    //First remove n from any parent
    for(const auto &parent : n->incoming) {
        int idx = 0;
        for(const auto &cp : parent->outgoing) {
            if (cp.get() == parent.get()) {
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
        if (rootNode.get() == n) {
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
    out << std::to_string(n.ruleID) << "\t";
    for (const auto child : n.incoming) {
        out << child->label << "\t";
    }
    out << n.label << std::endl;
}

void TriggerGraph::saveAllPaths(std::ostream &out) {
    for(const auto &n : nodes) {
        if (n->outgoing.size() == 0) {
            if (n->incoming.size() != 0) {
                processNode(*n.get(), out);
            }
        }
    }
}
