#include <vlog/trigger/tg.h>
#include <kognac/logs.h>

#include <inttypes.h>
#include <unordered_set>
#include <chrono>

TriggerGraph::TriggerGraph() {
    freshIndividualCounter = (uint64_t)1 << 32;
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
    const std::vector<Literal> *lit = NULL;
    linearChase(program, node, db, &lit);
    out.clear();
    for (const auto &l : *lit) {
        out.insert(l.tostring(NULL, NULL));
    }
}


void TriggerGraph::linearChase(Program &program,
        Node *node, const std::vector<Literal> &db,
        std::vector<Literal> &out) {
    out.clear();
    const std::vector<Literal> *lit = NULL;
    linearChase(program, node, db, &lit);
    for(auto &l : *lit) {
        out.push_back(l);
    }
}

void TriggerGraph::linearChase(Program &program,
        Node *node, const std::vector<Literal> &db,
        const std::vector<Literal> **out) {
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
            Node *parentNode = node->incoming[0].get();
            linearChase(program, parentNode, db, &pointer);
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
            outNode = &(node->facts.back().facts);
        } else {
            for(const auto &inputLinear : *pointer) {
                if (inputLinear.getPredicate().getId()
                        == node->literal->getPredicate().getId()) {
                    localDerivations.facts.push_back(inputLinear);
                }
            }
            //Annotate the node with the dataset so that we do not recompute it
            node->facts.push_back(localDerivations);
            outNode = &(node->facts.back().facts);
        }
    }

    *out = outNode;
}

void TriggerGraph::linearBuild_process(Program &program,
        std::vector<Rule> &rules,
        std::shared_ptr<Node> node,
        std::vector<Literal> &chase,
        std::vector<std::shared_ptr<Node>> &children) {

    //If there is a rule with body compatible with the literal,
    //then create a node
    std::vector<Substitution> subs;
    PredId_t litp = node->literal->getPredicate().getId();
    if (!pred2bodyrules.count(litp)) {
        return;
    }

    for(const auto &ruleidx : pred2bodyrules[litp]) {
        const auto &rule = rules[ruleidx];
        const auto &body = rule.getBody();
        assert(body.size() == 1);
        const auto &bodyAtom = body[0];
        const auto &heads = rule.getHeads();
        assert(heads.size() == 1);
        Literal head = heads[0];
        int nsubs = Literal::getSubstitutionsA2B(subs, bodyAtom,
                *(node->literal.get()));
        assert(nsubs != -1);

        std::unique_ptr<Literal> groundHead;
        groundHead = std::unique_ptr<Literal>(new Literal(head.substitutes(subs)));
        //Add existentially quantified IDs if necessary
        if (rule.isExistential()) {
            const auto &varsNotInBody = rule.getExistentialVariables();
            VTuple tuple = groundHead->getTuple();
            for(int i = 0; i < head.getTupleSize(); ++i) {
                const auto &t = head.getTermAtPos(i);
                if (t.isVariable()) {
                    bool found = false;
                    for(const auto &vnb : varsNotInBody) {
                        if (vnb == t.getId()) {
                            found = true;
                        }
                    }
                    if (found) {
                        //The var is existential
                        tuple.set(VTerm(0, freshIndividualCounter++), i);
                    }
                }
            }
            groundHead = std::unique_ptr<Literal>(
                    new Literal(groundHead->getPredicate(), tuple));
        }

        //Check if the head exists in the chase constructed so far
        bool found = false;
        for(const auto &existingFact : chase) {
            std::vector<Substitution> s;
            int nsubs = Literal::subsumes(s, existingFact, *groundHead.get());
            if (nsubs != -1) {
                found = true;
                break;
            }
        }

        //Add a new node
        if (!found) {
            //Add the head to the database (line 18)
            chase.push_back(Literal(*(groundHead.get())));

            //Withness check TODO

            //Add node to the graph (lines 20--24)
            std::shared_ptr<Node> n = std::shared_ptr<Node>(new Node(nodecounter));
            nodecounter += 1;
            n->ruleID = rule.getId();
            assert(n->ruleID != -1);
            n->label = "node-" + std::to_string(n->getID());
            n->literal = std::move(groundHead);
            n->incoming.push_back(node);
            //Add the node to the parent
            node->outgoing.push_back(n);
            //Add the new node to be processed
            children.push_back(n);
            //Add the node also to the list of all nodes
            allnodes.insert(std::make_pair(nodecounter - 1, n));
        }
    }
}

//LinearBuild corresponds to function "build"
void TriggerGraph::linearBuild(Program &program,
        std::vector<Rule> &rules,
        const Literal &literal,
        std::shared_ptr<Node> root) {

    std::vector<std::shared_ptr<Node>> newNodes;
    std::vector<Literal> chase; //All data from F
    newNodes.push_back(root);
    while (!newNodes.empty()) {
        std::vector<std::shared_ptr<Node>> nodesToProcess;
        nodesToProcess.swap(newNodes);
        for(auto n : nodesToProcess) {
            linearBuild_process(program, rules, n, chase, newNodes);
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

bool TriggerGraph::CardSorter::operator  ()(const std::shared_ptr<TriggerGraph::Node> &a,
        const std::shared_ptr<TriggerGraph::Node> &b) const {
    auto id_a = a->literal->getPredicate().getId();
    auto id_b = b->literal->getPredicate().getId();
    size_t card_a = e.getPredSize(id_a);
    size_t card_b = e.getPredSize(id_b);
    //std::cout << "pred " << id_a << " " << id_b << " card " << card_a << " " << card_b << std::endl;
    return card_a > card_b;
}

void TriggerGraph::sortByCardinalities(
        std::vector<std::shared_ptr<Node>> &atoms,
        EDBLayer &edb) {
    CardSorter c(edb);
    std::sort(atoms.begin(), atoms.end(), c);
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

uint64_t TriggerGraph::getNNodes(Node *n, std::set<std::string> *cache) {
    uint64_t out = 0;
    if (n == NULL) {
        out = nodes.size();
        std::set<std::string> cache;
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

void TriggerGraph::remove(EDBLayer &db, Program &program,
        std::vector<Literal> &database,
        std::shared_ptr<Node> u,
        std::unordered_map<size_t,
        std::vector<std::shared_ptr<Node>>> &headPred2nodes) {

    //First process the children of u
    auto children = u->outgoing;
    for (auto child : children) {
        remove(db, program, database, child, headPred2nodes);
    }

    //Then get the list of all nodes descendant from u
    std::vector<std::shared_ptr<Node>> childrenU;
    linearGetAllNodesRootedAt(u, childrenU);
    std::unordered_set<uint64_t> pointersChildrenU;
    for(const auto &cu : childrenU) {
        pointersChildrenU.insert((uint64_t)cu.get());
    }

    //Compute the set of all facts we can get from u
    const std::vector<Literal> *dbU = NULL;
    linearChase(program, u.get(), database, &dbU);

    bool toBeRemoved = false;
    auto headPred = u->literal->getPredicate().getId();
    if (headPred2nodes.count(headPred)) {
        auto &similarNodes = headPred2nodes[headPred];
        for(auto &v : similarNodes) {
            if (v.get() == u.get()) {
                continue;
            }

            if (!pointersChildrenU.count((uint64_t)v.get())) {
                //Check whether u is redundant to v w.r.t. the database
                const std::vector<Literal> *dbV = NULL;
                linearChase(program, v.get(), database, &dbV);

                //If every fact in dbU is in dbV, then we can remove u
                bool subsumed = true;
                for(const auto &s : *dbU) {
                    bool contained = false;
                    for(const auto &v : *dbV) {
                        if (s == v) {
                            contained = true;
                            break;
                        }
                    }
                    if (!contained) {
                        subsumed = false;
                        break;
                    }
                }

                //if subsumed = true, then u is redundant w.r.t. v
                if (subsumed) {
                    //aux2 on v. This procedure will move away all good children of u
                    prune(program, u, v, database);
                    toBeRemoved = true;
                    break;
                }
            }
        }
    }
    if (toBeRemoved) {
        removeNode(u, headPred2nodes);
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

    LOG(DEBUGL) << "Start creation of trigger graphs ...";

    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();

    //Create a map that associates predicates to rules
    auto rules = program.getAllRules();
    int idxp = 0;
    for (const auto &rule : rules) {
        const auto &body = rule.getBody();
        assert(body.size() == 1);
        const auto &bodyAtom = body[0];
        PredId_t pred = bodyAtom.getPredicate().getId();
        if (!pred2bodyrules.count(pred)) {
            pred2bodyrules.insert(make_pair(pred, std::vector<size_t>()));
        }
        pred2bodyrules[pred].push_back(idxp);
        idxp++;
    }

    //Create some fake EDB nodes from the canonical instance.
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

            //The following is the function build() lines 9--26
            linearBuild(program, rules, l, n);
            if (!n->outgoing.empty()) {
                nodes.push_back(n);
                allnodes.insert(std::make_pair(nodecounter - 1, n));
            }
        }
    }

    std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    LOG(INFOL) << "Time creating execution graph (msec): " << sec.count() * 1000;

    int n_allnodes = getNNodes();
    LOG(INFOL) << "Before pruning the graph contains nodes " << n_allnodes;

#ifdef DEBUG
    dumpGraphToFile(fedges, fnodes, db, program);
    fedges.close();
    fnodes.close();
#endif

#ifdef DEBUG
    fedges.open("graph.after.edges");
    fnodes.open("graph.after.nodes");
#endif

    //Group all the nodes by the head predicate
    std::unordered_map<size_t, std::vector<std::shared_ptr<Node>>> headPred2nodes;
    for(const auto pair : allnodes) {
        auto &v = pair.second;
        auto headPredicate = v->literal->getPredicate().getId();
        if (!headPred2nodes.count(headPredicate)) {
            headPred2nodes.insert(std::make_pair(headPredicate,
                        std::vector<std::shared_ptr<Node>>()));
        }
        headPred2nodes[headPredicate].push_back(v);
    }

    //*** Prune the graph ***
    start = std::chrono::system_clock::now();
    auto nodesToProcess = nodes;
    int idx = 0;
    for (auto n : nodesToProcess) {
        //start = std::chrono::system_clock::now();
        remove(db, program, database, n, headPred2nodes);
        //std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
        //LOG(INFOL) << "Time pruning " << idx << " of " << nodesToProcess.size() << " (msec): " << sec.count() * 1000;
        idx++;
    }
    sec = std::chrono::system_clock::now() - start;
    LOG(INFOL) << "Time pruning (msec): " << sec.count() * 1000;

#ifdef DEBUG
    dumpGraphToFile(fedges, fnodes, db, program);
    fedges.close();
    fnodes.close();
#endif

    n_allnodes = getNNodes();
    LOG(INFOL) << "After pruning the graph contains nodes " << n_allnodes;
}

void TriggerGraph::applyRule(const Rule &rule,
        std::vector<Literal> &out,
        const std::vector<Literal> &bodyAtoms) {

    const auto &body = rule.getBody();
    assert(body.size() == 1);
    const auto &bodyAtom = body[0];
    const auto &heads = rule.getHeads();
    assert(heads.size() == 1);
    Literal head = heads[0];

    std::vector<std::pair<uint8_t, uint8_t>> subs;
    std::vector<int> processedVars;
    for(int i = 0; i < bodyAtom.getTupleSize(); ++i) {
        VTerm term = bodyAtom.getTermAtPos(i);
        bool found = false;
        for(auto &var : processedVars) {
            if (term.getId() == var) {
                found = true;
                break;
            }
        }
        if (!found) {
            //Check where I should add it in the head
            for(int j = 0; j < head.getTupleSize(); ++j) {
                VTerm headTerm = head.getTermAtPos(j);
                if (headTerm.getId() == term.getId()) {
                    subs.push_back(std::make_pair(i, j));
                }
            }
            processedVars.push_back(term.getId());
        }
    }
    bool isExistential = rule.isExistential();
    std::vector<std::vector<uint8_t>> extpos;
    if (isExistential) {
        auto vars = rule.getExistentialVariables();
    }

    VTuple headTuple(head.getTupleSize());
    for(const Literal &bodyAtom : bodyAtoms) {
        VTuple t = bodyAtom.getTuple();
        for(auto &s : subs) {
            headTuple.set(bodyAtom.getTermAtPos(s.first), s.second);
        }
        for(auto &positions : extpos) {
            for(auto position : positions)
                t.set(VTerm(0, freshIndividualCounter), position);
            freshIndividualCounter++;
        }
        out.push_back(Literal(head.getPredicate(), headTuple));
    }
}

bool TriggerGraph::isWitness(Program &program,
        std::shared_ptr<Node> u,
        std::shared_ptr<Node> v,
        std::vector<Literal> &database) {
    //I apply rule rule(u) on v. If all conclusions are already in u,
    //then u is witness of v
    std::unordered_set<std::string> v_of_u;
    linearChase(program, u.get(), database, v_of_u);

    std::vector<Literal> v_of_v;
    linearChase(program, v.get(), database, v_of_v);

    //Apply the rule
    const auto &rule = program.getRule(u->ruleID);
    std::vector<Literal> ruleOutput;
    applyRule(rule, ruleOutput, v_of_v);

    //Check whether there is an homomorphism
    bool hom = true;
    for(const auto &l : ruleOutput) {
        std::string sl = l.tostring(NULL, NULL);
        if (!v_of_u.count(sl)) {
            hom = false;
            break;
        }
    }

    return hom;
}

void TriggerGraph::prune(Program &program,
        std::shared_ptr<Node> u, std::shared_ptr<Node> v,
        std::vector<Literal> &database) {
    int idx = 0;
    std::vector<int> childrenToRemove;
    for(const auto &u_prime : u->outgoing) {
        //Check whether u' is a witness of v
        if (isWitness(program, u_prime, v, database)) {
            //If there are no other witnesses among the children of v
            bool foundWitness = false;
            std::vector<std::shared_ptr<Node>> children_v;
            linearGetAllNodesRootedAt(v, children_v);
            for(auto &child_v : children_v) {
                if (child_v->ruleID == u_prime->ruleID &&
                        isWitness(program, child_v, v, database)) {
                    foundWitness = true;
                    break;
                }
            }
            if (!foundWitness) {
                //Move u_prime under v and remove it from u
                u_prime->incoming.clear();
                u_prime->incoming.push_back(v);
                v->outgoing.push_back(u_prime);
                childrenToRemove.push_back(idx);
            }
        }
        idx++;
    }
    for(int i = childrenToRemove.size() - 1; i >= 0; i--) {
        u->outgoing.erase(u->outgoing.begin() + i);
    }

    //Process the children of u and v
    std::vector<std::shared_ptr<Node>> children_v;
    linearGetAllNodesRootedAt(v, children_v);
    std::vector<std::shared_ptr<Node>> children_u;
    linearGetAllNodesRootedAt(v, children_u);
    for(auto &child_u : children_u) {
        if (child_u.get() != u.get()) {
            for (auto &child_v : children_v) {
                if (child_v.get() != v.get()) {
                    if (child_u->ruleID == child_v->ruleID && child_u.get() != child_v.get()) {
                        prune(program, child_u, child_v, database);
                    }
                }
            }
        }
    }

}

void TriggerGraph::removeNode(std::shared_ptr<Node> n,
        std::unordered_map<size_t,
        std::vector<std::shared_ptr<Node>>> &headPred2nodes) {
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
    auto el = allnodes.find(n->getID());
    assert(el != allnodes.end());
    allnodes.erase(el);
    //Remove the node also from the map
    auto headPred = el->second->literal->getPredicate().getId();
    if (headPred2nodes.count(headPred)) {
        auto &nodes = headPred2nodes[headPred];
        size_t i = 0;
        for(;i < nodes.size(); ++i) {
            if (nodes[i].get() == el->second.get())
                break;
        }
        if (i == nodes.size()) {
            LOG(ERRORL) << i << " should not happen!";
        }
        nodes.erase(nodes.begin() + i);
    }
}

void TriggerGraph::createKBound(EDBLayer &db, Program &p) {
    LOG(ERRORL) << "Not yet implemented";
}

void TriggerGraph::processNode(const Node &n, std::ostream &out) {
    //Write a line
    if (n.ruleID != -1) {
        out << std::to_string(n.ruleID) << "\t";
        for (const auto child : n.incoming) {
            out << child->label << "\t";
        }
        out << n.label << std::endl;
    }

    for (const auto child : n.outgoing) {
        processNode(*child, out);
    }
}

void TriggerGraph::saveAllPaths(EDBLayer &edb, std::ostream &out) {
    LOG(INFOL) << "Saving all paths ...";
    std::vector<std::shared_ptr<Node>> nns;

    std::vector<std::shared_ptr<Node>> toprocess;
    for(const auto &n : nodes) {
        toprocess.push_back(n);
    }
    //Sort the nodes by cardinalities
    sortByCardinalities(toprocess, edb);

    for(const auto &n : toprocess) {
        if (n->outgoing.size() != 0) {
            processNode(*n.get(), out);
        }
    }
}
