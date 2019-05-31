#include <vlog/concepts.h>
#include <vlog/graph.h>
#include <vlog/optimizer.h>
#include <vlog/edb.h>
#include <vlog/fcinttable.h>

#include <kognac/consts.h>

#include <cctype>
#include <set>
#include <stdlib.h>
#include <fstream>

using namespace std;

bool Literal::hasRepeatedVars() const {
    std::vector<uint8_t> exVar;
    for (int i = 0; i < tuple.getSize(); ++i) {
        if (tuple.get(i).isVariable()) {
            for (std::vector<uint8_t>::iterator itr = exVar.begin(); itr != exVar.end();
                    ++itr) {
                if (*itr == tuple.get(i).getId()) {
                    return true;
                }
            }
        }
    }
    return false;
}

uint8_t Literal::getNVars() const {
    uint8_t n = 0;
    for (int i = 0; i < tuple.getSize(); ++i) {
        if (tuple.get(i).isVariable())
            n++;
    }
    return n;
}

uint8_t Literal::getNConstants() const {
    return getTupleSize() - getNVars();
}

std::vector<uint8_t> Literal::getPosVars() const {
    std::vector<uint8_t> out;
    for (uint8_t i = 0; i < tuple.getSize(); ++i) {
        if (tuple.get(i).isVariable())
            out.push_back(i);
    }
    return out;
}

uint8_t Literal::getNUniqueVars() const {
    std::vector<uint8_t> exVar;
    uint8_t n = 0;
    for (int i = 0; i < tuple.getSize(); ++i) {
        if (tuple.get(i).isVariable()) {
            bool found = false;
            for (std::vector<uint8_t>::iterator itr = exVar.begin(); itr != exVar.end();
                    ++itr) {
                if (*itr == tuple.get(i).getId()) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                n++;
                exVar.push_back(tuple.get(i).getId());
            }
        }
    }
    return n;
}

std::string adornmentToString(uint8_t adornment, int size) {
    std::string out = "";
    for (int i = 0; i < size; ++i) {
        if (adornment & 1) {
            out = out + std::string("b");
        } else {
            out = out + std::string("f");
        }
        adornment >>= 1;
    }
    return out;
}

std::string Literal::tostring() const {
    return tostring(NULL, NULL);
}

std::string Literal::tostring(Program *program, EDBLayer *db) const {

    std::string predName;
    if (program != NULL)
        predName = program->getPredicateName(pred.getId());
    else
        predName = std::to_string(pred.getId());
    if (isNegated()) {
        predName = "neg_" + predName;
    }

    std::string out = predName + std::string("[") +
        std::to_string(pred.getType()) + std::string("]") +
        adornmentToString(pred.getAdorment(), tuple.getSize()) + std::string("(");

    for (int i = 0; i < tuple.getSize(); ++i) {
        if (tuple.get(i).isVariable()) {
            out += std::string("?") + std::to_string(tuple.get(i).getId());
        } else {
            if (db == NULL) {
                out += std::to_string(tuple.get(i).getValue());
            } else {
                uint64_t id = tuple.get(i).getValue();
                char text[MAX_TERM_SIZE];
                if (db->getDictText(id, text)) {
                    out += Program::compressRDFOWLConstants(std::string(text));
                } else {
                    if (program == NULL) {
                        out += std::to_string(id);
                    } else {
                        std::string t = db->getDictText(id);
                        if (t == std::string("")) {
                            out += std::to_string(id);
                        } else {
                            out += Program::compressRDFOWLConstants(t);
                        }
                    }
                }
            }
        }
        if (i < tuple.getSize() - 1) {
            out += std::string(",");
        }
    }

    out += std::string(")");
    return out;
}

std::string Literal::toprettystring(Program *program, EDBLayer *db, bool replaceConstants) const {

    std::string predName;
    if (program != NULL)
        predName = program->getPredicateName(pred.getId());
    else
        predName = std::to_string(pred.getId());

    std::string out = "";
    if (isNegated())
        out = "~";
    out += predName + std::string("(");

    for (int i = 0; i < tuple.getSize(); ++i) {
        if (tuple.get(i).isVariable()) {
            out += std::string("A") + std::to_string(tuple.get(i).getId());
        } else {
	    if (replaceConstants) {
		out += "*";
	    } else if (db == NULL) {
                out += std::to_string(tuple.get(i).getValue());
            } else {
                uint64_t id = tuple.get(i).getValue();
                char text[MAX_TERM_SIZE];
                if (db->getDictText(id, text)) {
                    string v = Program::compressRDFOWLConstants(std::string(text));
                    out += v;
                } else {
                    if (program == NULL) {
                        out += std::to_string(id);
                    } else {
                        std::string t = db->getDictText(id);
                        if (t == std::string("")) {
                            out += std::to_string(id);
                        } else {
                            out += Program::compressRDFOWLConstants(t);
                        }
                    }
                }
            }
        }
        if (i < tuple.getSize() - 1) {
            out += std::string(",");
        }
    }

    out += std::string(")");
    return out;
}



std::vector<std::pair<uint8_t, uint8_t>> Literal::getRepeatedVars() const {
    return tuple.getRepeatedVars();
}

int Literal::mgu(Substitution *substitutions, const Literal &l, const Literal &m) {
    if (l.getPredicate().getId() != l.getPredicate().getId()) {
        return -1;
    }

    int tupleSize = 0;
    for (int i = 0; i < l.getTupleSize(); ++i) {
        VTerm tl = l.getTermAtPos(i);
        VTerm tm = m.getTermAtPos(i);
        if (!tl.isVariable() && !tm.isVariable() && tl.getValue() != tm.getValue())
            return -1;

        if (tl.isVariable()) {
            bool found = false;
            for (int j = 0; j < tupleSize && !found; ++j) {
                if (substitutions[j].origin == tl.getId()) {
                    found = true;
                    if (substitutions[j].destination != tm) {
                        return -1;
                    }
                }
            }

            if (!found)
                substitutions[tupleSize++] = Substitution(tl.getId(), tm);
        } else if (tm.isVariable()) {
            bool found = false;
            for (int j = 0; j < tupleSize && !found; ++j) {
                if (substitutions[j].origin == tm.getId()) {
                    found = true;
                    if (substitutions[j].destination != tl) {
                        return -1;
                    }
                }
            }
            if (!found)
                substitutions[tupleSize++] = Substitution(tm.getId(), tl);
        }
    }
    return tupleSize;
}

bool Literal::sameVarSequenceAs(const Literal &l) const {
    if (getNVars() == l.getNVars()) {
        std::vector<uint8_t> v1 = getAllVars();
        std::vector<uint8_t> v2 = l.getAllVars();
        if (v1.size() != v2.size()) {
            // Apparently, one has duplicates
            return false;
        }
        for (uint8_t i = 0; i < v1.size(); ++i) {
            if (v1[i] != v2[i])
                return false;
        }
        return true;
    }
    return false;
}

int Literal::subsumes(std::vector<Substitution> &substitutions, const Literal &l, const Literal &m) {
    substitutions.clear();
    if (l.getPredicate().getId() != m.getPredicate().getId()) {
        return -1;
    }
    assert (l.getTupleSize() == m.getTupleSize());

    for (int i = 0; i < l.getTupleSize(); ++i) {
        VTerm tl = l.getTermAtPos(i);
        VTerm tm = m.getTermAtPos(i);
        if (!tl.isVariable() && !tm.isVariable() && tl.getValue() != tm.getValue())
            return -1;

        if (tl.isVariable()) {
            bool found = false;
            for (int j = 0; j < substitutions.size() && !found; ++j) {
                if (substitutions[j].origin == tl.getId()) {
                    found = true;
                    if (substitutions[j].destination != tm) {
                        return -1;
                    }
                }
            }
            if (!found) {
                substitutions.push_back(Substitution(tl.getId(), tm));
            }
        } else {
            if (tm.isVariable())
                return -1;
        }
    }
    return substitutions.size();
}

int Literal::getSubstitutionsA2B(std::vector<Substitution> &substitutions,
        const Literal &a, const Literal &b) {
    substitutions.clear();
    if (a.getPredicate().getId() != b.getPredicate().getId()) {
        return -1;
    }
    assert(a.getTupleSize() == b.getTupleSize());

    for (int i = 0; i < a.getTupleSize(); ++i) {
        VTerm ta = a.getTermAtPos(i);
        VTerm tb = b.getTermAtPos(i);

        if (!ta.isVariable() && !tb.isVariable() &&
                ta.getValue() != tb.getValue())
            return -1;

        if (ta.isVariable()) {
            bool found = false;
            //Is there already a substitution?
            for (int j = 0; j < substitutions.size() && !found; ++j) {
                if (substitutions[j].origin == ta.getId()) {
                    found = true;
                    if (substitutions[j].destination != tb) {
                        if (!tb.isVariable()) {
                            if (!substitutions[j].destination.isVariable()) {
                                assert(substitutions[j].destination.getValue() != tb.getValue());
                                return -1;
                            } else {
                                //replace the value with tb
                                substitutions[j].destination = tb;
                                found = false;
                            }
                        }
                    }
                }
            }
            if (!found)
                substitutions.push_back(Substitution(ta.getId(), tb));
        } else {
            //if (tm.isVariable())
            //    return -1;
        }
    }
    return substitutions.size();
}

Literal Literal::substitutes(std::vector<Substitution> &subs) const {
    VTuple newTuple((uint8_t) this->tuple.getSize());
    for (uint8_t i = 0; i < newTuple.getSize(); ++i) {
        bool found = false;
        int j = 0;
        while (j < subs.size() && !found) {
            if (subs[j].origin == tuple.get(i).getId()) {
                found = true;
                break;
            }
            j++;
        }
        if (found) {
            newTuple.set(subs[j].destination, i);
        } else {
            newTuple.set(tuple.get(i), i);
        }
    }
    return Literal(pred, newTuple);
}

std::vector<uint8_t> Literal::getSharedVars(const std::vector<uint8_t> &vars) const {
    std::vector<uint8_t> output;
    for (int i = 0; i < getTupleSize(); ++i) {
        VTerm t = getTermAtPos(i);
        if (t.isVariable()) {
            for (std::vector<uint8_t>::const_iterator itr = vars.cbegin(); itr != vars.cend();
                    ++itr) {
                if (t.getId() == *itr) {
                    //Check is not already in output
                    bool found = false;
                    for (std::vector<uint8_t>::iterator itr2 = output.begin(); itr2 != output.end(); ++itr2) {
                        if (*itr == *itr2) {
                            found = true;
                            break;
                        }
                    }
                    if (!found)
                        output.push_back(t.getId());
                }
            }
        }
    }
    return output;
}

std::vector<uint8_t> Literal::getNewVars(std::vector<uint8_t> &vars) const {
    std::vector<uint8_t> output;
    for (int i = 0; i < getTupleSize(); ++i) {
        VTerm t = getTermAtPos(i);
        if (t.isVariable()) {
            bool found = false;
            for (std::vector<uint8_t>::iterator itr = vars.begin(); itr != vars.end();
                    ++itr) {
                if (t.getId() == *itr) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                //Check is not in output
                for (std::vector<uint8_t>::iterator itr = output.begin(); itr != output.end();
                        ++itr) {
                    if (*itr == t.getId()) {
                        found = true;
                        break;
                    }
                }
            }

            if (!found) {
                output.push_back(t.getId());
            }
        }
    }
    return output;

}

std::vector<uint8_t> Literal::getAllVars() const {
    std::vector<uint8_t> output;
    for (int i = 0; i < getTupleSize(); ++i) {
        VTerm t = getTermAtPos(i);
        if (t.isVariable()) {
            bool found = false;
            for (std::vector<uint8_t>::iterator itr = output.begin(); itr != output.end();
                    ++itr) {
                if (*itr == t.getId()) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                output.push_back(t.getId());
            }
        }
    }
    return output;
}

bool Literal::operator==(const Literal & other) const {
    if (pred.getId() != other.pred.getId())
        return false;
    for (uint8_t i = 0; i < tuple.getSize(); ++i) {
        if (tuple.get(i) != other.tuple.get(i))
            return false;
    }
    if (negated != other.isNegated())
        return false;
    return true;
}

bool Rule::checkRecursion(const std::vector<Literal> &heads,
        const std::vector<Literal> &body) {
    for (const auto bodyLit : body) {
        std::vector<Substitution> subs;
        for(const auto& head : heads) {
            if (Literal::getSubstitutionsA2B(subs, bodyLit, head) != -1)
                return true;
        }

    }
    return false;
}

void Rule::checkRule() const {
    bool vars[256];
    memset(vars, 0, sizeof(bool) * 256);
    int varCount = 0;
    for (int i = 0; i < body.size(); ++i) {
        Literal l = body[i];
        std::vector<uint8_t> v = l.getAllVars();
        for (int j = 0; j < v.size(); j++) {
            if (! vars[v[j]]) {
                vars[v[j]] = true;
                varCount++;
            }
        }
    }
    LOG(DEBUGL) << "Rule " << this->tostring() << " has " << varCount << " variables";
}

bool Rule::doesVarAppearsInFollowingPatterns(int startingPattern, uint8_t value) const {
    for (int i = startingPattern; i < body.size(); ++i) {
        Literal l = body[i];
        for (int j = 0; j < l.getTupleSize(); ++j) {
            if (l.getTermAtPos(j).isVariable() && l.getTermAtPos(j).getId() == value) {
                return true;
            }
        }
    }
    for(const auto& head : heads) {
        for (int i = 0; i < head.getTupleSize(); ++i) {
            if (head.getTermAtPos(i).isVariable() && head.getTermAtPos(i).getId() == value) {
                return true;
            }
        }
    }
    return false;
}

Rule Rule::normalizeVars() const {
    std::set<uint8_t> s_vars;
    for(auto &h : heads) {
        for(auto &v : h.getAllVars()) {
            s_vars.insert(v);
        }
    }
    std::vector<uint8_t> vars; //Converts in a vector
    for(auto v : s_vars) vars.push_back(v);
    for (std::vector<Literal>::const_iterator itr = body.cbegin(); itr != body.cend();
            ++itr) {
        std::vector<uint8_t> newvars = itr->getNewVars(vars);
        for (int i = 0; i < newvars.size(); i++) {
            vars.push_back(newvars[i]);
        }
    }
    std::vector<Substitution> subs;
    for (int i = 0; i < vars.size(); i++) {
        subs.push_back(Substitution(vars[i], VTerm(i+1, 0)));
    }
    std::vector<Literal> newheads;
    for(auto &head : heads) {
        Literal newHead = head.substitutes(subs);
        LOG(DEBUGL) << "head = " << head.tostring() << ", newHead = " << newHead.tostring();
        newheads.push_back(newHead);
    }
    std::vector<Literal> newBody;
    for (std::vector<Literal>::const_iterator itr = body.cbegin(); itr != body.cend();
            ++itr) {
        newBody.push_back(itr->substitutes(subs));
    }
    return Rule(ruleId, newheads, newBody);
}

Rule Rule::createAdornment(uint8_t headAdornment) const {
    if (heads.size() > 1) {
        LOG(ERRORL) << "Function createAdornment is defined only for rules with a single head";
        throw 10;
    }

    Literal head = heads[0];
    //Assign the adornment to the head
    Literal newHead(Predicate(head.getPredicate(), headAdornment), head.getTuple());
    std::vector<uint8_t> boundVars;
    for (int i = 0; i < head.getTupleSize(); ++i) {
        if (headAdornment >> i & 1) {
            VTerm t = head.getTermAtPos(i);
            if (t.isVariable()) {
                int prevOccurrence = -1;
                for (int j = 0; j < boundVars.size(); ++j) {
                    if (boundVars[j] == t.getId()) {
                        prevOccurrence = j;
                        break;
                    }
                }
                if (prevOccurrence == -1) {
                    boundVars.push_back(t.getId());
                }
            }
        }
    }

    std::vector<const Literal*> rearragendBody =
        Optimizer::rearrangeBodyAfterAdornment(boundVars, body);

    //Calculate the adornments for the body patterns
    std::vector<Literal> newBody;
    for (int j = 0; j < body.size(); ++j) {
        const Literal *literal = rearragendBody.at(j);
        uint8_t adornment = 0;
        std::vector<uint8_t> uniqueVars;
        for (int i = 0; i < literal->getTupleSize(); ++i) {
            if (literal->getTermAtPos(i).isVariable()) {
                bool found = false;
                for (std::vector<uint8_t>::iterator itr = boundVars.begin();
                        itr != boundVars.end(); ++itr) {
                    if (*itr == literal->getTermAtPos(i).getId()) {
                        found = true;
                        break;
                    }
                }
                if (found) {
                    adornment |= 1 << i;
                } else {
                    bool isUnique = true;
                    for (std::vector<uint8_t>::iterator itr = uniqueVars.begin();
                            itr != uniqueVars.end(); ++itr) {
                        if (*itr == literal->getTermAtPos(i).getId()) {
                            isUnique = false;
                            break;
                        }
                    }
                    if (isUnique)
                        uniqueVars.push_back(literal->getTermAtPos(i).getId());
                }
            }   else {
                adornment |= 1 << i;
            }
        }
        boundVars.insert(boundVars.end(), uniqueVars.begin(), uniqueVars.end());
        newBody.push_back(Literal(Predicate(literal->getPredicate(), adornment),
                    literal->getTuple()));
    }
    std::vector<Literal> newHeads;
    newHeads.push_back(newHead);
    return Rule(ruleId, newHeads, newBody);
}

std::string Rule::tostring() const {
    return tostring(NULL, NULL);
}

std::vector<uint8_t> Rule::getVarsNotInBody() const {
    //Check if every variable in the head appears in the body
    std::vector<uint8_t> out;
    for(const auto& head : heads) {
        for(auto var : head.getAllVars()) {
            //Does var appear in the body?
            bool ok = false;
            for(auto& bodyLit : body) {
                auto allvars = bodyLit.getAllVars();
                for(auto v : allvars) {
                    if (v == var) {
                        ok = true; //found it!
                        break;
                    }
                }
                if (ok)
                    break;
            }
            if (!ok) {
                for (auto v : out) {
                    if (v == var) {
                        ok = true;
                        break;
                    }
                }
                if (! ok) {
                    out.push_back(var);
                }
            }
        }
    }
    return out;
}

std::vector<uint8_t> Rule::getVarsInBody() const {
    //Check if every variable in the head appears in the body
    std::vector<uint8_t> out;
    for(const auto& head : heads) {
        for(auto var : head.getAllVars()) {
            //Does var appear in the body?
            bool ok = false;
            for(auto& bodyLit : body) {
                auto allvars = bodyLit.getAllVars();
                for(auto v : allvars) {
                    if (v == var) {
                        ok = true; //found it!
                        break;
                    }
                }
                if (ok)
                    break;
            }
            if (ok)
                out.push_back(var);
        }
    }
    return out;
}

bool Rule::isExistential() const {
    return existential;
}

std::string Rule::tostring(Program * program, EDBLayer *db) const {
    std::string output = std::string("HEAD=");
    for(const auto &head : heads) {
        output += " " + head.tostring(program, db) ;
    }
    output += std::string(" BODY= ");
    for (int i = 0; i < body.size(); ++i) {
        output += body[i].tostring(program, db) + std::string(" ");
    }
    return output;
}

std::string Rule::toprettystring(Program * program, EDBLayer *db, bool replaceConstants) const {
    std::string output = "";
    bool first = true;
    for(const auto& head : heads) {
	if (! first) {
	    output += ",";
	}
        output += head.toprettystring(program, db, replaceConstants);
	first = false;
    }
    output += " :- ";
    first = true;
    for (int i = 0; i < body.size(); ++i) {
	if (! first) {
	    output += ",";
	}
	first = false;
        output += body[i].toprettystring(program, db, replaceConstants);
    }
    return output;
}

bool Program::stratify(std::vector<int> &stratification, int &nClasses) {
    // First check if the rules can be stratified.
    std::set<std::pair<uint64_t, uint64_t>> usedNegated;
    uint64_t graphSize = getMaxPredicateId() + 1;
    Graph g(graphSize);
    Graph depends(graphSize);
    for (const Rule rule : allrules) {
        for (const auto &head : rule.getHeads()) {
            for (const auto &bodyLiteral : rule.getBody()) {
                g.addEdge(bodyLiteral.getPredicate().getId(), head.getPredicate().getId());
                depends.addEdge(head.getPredicate().getId(), bodyLiteral.getPredicate().getId());
                if (bodyLiteral.isNegated() && bodyLiteral.getPredicate().getType() == IDB) {
                    usedNegated.insert(std::make_pair(bodyLiteral.getPredicate().getId(), head.getPredicate().getId()));
                    LOG(DEBUGL) << "Adding usedNegated: " << bodyLiteral.getPredicate().getId() << ", " << head.getPredicate().getId();
                }
            }
        }
    }

    Graph negationsClosure(graphSize);

    // Create a negationsClosure graph, one that has an edge from p to q iff there is a path from p to q in the dependency graph, containing a negative edge.
    for (auto it = usedNegated.begin(); it != usedNegated.end(); ++it) {
        // usedNegated contains all negative edges.
        std::set<uint64_t> deps;
        std::set<uint64_t> reaches;
        depends.getRecursiveDestinations(it->first, deps);
        g.getRecursiveDestinations(it->second, reaches);
        // We know we have a negative edge from first to second.
        // Now, if we can reach first from second, we have a cycle
        // with a negative edge --> cannot be stratified.
        if (reaches.find(it->first) != reaches.end()) {
            return false;
        }
        for(auto it1 : reaches) {
            for (auto it2 : deps) {
                negationsClosure.addEdge(it1, it2);
            }
        }
    }

    stratification.resize(graphSize);
    for (size_t i = 0; i < graphSize; i++) {
        stratification[i] = -1;
    }

    
    int markedCount = 0;
    int count = 0;
    while (count < graphSize) {
        std::set<uint64_t> toRemove;
        for (int i = 0; i < graphSize; i++) {
            if (stratification[i] < 0 && negationsClosure.getDestinations(i)->empty()) {
                LOG(DEBUGL) << "Marking " << i << " with " << markedCount;
                stratification[i] = markedCount;
                count++;
                toRemove.insert(i);
            }
        }
        negationsClosure.removeNodes(toRemove);
        markedCount++;
    }

    nClasses = markedCount;
    return true;
}

bool Program::areExistentialRules() {
    for(auto& rule : allrules) {
        if (rule.isExistential()) {
            return true;
        }
    }
    return false;
}

bool Program::doesPredicateExist(const PredId_t id) const {
    return cardPredicates.count(id);
}

const Rule &Program::getRule(uint32_t ruleid) const {
    return allrules[ruleid];
}

std::vector<PredId_t> Program::getAllPredicateIDs() const {
    std::vector<PredId_t> result;
    for (auto iter: cardPredicates) {
        result.push_back(iter.first);
    }
    return result;
}

size_t Program::getNRulesByPredicate(PredId_t predid) const {
    return rules[predid].size();
}

const std::vector<uint32_t> &Program::getRulesIDsByPredicate(PredId_t predid) const {
    return rules[predid];
}

Program::Program(EDBLayer *kb) : kb(kb),
    dictPredicates(kb->getPredDictionary()),
    cardPredicates(kb->getPredicateCardUnorderedMap()) {
    }

std::string trim(const std::string& str,
        const std::string& whitespace = "\r \t")
{
    const auto strBegin = str.find_first_not_of(whitespace);
    if (strBegin == std::string::npos)
        return ""; // no content

    const auto strEnd = str.find_last_not_of(whitespace);
    const auto strRange = strEnd - strBegin + 1;

    return str.substr(strBegin, strRange);
}

std::string Program::readFromFile(std::string pathFile, bool rewriteMultihead) {
    LOG(INFOL) << "Read program from file " << pathFile;
    if (pathFile == "") {
        LOG(INFOL) << "Using default rule TI(A,B,C) :- TE(A,B,C)";
        return parseRule("TI(A,B,C) :- TE(A,B,C)", false);
    } else {
        std::ifstream file(pathFile);
        std::string line;
        while (std::getline(file, line)) {
            line = trim(line);
            if (line != "" && line.substr(0, 2) != "//") {
                LOG(DEBUGL) << "Parsing rule \"" << line << "\"";
                std::string s = parseRule(line, rewriteMultihead);
                if (s != "") {
                    return s;
                }
            }
        }
        //LOG(INFOL) << "New assigned constants: " << additionalConstants.size();
    }
    return "";
}

//TODO use rewrite Multihead
void Program::parseRuleFile(std::string pathFile, bool rewriteMultihead) {
    MC::RuleDriver driver;
    driver.parse(pathFile);
    MC::RuleAST *root = driver.get_root();
    //root->print();
    parseAST(root, rewriteMultihead, NULL, NULL, NULL);
}

void Program::parseAST(MC::RuleAST *root, bool rewriteMultihead, Dictionary *dictVariables, std::vector<Literal> *listOfLiterals, std::vector<VTerm> *terms){
    //TODO transform this to integers and use a switch
    std::string type = root->getType();
    if (type == "LISTOFRULES") {
        //LOG(DEBUGL) << "L. starting rulelist parsing";
        MC::RuleAST *first = root->getFirst();
        if (first != NULL) {
            parseAST(first, rewriteMultihead, dictVariables, listOfLiterals, terms);
        }
        MC::RuleAST *second = root->getSecond();
        if (second != NULL) {
            parseAST(second, rewriteMultihead, dictVariables, listOfLiterals, terms);
        }
        //LOG(DEBUGL) << "L. rulelist parsing completed";
    } else if (type == "RULE") {
        //LOG(DEBUGL) << "L. starting rule parsing";
        Dictionary dVariables;
        MC::RuleAST * headAST = root->getFirst();
        if (headAST ==NULL)
            throw "Error, headAST can not be null";
        MC::RuleAST * bodyAST = root->getSecond();
        if (bodyAST ==NULL)
            throw "Error, bodyAST can not be null";
        std::vector<Literal> listOfHeadLiterals;
        std::vector<Literal> listOfBodyLiterals;
        parseAST(headAST, rewriteMultihead, &dVariables, &listOfHeadLiterals, terms);
        parseAST(bodyAST, rewriteMultihead, &dVariables, &listOfBodyLiterals, terms);
        //Add the rule
        Rule r = Rule(allrules.size(), listOfHeadLiterals, listOfBodyLiterals);
        addRule(r, rewriteMultihead);
        //LOG(DEBUGL) <<  "L. rule parsing completed";
        LOG(DEBUGL) <<  "Adding rule: "<< r.toprettystring(this, this->kb, false);
    } else if (type == "LISTOFLITERALS") {
        //LOG(DEBUGL) <<  "L. starting listofliterals parsing";
        MC::RuleAST *firstLiteral = root->getFirst();
        if (firstLiteral != NULL) {
            parseAST(firstLiteral, rewriteMultihead, dictVariables, listOfLiterals, NULL);
        }
        MC::RuleAST *rest = root->getSecond();
        if (rest != NULL) {
            parseAST(rest, rewriteMultihead, dictVariables, listOfLiterals, NULL);
        }
        //LOG(DEBUGL) <<  "L. listofliterals parsing completed";
    } else if (type == "POSITIVELITERAL") {
        //LOG(DEBUGL) <<  "L starting positive literal parsing";
        std::string predicate = root->getValue();
        std::vector<VTerm> t;
        MC::RuleAST *listOfTerms = root->getFirst();
        parseAST(listOfTerms, rewriteMultihead, dictVariables, listOfLiterals, &t);
        if (t.size() != (uint8_t) t.size()) {
            throw "Arity of predicate " + predicate + " is too high (" + std::to_string(t.size()) + " > 255)";
        }

        VTuple t1((uint8_t) t.size());
        int pos = 0;
        for (std::vector<VTerm>::iterator itr = t.begin(); itr != t.end(); ++itr) {
            t1.set(*itr, pos++);
        }

        //Check if predicate starts with "neg_".
        //bool negated = false;
        //if (predicate.substr(0,4) == "neg_") {
        //    negated = true;
        //    predicate = predicate.substr(4);
        //}

        //Determine predicate
        PredId_t predid = (PredId_t) dictPredicates.getOrAdd(predicate);
        if (cardPredicates.find(predid) == cardPredicates.end()) {
            cardPredicates.insert(make_pair(predid, t.size()));
        } else {
            if (cardPredicates.find(predid)->second != t.size()) {
                throw ("Wrong arity in predicate \""+ predicate + "\". It should be " + std::to_string((int) cardPredicates.find(predid)->second) +".");
            }
        }
        Predicate pred(predid, Predicate::calculateAdornment(t1), kb->doesPredExists(predid) ? EDB : IDB, (uint8_t) t.size());
        LOG(DEBUGL) << "Predicate : " << predicate << ", type = " << ((pred.getType() == EDB) ? "EDB" : "IDB");

        Literal literal(pred, t1, false);
        listOfLiterals->push_back(literal);
        //LOG(DEBUGL) <<  "L. positive literal parsing completed";
    } else if (type == "NEGATEDLITERAL") {
        //LOG(DEBUGL) <<  "L. starting negative literal parsing";
        std::string predicate = root->getValue();
        std::vector<VTerm> t;
        MC::RuleAST *listOfTerms = root->getFirst();
        parseAST(listOfTerms, rewriteMultihead, dictVariables, listOfLiterals, &t);
        if (t.size() != (uint8_t) t.size()) {
            throw "Arity of predicate " + predicate + " is too high (" + std::to_string(t.size()) + " > 255)";
        }

        VTuple t1((uint8_t) t.size());
        int pos = 0;
        for (std::vector<VTerm>::iterator itr = t.begin(); itr != t.end(); ++itr) {
            t1.set(*itr, pos++);
        }

        //Check if predicate starts with "neg_".
        //bool negated = false;
        //if (predicate.substr(0,4) == "neg_") {
        //    negated = true;
        //    predicate = predicate.substr(4);
        //}

        //Determine predicate
        PredId_t predid = (PredId_t) dictPredicates.getOrAdd(predicate);
        if (cardPredicates.find(predid) == cardPredicates.end()) {
            cardPredicates.insert(make_pair(predid, t.size()));
        } else {
            if (cardPredicates.find(predid)->second != t.size()) {
                throw ("Wrong arity in predicate \""+ predicate + "\". It should be " + std::to_string((int) cardPredicates.find(predid)->second) +".");
            }
        }
        Predicate pred(predid, Predicate::calculateAdornment(t1), kb->doesPredExists(predid) ? EDB : IDB, (uint8_t) t.size());
        LOG(DEBUGL) << "Predicate : " << predicate << ", type = " << ((pred.getType() == EDB) ? "EDB" : "IDB");

        Literal literal(pred, t1, true);
        listOfLiterals->push_back(literal);
        //LOG(DEBUGL) <<  "L. negative literal parsing completed";
    } else if (type == "LISTOFTERMS") {
        //LOG(DEBUGL) <<  "L starting listofterms parsing";
        MC::RuleAST *firstTerm = root->getFirst();
        if (firstTerm != NULL) {
            parseAST(firstTerm, rewriteMultihead, dictVariables, listOfLiterals, terms);
        }
        MC::RuleAST *rest = root->getSecond();
        if (rest != NULL) {
            parseAST(rest, rewriteMultihead, dictVariables, listOfLiterals, terms);
        }
        //LOG(DEBUGL) <<  "L. listofterms parsing completed";
    } else if (type == "VARIABLE") {
        parseVariableAST(root, dictVariables, terms);
    } else if (type == "CONSTANT") {
        parseConstantAST(root, terms);
    } else {
        //LOG(DEBUGL) << "L. ERROR: " << type;
        throw "parseAST error";
    }
}

void Program::parseVariableAST(MC::RuleAST *root, Dictionary *dictVariables, std::vector<VTerm> *terms){
    //LOG(DEBUGL) << "L. starting variable parsing";
    if (dictVariables == NULL)
        throw "parseVariableAST error. dictVariables can not be null";
    if (terms == NULL)
        throw "parseVariableAST error. terms can not be null";
    std::string variable = root->getValue();
    uint64_t v = dictVariables->getOrAdd(variable);
    if (v != (uint8_t) v) {
        throw "Too many variables in rule (> 255)";
    }
    terms->push_back(VTerm((uint8_t) v, 0));
    //LOG(DEBUGL) << "L. variable parsing complete";
}

void Program::parseConstantAST(MC::RuleAST *root, std::vector<VTerm> *terms){
    //LOG(DEBUGL) << "L. starting constant parsing";
    std::string constant = rewriteRDFOWLConstants(root->getValue());
    //LOG(DEBUGL) << "L. constant" << constant;
    uint64_t dictTerm;
    if (!kb->getOrAddDictNumber(constant.c_str(), constant.size(), dictTerm)) {
        //Get an ID from the temporary dictionary
        //dictTerm = additionalConstants.getOrAdd(term);
        throw 10; //Could not add a term? Why?
    }
    if (terms == NULL)
        throw "parseConstantAST error. Terms can not be null";
    terms->push_back(VTerm(0, dictTerm));
    //MC::RuleAST * next = root->getFirst();
    //if (next != NULL)
    LOG(DEBUGL) << "L constant parsing completed";
}


std::string Program::readFromString(std::string rules, bool rewriteMultihead) {
    stringstream ss(rules);
    string rule;
    while (getline(ss, rule)) {
        rule = trim(rule);
        if (rule != "" && rule .substr(0, 2) != "//") {
            LOG(DEBUGL) << "Parsing rule " << rule;
            std::string s = parseRule(rule, rewriteMultihead);
            if (s != "") {
                return s;
            }
        }
    }
    return "";
    //LOG(INFOL) << "New assigned constants: " << additionalConstants.size();
}

std::string Program::compressRDFOWLConstants(std::string input) {
    size_t rdfPos = input.find("<http://www.w3.org/1999/02/22-rdf-syntax-ns#");
    if (rdfPos != std::string::npos) {
        return string("rdf:") + input.substr(44, input.size() - 45);
    }

    size_t rdfsPos = input.find("<http://www.w3.org/2000/01/rdf-schema#");
    if (rdfsPos != std::string::npos) {
        input = string("rdfs:") + input.substr(38, input.size() - 39);
        return input;
    }

    size_t owlPos = input.find("<http://www.w3.org/2002/07/owl#");
    if (owlPos != std::string::npos) {
        input = string("owl:") + input.substr(31, input.size() - 32);
        return input;
    }


    return input;
}

std::string Program::rewriteRDFOWLConstants(std::string input) {
    size_t rdfPos = input.find("rdf:");
    if (rdfPos != std::string::npos) {
        input = string("<http://www.w3.org/1999/02/22-rdf-syntax-ns#") + input.substr(rdfPos + 4, std::string::npos) + string(">");
        return input;
    }

    size_t rdfsPos = input.find("rdfs:");
    if (rdfsPos != std::string::npos) {
        input = string("<http://www.w3.org/2000/01/rdf-schema#") + input.substr(rdfsPos + 5, std::string::npos) + string(">");
        return input;
    }

    size_t owlPos = input.find("owl:");
    if (owlPos != std::string::npos) {
        input = string("<http://www.w3.org/2002/07/owl#") + input.substr(owlPos + 4, std::string::npos) + string(">");
        return input;
    }

    //is this the correct way of saying that something is a literal of type string?
    //this should be equivalent to convertString function (inmemorytable file)
    if (input.at(0)!='<' || input.at(input.length()-1)!='>'){
        if (input.at(0)=='"' && input.at(input.length()-1)=='"')
            input += "^^<http://www.w3.org/2001/XMLSchema#string>";
        else
            input = '"' + input + "\"^^<http://www.w3.org/2001/XMLSchema#string>";
    }
    return input;
}

Literal Program::parseLiteral(std::string l, Dictionary &dictVariables) {
    size_t posBeginTuple = l.find("(");
    if (posBeginTuple == std::string::npos) {
        throw "Missing '(' in literal";
    }
    std::string predicate = trim(l.substr(0, posBeginTuple));
    std::string tuple = l.substr(posBeginTuple + 1, std::string::npos);
    if (tuple[tuple.size() - 1] != ')') {
        throw "Missing ')' in literal";
    }
    tuple = trim(tuple.substr(0, tuple.size() - 1));

    LOG(DEBUGL) << "Found predicate \"" << predicate  << "\"";

    //Calculate the tuple
    std::vector<VTerm> t;
    std::string term;
    while (tuple.size() > 0) {
        size_t posTerm = 0;
        while (posTerm < tuple.size()) {
            if (tuple[posTerm] == ',' || tuple[posTerm] == ')') {
                break;
            }
            switch(tuple[posTerm]) {
                case '\'':
                    posTerm++;
                    while (posTerm < tuple.size()) {
                        if (tuple[posTerm] == '\\') {
                            posTerm++;
                            if (posTerm != tuple.size()) {
                                posTerm++;
                            }
                            continue;
                        }
                        if (tuple[posTerm] == '\'') {
                            // Index must be incremented here, because otherwise
                            // we have to deal with this quote in next iteration
                            posTerm++;
                            break;
                        }
                        posTerm++;
                    }
                    break;
                case '\"':
                    posTerm++;
                    while (posTerm < tuple.size()) {
                        if (tuple[posTerm] == '\\') {
                            posTerm++;
                            if (posTerm != tuple.size()) {
                                posTerm++;
                            }
                            continue;
                        }
                        if (tuple[posTerm] == '\"') {
                            // Index must be incremented here, because otherwise
                            // we have to deal with this quote in next iteration
                            posTerm++;
                            break;
                        }
                        posTerm++;
                    }
                    break;
                case '<':
                    posTerm++;
                    while (posTerm < tuple.size()) {
                        if (tuple[posTerm] == '\\') {
                            posTerm++;
                            if (posTerm != tuple.size()) {
                                posTerm++;
                            }
                            continue;
                        }
                        if (tuple[posTerm] == '>') {
                            break;
                        }
                        posTerm++;
                    }
                    break;
                default:
                    posTerm++;
                    break;
            }
        }
        if (posTerm != tuple.size()) {
            term = tuple.substr(0, posTerm);
            tuple = trim(tuple.substr(posTerm + 1, tuple.size()));
        } else {
            term = tuple;
            tuple = "";
        }

        LOG(DEBUGL) << "Found term \"" << term << "\"";

        //Parse the term
        if (std::isupper(term.at(0))) {
            uint64_t v = dictVariables.getOrAdd(term);
            if (v != (uint8_t) v) {
                throw "Too many variables in rule (> 255)";
            }
            t.push_back(VTerm((uint8_t) v, 0));
        } else {
            //Constant
            term = rewriteRDFOWLConstants(term);
            uint64_t dictTerm;
            if (!kb->getOrAddDictNumber(term.c_str(), term.size(), dictTerm)) {
                //Get an ID from the temporary dictionary
                //dictTerm = additionalConstants.getOrAdd(term);
                throw 10; //Could not add a term? Why?
            }

            t.push_back(VTerm(0, dictTerm));
        }
    }

    if (t.size() != (uint8_t) t.size()) {
        throw "Arity of predicate " + predicate + " is too high (" + std::to_string(t.size()) + " > 255)";
    }

    VTuple t1((uint8_t) t.size());
    int pos = 0;
    for (std::vector<VTerm>::iterator itr = t.begin(); itr != t.end(); ++itr) {
        t1.set(*itr, pos++);
    }

    //Check if predicate starts with "neg_".
    bool negated = false;
    if (predicate.substr(0,4) == "neg_") {
        negated = true;
        predicate = predicate.substr(4);
    }

    //Determine predicate
    PredId_t predid = (PredId_t) dictPredicates.getOrAdd(predicate);
    if (cardPredicates.find(predid) == cardPredicates.end()) {
        cardPredicates.insert(make_pair(predid, t.size()));
    } else {
        if (cardPredicates.find(predid)->second != t.size()) {
            throw ("Wrong arity in predicate \""+ predicate + "\". It should be " + std::to_string((int) cardPredicates.find(predid)->second) +".");
        }
    }
    Predicate pred(predid, Predicate::calculateAdornment(t1), kb->doesPredExists(predid) ? EDB : IDB, (uint8_t) t.size());
    LOG(DEBUGL) << "Predicate : " << predicate << ", type = " << ((pred.getType() == EDB) ? "EDB" : "IDB");

    Literal literal(pred, t1, negated);
    return literal;
}

PredId_t Program::getPredicateID(std::string & p, const uint8_t card) {
    PredId_t predid = (PredId_t) dictPredicates.getOrAdd(p);
    //add the cardinality associated to this predicate
    if (cardPredicates.find(predid) == cardPredicates.end()) {
        //add it
        cardPredicates.insert(std::make_pair(predid, card));
    } else {
        assert(cardPredicates.find(predid)->second == card);
    }
    return predid;
}

std::string Program::getPredicateName(const PredId_t id) {
    return dictPredicates.getRawValue(id);
}

Program Program::clone() const {
    return *this;
}

int Program::getNRules() const {
    int size = 0;
    for (int j = 0; j < rules.size(); ++j) {
        size += rules[j].size();
    }
    return size;
}

std::shared_ptr<Program> Program::cloneNew() const {
    Program *p = new Program(kb);
    p->dictPredicates = dictPredicates;
    p->cardPredicates = cardPredicates;
    return std::shared_ptr<Program>(p);
}

void Program::cleanAllRules() {
    rules.clear();
    allrules.clear();
}

void Program::addRule(Rule &rule, bool rewriteMultihead) {
    if (rewriteMultihead && rule.isExistential() && rule.getHeads().size() > 1) {
        rewriteRule(rule);
    } else {
        for (const auto &head : rule.getHeads()) {
            if (rules.size() <= head.getPredicate().getId()) {
                rules.resize(head.getPredicate().getId() + 1);
            }
            rules[head.getPredicate().getId()].push_back(allrules.size());
        }
        allrules.push_back(rule);
    }
}

void Program::addRule(std::vector<Literal> heads, std::vector<Literal> body, bool rewriteMultihead) {
    Rule rule(allrules.size(), heads, body);
    addRule(rule, rewriteMultihead);
}

void Program::addAllRules(std::vector<Rule> &rules) {
    for (auto &r : rules) {
        addRule(r);
    }
}

bool Program::isPredicateIDB(const PredId_t id) {
    return !kb->doesPredExists(id);
}

int Program::getNEDBPredicates() {
    int n = 0;
    for (const auto &el : dictPredicates.getMap()) {
        if (kb->doesPredExists(el.second)) {
            n++;
        }
    }
    return n;
}

int Program::getNIDBPredicates() {
    int n = 0;
    for (const auto &el : dictPredicates.getMap()) {
        if (!kb->doesPredExists(el.second)) {
            n++;
        }
    }
    return n;
}

size_t findEndLiteral(std::string s) {
    size_t pos = s.find("),");
    size_t oldpos = 0;
    while (pos != std::string::npos) {
        LOG(DEBUGL) << "string: " << s << ",found pos " << pos;
        int count = 0;
        for (int i = oldpos; i < pos; i++) {
            if (s[i] == '"') {
                count++;
            }
        }
        LOG(DEBUGL) << "quote count = " << count;
        if (count % 2 == 0) {
            return pos;
        }
        oldpos = pos;
        pos = s.find("),", oldpos+2);
    }
    return pos;
}

std::string Program::parseRule(std::string rule, bool rewriteMultihead) {
    //split the rule between head and body
    Dictionary dictVariables;
    try {
        size_t posEndHead = rule.find(":-");
        if (posEndHead == std::string::npos) {
            throw "Missing ':-'";
        }
        //process the head(s)
        std::string head = trim(rule.substr(0, posEndHead));
        std::vector<Literal> lHeads;
        LOG(DEBUGL) << "head = \"" << head << "\"";
        while (head.size() > 0) {
            std::string headLiteral;
            size_t posEndLiteral = findEndLiteral(head);
            if (posEndLiteral != std::string::npos) {
                headLiteral = trim(head.substr(0, posEndLiteral + 1));
                head = trim(head.substr(posEndLiteral + 2, std::string::npos));
            } else {
                headLiteral = head;
                head = "";
            }
            LOG(DEBUGL) << "headliteral = \"" << headLiteral << "\"";
            Literal h = parseLiteral(headLiteral, dictVariables);
            lHeads.push_back(h);
        }

        //process the body
        std::string body = trim(rule.substr(posEndHead+2, std::string::npos));
        LOG(DEBUGL) << "body = \"" << body << "\"";
        std::vector<Literal> lBody;
        while (body.size() > 0) {
            std::string bodyLiteral;
            size_t posEndLiteral = findEndLiteral(body);
            if (posEndLiteral != std::string::npos) {
                bodyLiteral = trim(body.substr(0, posEndLiteral + 1));
                body = trim(body.substr(posEndLiteral + 2, std::string::npos));
            } else {
                bodyLiteral = body;
                body = "";
            }
            LOG(DEBUGL) << "bodyliteral = \"" << bodyLiteral << "\"";
            lBody.push_back(parseLiteral(bodyLiteral, dictVariables));
        }

        //Add the rule
        Rule r = Rule(allrules.size(), lHeads, lBody);
        addRule(r, rewriteMultihead);
        return "";
    } catch (std::string e) {
        return "Failed parsing rule '" + rule + "': " + e;
    } catch(char const * e) {
        return "Failed parsing rule '" + rule + "': " + std::string(e);
    }
}

static bool isInVector(uint8_t v, std::vector<uint8_t> &vec) {
    for (int i = 0; i < vec.size(); i++) {
        if (v == vec[i]) {
            return true;
        }
    }
    return false;
}

void Program::rewriteRule(Rule &r) {
    std::vector<Literal> lHeads = r.getHeads();
    std::vector<Literal> lBody = r.getBody();
    LOG(DEBUGL) << "Trying to rewrite rule";
    std::vector<uint8_t> bodyVars;
    // First determine the non-existential variables.
    for (auto body: lBody) {
        for (int i = 0; i < body.getTupleSize(); i++) {
            const VTerm t = body.getTermAtPos(i);
            if (t.isVariable()) {
                if (! isInVector(t.getId(), bodyVars)) {
                    bodyVars.push_back(t.getId());
                }
            }
        }
    }

    std::vector<uint8_t> done;
    for (int i = 0; i < lHeads.size(); i++) {
        if (! isInVector(i, done)) {
            std::vector<uint8_t> extVars;
            Literal head = lHeads[i];
            std::vector<uint8_t> addedHeads;

            addedHeads.push_back(i);
            // Determine existential variables.
            for (int k = 0; k < head.getTupleSize(); k++) {
                const VTerm t = head.getTermAtPos(k);
                if (t.isVariable()
                        && ! isInVector(t.getId(), bodyVars)
                        && ! isInVector(t.getId(), extVars)) {
                    extVars.push_back(t.getId());
                }
            }
            if (extVars.size() == 0) {
                std::vector<Literal> heads;
                done.push_back(i);
                heads.push_back(lHeads[i]);
                Rule r = Rule(allrules.size(), heads, lBody);
                addRule(r);
                continue;
            }

            for (int j = i+1; j < lHeads.size(); j++) {
                if (isInVector(j, addedHeads)) {
                    continue;
                }
                // Go through the head, to see if it uses an extvar that is used earlier.
                Literal head1 = lHeads[j];
                bool used = false;
                for (int l = 0; l < head1.getTupleSize(); l++) {
                    const VTerm t = head1.getTermAtPos(l);
                    if (t.isVariable() && ! isInVector(t.getId(), bodyVars)) {
                        if (isInVector(t.getId(), extVars)) {
                            used = true;
                            break;
                        }
                    }
                }
                if (used) {
                    // If it does, we add it to the list of heads, and add all extvars that it uses
                    // to the list, and restart the loop.
                    for (int l = 0; l < head1.getTupleSize(); l++) {
                        const VTerm t = head1.getTermAtPos(l);
                        if (t.isVariable()
                                && ! isInVector(t.getId(), bodyVars)
                                && ! isInVector(t.getId(), extVars)) {
                            extVars.push_back(t.getId());
                        }
                    }
                    addedHeads.push_back(j);
                    j = i;  // Will be incremented.
                }
            }
            std::vector<Literal> newHeads;
            for (uint8_t h : addedHeads) {
                newHeads.push_back(lHeads[h]);
                done.push_back(h);
            }
            Rule r = Rule(allrules.size(), newHeads, lBody);
            addRule(r);
        }
    }
}

std::vector<Rule> Program::getAllRulesByPredicate(PredId_t predid) const {
    std::vector<Rule> out;
    for(const auto& idx : rules[predid])
        out.push_back(allrules[idx]);
    return out;
}

std::vector<Rule> Program::getAllRules() {
    return allrules;
}

struct RuleSorter {
    const std::vector<Rule> &origVector;
    RuleSorter(const std::vector<Rule> &v) : origVector(v) {
    }
    bool operator() (const size_t i1, const size_t i2) {
        return origVector[i1].getNIDBPredicates() < origVector[i2].getNIDBPredicates();
    }
};

void Program::sortRulesByIDBPredicates() {
    for (int i = 0; i < rules.size(); ++i) {
        if (rules[i].size() > 0) {
            std::vector<uint32_t> tmpC = rules[i];
            std::stable_sort(tmpC.begin(), tmpC.end(), RuleSorter(allrules));
            rules[i] = tmpC;
        }
    }
}

Predicate Program::getPredicate(std::string & p) {
    return getPredicate(p, 0);
}

Predicate Program::getPredicate(const PredId_t id) {
    if (kb->doesPredExists(id)) {
        return Predicate(id, 0, EDB, kb->getPredArity(id));
    }
    if (cardPredicates.find(id) != cardPredicates.end()) {
        uint8_t card = cardPredicates.find(id)->second;
        return Predicate(id, 0, IDB, card);
    }
    return Predicate(id, 0, IDB, 0);
}

Predicate Program::getPredicate(std::string & p, uint8_t adornment) {
    PredId_t id = (PredId_t) dictPredicates.getOrAdd(p);
    if (kb->doesPredExists(id)) {
        return Predicate(id, adornment, EDB, kb->getPredArity(id));
    }
    if (cardPredicates.find(id) != cardPredicates.end()) {
        uint8_t card = cardPredicates.find(id)->second;
        return Predicate(id, adornment, IDB, card);
    }
    return Predicate(id, 0, IDB, 0);
}

int64_t Program::getOrAddPredicate(std::string & p, uint8_t cardinality) {
    PredId_t id = (PredId_t) dictPredicates.getOrAdd(p);
    if (cardPredicates.find(id) == cardPredicates.end()) {
        cardPredicates.insert(make_pair(id, cardinality));
    } else {
        if (cardPredicates.find(id)->second != cardinality) {
            LOG(INFOL) << "Wrong cardinality for predicate " << p << ": should be " << (int) cardPredicates.find(id)->second;
            return -1;
        }
    }
    if (id >= rules.size()) {
        rules.resize(id+1);
    }
    return id;
}

std::string Program::getAllPredicates() {
    return dictPredicates.tostring();
}

std::vector<std::string> Program::getAllPredicateStrings() {
    return dictPredicates.getKeys();
}

std::vector<PredId_t> Program::getAllEDBPredicateIds() {
    std::vector<PredId_t> output;
    std::vector<std::string> predicateStrings = this->getAllPredicateStrings();
    for (int i = 0; i < predicateStrings.size(); ++i) {
        PredId_t pid = this->getPredicate(predicateStrings[i]).getId();
        if (kb->doesPredExists(pid)) {
            output.push_back(pid);
        }
    }
    return output;
}

std::string Program::tostring() {
    std::string output = "";
    for(const auto &rule : allrules) {
        output += rule.tostring() + std::string("\n");
    }
    return output;
}

std::string extractFileName(std::string& filePath) {
    int index = filePath.find_last_of('/');
    std::string out = filePath.substr(index+1, (filePath.find_last_of('.') - index)-1);
    return out;
}

std::vector<Substitution> concat(std::vector<Substitution>& sigma1, std::vector<Substitution>& sigma2) {
    std::vector<Substitution> result;
    for (std::vector<Substitution>::iterator itr1 = sigma1.begin(); itr1 != sigma1.end(); ++itr1) {
        for (std::vector<Substitution>::iterator itr2 = sigma2.begin(); itr2 != sigma2.end(); ++itr2) {
            if (itr1->origin == itr2->origin) {
                if (itr2->destination.isVariable()){
                    result.push_back(Substitution(itr1->destination.getId(), VTerm(itr2->destination.getId(), 0)));
                } else {
                    result.push_back(Substitution(itr1->destination.getId(), VTerm(0, itr2->destination.getValue())));
                }
            }
        }
    }
    return result;
}

std::vector<Substitution> inverse_concat(std::vector<Substitution>& sigma1, std::vector<Substitution>& sigma2) {
    std::vector<Substitution> result;
    for (std::vector<Substitution>::iterator itr1 = sigma1.begin(); itr1 != sigma1.end(); ++itr1) {
        for (std::vector<Substitution>::iterator itr2 = sigma2.begin(); itr2 != sigma2.end(); ++itr2) {
            if (itr1->destination == itr2->destination) {
                result.push_back(Substitution(itr1->origin, VTerm(itr2->origin, 0)));
            }
        }
    }
    return result;
}
