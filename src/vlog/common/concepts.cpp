#include <vlog/concepts.h>
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
                        std::string t = program->getFromAdditional(id);
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

std::string Literal::toprettystring(Program *program, EDBLayer *db) const {

    std::string predName;
    if (program != NULL)
        predName = program->getPredicateName(pred.getId());
    else
        predName = std::to_string(pred.getId());

    std::string out = predName + std::string("(");

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
                    string v = Program::compressRDFOWLConstants(std::string(text));
                    if (v[0] == '<')
                        v = v.substr(1, v.size() - 2);
                    out += v;
                } else {
                    if (program == NULL) {
                        out += std::to_string(id);
                    } else {
                        std::string t = program->getFromAdditional(id);
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

int Literal::subsumes(Substitution *substitutions, const Literal &l, const Literal &m) {
    if (l.getPredicate().getId() != m.getPredicate().getId()) {
        return -1;
    }
    assert (l.getTupleSize() == m.getTupleSize());

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
        } else {
            if (tm.isVariable())
                return -1;
        }
    }
    return tupleSize;
}

int Literal::getSubstitutionsA2B(Substitution *substitutions,
        const Literal &a, const Literal &b) {
    if (a.getPredicate().getId() != b.getPredicate().getId()) {
        return -1;
    }
    assert(a.getTupleSize() == b.getTupleSize());

    int tupleSize = 0;
    for (int i = 0; i < a.getTupleSize(); ++i) {
        VTerm ta = a.getTermAtPos(i);
        VTerm tb = b.getTermAtPos(i);

        if (!ta.isVariable() && !tb.isVariable() &&
                ta.getValue() != tb.getValue())
            return -1;

        if (ta.isVariable()) {
            bool found = false;
            //Is there already a substitution?
            for (int j = 0; j < tupleSize && !found; ++j) {
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
                substitutions[tupleSize++] = Substitution(ta.getId(), tb);
        } else {
            //if (tm.isVariable())
            //    return -1;
        }
    }
    return tupleSize;
}

Literal Literal::substitutes(Substitution * subs, const int nsubs) const {
    VTuple newTuple((uint8_t) this->tuple.getSize());
    for (uint8_t i = 0; i < newTuple.getSize(); ++i) {
        bool found = false;
        int j = 0;
        while (j < nsubs && !found) {
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
    return true;
}

bool Rule::checkRecursion(const std::vector<Literal> &heads,
        const std::vector<Literal> &body) {
    for (const auto bodyLit : body) {
        Substitution subs[10];
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
    if (varCount > SIZETUPLE) {
        LOG(ERRORL) << "SIZETUPLE needs to be set to at least " << varCount << "!";
        abort();
    }
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
        Literal newHead = head.substitutes(&subs[0], subs.size());
        LOG(DEBUGL) << "head = " << head.tostring() << ", newHead = " << newHead.tostring();
        newheads.push_back(newHead);
    }
    std::vector<Literal> newBody;
    for (std::vector<Literal>::const_iterator itr = body.cbegin(); itr != body.cend();
            ++itr) {
        newBody.push_back(itr->substitutes(&subs[0], subs.size()));
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
            if (!ok)
                out.push_back(var);
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

std::string Rule::toprettystring(Program * program, EDBLayer *db) const {
    std::string output = "";
    for(const auto& head : heads) {
        output += head.toprettystring(program, db) + " AND ";
    }
    output = output.substr(0, output.length() - 5);
    output += ":-";
    for (int i = 0; i < body.size(); ++i) {
        output += body[i].toprettystring(program, db) + std::string(",");
    }
    output = output.substr(0, output.size() - 1);
    return output;
}

bool Program::areExistentialRules() {
    for(auto& rule : allrules) {
        if (rule.isExistential()) {
            return true;
        }
    }
    return false;
}

const Rule &Program::getRule(uint32_t ruleid) const {
    return allrules[ruleid];
}

size_t Program::getNRulesByPredicate(PredId_t predid) const {
    return rules[predid].size();
}

const std::vector<uint32_t> &Program::getRulesIDsByPredicate(PredId_t predid) const {
    return rules[predid];
}

Program::Program(const uint64_t assignedIds,
        EDBLayer *kb) : assignedIds(assignedIds),
    kb(kb),
    dictPredicates(kb->getPredDictionary()),
    additionalConstants(assignedIds) {
    }

void Program::readFromFile(std::string pathFile, bool rewriteMultihead) {
    LOG(INFOL) << "Read program from file " << pathFile;
    if (pathFile == "") {
        LOG(INFOL) << "Using default rule TI(A,B,C) :- TE(A,B,C)";
        parseRule("TI(A,B,C) :- TE(A,B,C)", false);
    } else {
        std::ifstream file(pathFile);
        std::string line;
        while (std::getline(file, line)) {
            if (line != "" && line.substr(0, 2) != "//") {
                LOG(DEBUGL) << "Parsing rule " << line;
                parseRule(line, rewriteMultihead);
            }
        }
        LOG(INFOL) << "New assigned constants: " << additionalConstants.size();
    }
}

void Program::readFromString(std::string rules, bool rewriteMultihead) {
    stringstream ss(rules);
    string rule;
    while (getline(ss, rule)) {
        if (rule != "" && rule .substr(0, 2) != "//") {
            LOG(DEBUGL) << "Parsing rule " << rule;
            parseRule(rule, rewriteMultihead);
        }
    }
    LOG(INFOL) << "New assigned constants: " << additionalConstants.size();
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

    return input;
}

Literal Program::parseLiteral(std::string l, Dictionary &dictVariables) {
    size_t posBeginTuple = l.find("(");
    if (posBeginTuple == std::string::npos) {
        throw 10;
    }
    std::string predicate = l.substr(0, posBeginTuple);
    std::string tuple = l.substr(posBeginTuple + 1, std::string::npos);
    tuple = tuple.substr(0, tuple.size() - 1);

    //Calculate the tuple
    std::vector<VTerm> t;
    while (tuple.size() > 0) {
        size_t posTerm = tuple.find(",");
        std::string term;
        if (posTerm != std::string::npos) {
            term = tuple.substr(0, posTerm);
            tuple = tuple.substr(posTerm + 1, std::string::npos);
        } else {
            term = tuple;
            tuple = "";
        }

        //Parse the term
        if (std::isupper(term.at(0))) {
            t.push_back(VTerm((uint8_t) dictVariables.getOrAdd(term), 0));
        } else {
            //Constant
            term = rewriteRDFOWLConstants(term);
            uint64_t dictTerm;
            if (!kb->getDictNumber(term.c_str(), term.size(), dictTerm)) {
                //Get an ID from the temporary dictionary
                dictTerm = additionalConstants.getOrAdd(term);
            }

            t.push_back(VTerm(0, dictTerm));
        }
    }

    VTuple t1((uint8_t) t.size());
    int pos = 0;
    for (std::vector<VTerm>::iterator itr = t.begin(); itr != t.end(); ++itr) {
        t1.set(*itr, pos++);
    }

    //Determine predicate
    PredId_t predid = (PredId_t) dictPredicates.getOrAdd(predicate);
    if (cardPredicates.find(predid) == cardPredicates.end()) {
        cardPredicates.insert(make_pair(predid, t.size()));
    } else {
        if (cardPredicates.find(predid)->second != t.size()) {
            LOG(INFOL) << "Wrong size in predicate: should be " << (int) cardPredicates.find(predid)->second;
            throw 10;
        }
    }
    Predicate pred(predid, Predicate::calculateAdornment(t1), kb->doesPredExists(predid) ? EDB : IDB, (uint8_t) t.size());

    Literal literal(pred, t1);
    return literal;
}

PredId_t Program::getPredicateID(std::string & p, const uint8_t card) {
    PredId_t predid = (PredId_t) dictPredicates.getOrAdd(p);
    if (predid >= MAX_NPREDS) {
        LOG(DEBUGL) << "Too many predicates";
        throw OUT_OF_PREDICATES;
    }
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
    for (int j = 0; j < MAX_NPREDS; ++j) {
        size += rules[j].size();
    }
    return size;
}

std::shared_ptr<Program> Program::cloneNew() const {
    Program *p = new Program(assignedIds, kb);
    p->dictPredicates = dictPredicates;
    p->cardPredicates = cardPredicates;
    p->additionalConstants = additionalConstants;
    return std::shared_ptr<Program>(p);
}

void Program::cleanAllRules() {
    for (int i = 0; i < MAX_NPREDS; ++i) {
        rules[i].clear();
    }
    allrules.clear();
}

void Program::addRule(Rule &rule) {
    for (const auto &head : rule.getHeads()) {
        rules[head.getPredicate().getId()].push_back(allrules.size());
    }
    allrules.push_back(rule);
}

void Program::addRule(std::vector<Literal> heads, std::vector<Literal> body) {
    Rule rule(allrules.size(), heads, body);
    for (const auto &head : heads) {
        rules[head.getPredicate().getId()].push_back(allrules.size());
    }
    allrules.push_back(rule);
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


void Program::parseRule(std::string rule, bool rewriteMultihead) {
    //split the rule between head and body
    Dictionary dictVariables;
    try {
        size_t posEndHead = rule.find(":-");
        if (posEndHead == std::string::npos) {
            throw 10;
        }
        //process the head(s)
        std::string head = rule.substr(0, posEndHead - 1);
        std::vector<Literal> lHeads;
        while (head.size() > 0) {
            std::string headLiteral;
            size_t posEndLiteral = head.find("),");
            if (posEndLiteral != std::string::npos) {
                headLiteral = head.substr(0, posEndLiteral + 1);
                head = head.substr(posEndLiteral + 2, std::string::npos);
            } else {
                headLiteral = head;
                head = "";
            }
            Literal h = parseLiteral(headLiteral, dictVariables);
            lHeads.push_back(h);
        }

        //process the body
        std::string body = rule.substr(posEndHead + 3, std::string::npos);
        std::vector<Literal> lBody;
        while (body.size() > 0) {
            std::string bodyLiteral;
            size_t posEndLiteral = body.find("),");
            if (posEndLiteral != std::string::npos) {
                bodyLiteral = body.substr(0, posEndLiteral + 1);
                body = body.substr(posEndLiteral + 2, std::string::npos);
            } else {
                bodyLiteral = body;
                body = "";
            }
            lBody.push_back(parseLiteral(bodyLiteral, dictVariables));
        }

        //Add the rule
        Rule r = Rule(allrules.size(), lHeads, lBody);
	if (rewriteMultihead && r.isExistential() && lHeads.size() > 1) {
	    rewriteRule(lHeads, lBody);
	} else {
	    addRule(r);
	}
    } catch (int e) {
        LOG(ERRORL) << "Failed in parsing rule " << rule;
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

void Program::rewriteRule(std::vector<Literal> &lHeads, std::vector<Literal> &lBody) {
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
		    j = i;	// Will be incremented.
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
    for (int i = 0; i < MAX_NPREDS; ++i) {
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
    uint8_t card = cardPredicates.find(id)->second;
    return Predicate(id, 0, kb->doesPredExists(id) ? EDB : IDB,
            card);
}

Predicate Program::getPredicate(std::string & p, uint8_t adornment) {
    PredId_t id = (PredId_t) dictPredicates.getOrAdd(p);
    return Predicate(id, adornment, kb->doesPredExists(id) ? EDB : IDB,
            cardPredicates.find(id)->second);
}

std::string Program::getAllPredicates() {
    return dictPredicates.tostring();
}

std::string Program::tostring() {
    std::string output = "";
    /*for (int i = 0; i < MAX_NPREDS; ++i) {
      for (std::vector<Rule>::iterator itr = rules[i].begin(); itr != rules[i].end();
      ++itr) {
      output += itr->tostring() + std::string("\n");
      }
      }*/
    for(const auto &rule : allrules) {
        output += rule.tostring() + std::string("\n");
    }
    return output;
}
