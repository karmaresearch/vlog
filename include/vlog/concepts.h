#ifndef CONCEPTS_H
#define CONCEPTS_H

#include <vlog/support.h>
#include <vlog/consts.h>
#include <vlog/graph.h>

#include <kognac/logs.h>

#include <string>
#include <inttypes.h>
#include <stdlib.h>
#include <vector>
#include <set>
#include <map>
#include <unordered_map>

/*** PREDICATES ***/
#define EDB 0
#define IDB 1
#define MAX_NPREDS (2048*1024)


typedef uint32_t PredId_t;
typedef uint32_t Var_t;

class EDBLayer;

inline std::string fields2str(const std::vector<uint8_t> &fields) {
    std::ostringstream os;
    os << "[" << fields.size() << "]{";
    for (auto f : fields) {
        os << (int)f << ",";
    }
    os << "}";

    return os.str();
}


/*** TERMS ***/
class VTerm {
    private:
        Var_t id; //ID != 0 => variable. ID==0 => const value
        uint64_t value;
    public:
        VTerm() : id(0), value(0) {}
        VTerm(const Var_t id, const uint64_t value) : id(id), value(value) {}
        Var_t getId() const {
            return id;
        }
        uint64_t getValue() const {
            return value;
        }
        void setId(const Var_t i) {
            id = i;
        }
        void setValue(const uint64_t v) {
            value = v;
        }
        bool isVariable() const {
            return id > 0;
        }
        bool operator==(const VTerm& rhs) const {
            return id == rhs.getId() && value == rhs.getValue();
        }
        bool operator!=(const VTerm& rhs) const {
            return id != rhs.getId() || value != rhs.getValue();
        }
};

/*** TUPLES ***/
class VTuple {
    private:
        const uint8_t sizetuple;
        VTerm *terms;
    public:
        VTuple(const uint8_t sizetuple) : sizetuple(sizetuple) {
            terms = new VTerm[sizetuple];
        }

        VTuple(const VTuple &v) : sizetuple(v.sizetuple) {
            terms = new VTerm[sizetuple];
            for (int i = 0; i < sizetuple; i++) {
                terms[i] = v.terms[i];
            }
        }

        size_t getSize() const {
            return sizetuple;
        }

        VTerm get(const int pos) const {
            return terms[pos];
        }

        void set(const VTerm term, const int pos) {
            terms[pos] = term;
        }

        void replaceAll(const VTerm termA, const VTerm termB) {
            for (int i = 0; i < sizetuple; i++) {
                if (terms[i] == termA) {
                    terms[i] = termB;
                }
            }
        }

        std::vector<std::pair<uint8_t, uint8_t>> getRepeatedVars() const {
            std::vector<std::pair<uint8_t, uint8_t>> output;
            for (int i = 0; i < sizetuple; ++i) {
                VTerm t1 = get(i);
                if (t1.isVariable()) {
                    for (int j = i + 1; j < sizetuple; ++j) {
                        VTerm t2 = get(j);
                        if (t2.getId() == t1.getId()) {
                            output.push_back(std::make_pair(i, j));
                        }
                    }
                }
            }
            return output;
        }

        bool operator==(const VTuple &other) const {
            if (sizetuple == other.sizetuple) {
                if (terms == other.terms) {
                    return true;
                }
                for (int i = 0; i < sizetuple; i++) {
                    if (terms[i].getId() != other.terms[i].getId()) {
                        return false;
                    }
                    if (terms[i].getId() == 0) {
                        if (terms[i].getValue() != other.terms[i].getValue()) {
                            return false;
                        }
                    }
                }
                return true;
            }
            return false;
        }

        /*
           VTuple & operator=(const VTuple &v) {
           if (this == &v) {
           return *this;
           }
           if (terms != NULL) {
           delete[] terms;
           }
           sizetuple = v.sizetuple;
           terms = new VTerm[sizetuple];
           for (int i = 0; i < sizetuple; i++) {
           terms[i] = v.terms[i];
           }
           return *this;
           }
           */
        //L. Can I create an iterator on it? begin, end etc?

        ~VTuple() {
            delete[] terms;
        }
};

struct hash_VTuple {
    size_t operator()(const VTuple &v) const {
        size_t hash = 0;
        int sz = v.getSize();
        for (int i = 0; i < sz; i++) {
            VTerm term = v.get(i);
            if (term.isVariable()) {
                hash = (hash + (324723947 + term.getId())) ^93485734985;
            } else {
                hash = (hash + (324723947 + term.getValue())) ^93485734985;
            }
        }
        return hash;
    }
};

class Predicate {
    private:
        const PredId_t id;
        const uint8_t type;
        const uint8_t adornment;
        const uint8_t card;

    public:
        Predicate(const Predicate &pred) : id(pred.id), type(pred.type), adornment(pred.adornment), card(pred.card) {
        }

        Predicate(const Predicate p, const uint8_t adornment) : id(p.id),
        type(p.type), adornment(adornment), card(p.card) {
        }

        Predicate(const PredId_t id, const uint8_t adornment, const uint8_t type,
                const uint8_t card) : id(id), type(type), adornment(adornment),
        card(card) {
        }

        PredId_t getId() const {
            return id;
        }

        uint8_t getType() const {
            return type & 1;
        }

        bool isMagic() const {
            return (type >> 1) != 0;
        }

        uint8_t getAdornment() const {
            return adornment;
        }

        uint8_t getCardinality() const {
            return card;
        }

        /*static bool isEDB(std::string pred) {
          return pred.at(pred.size() - 1) == 'E';
          }*/

        static uint8_t calculateAdornment(VTuple &t) {
            uint8_t adornment = 0;
            for (size_t i = 0; i < t.getSize(); ++i) {
                if (!t.get(i).isVariable()) {
                    adornment += 1 << i;
                }
            }
            return adornment;
        }

        static uint8_t changeVarToConstInAdornment(const uint8_t adornment, const uint8_t pos) {
            uint8_t shiftedValue = (uint8_t) (1 << pos);
            return adornment | shiftedValue;
        }

        static uint8_t getNFields(uint8_t adornment) {
            uint8_t n = 0;
            for (int i = 0; i < 8; ++i) {
                if (adornment & 1)
                    n++;
                adornment >>= 1;
            }
            return n;
        }
};

/*** SUBSTITUTIONS ***/
struct Substitution {
    Var_t origin;
    VTerm destination;
    Substitution() {}
    Substitution(Var_t origin, VTerm destination) : origin(origin), destination(destination) {}
};

VLIBEXP std::vector<Substitution> concat(std::vector<Substitution>&, std::vector<Substitution>&);
VLIBEXP std::vector<Substitution> inverse_concat(std::vector<Substitution>&, std::vector<Substitution>&);
VLIBEXP std::string extractFileName(std::string& filePath);

/*** LITERALS ***/
class Program;
class Literal {
    private:
        const Predicate pred;
        const VTuple tuple;
        const bool negated;
    public:
        Literal(const Predicate pred, const VTuple tuple) : pred(pred), tuple(tuple), negated(false) {}

        Literal(const Predicate pred, const VTuple tuple, bool negated) : pred(pred), tuple(tuple), negated(negated) {}

        Predicate getPredicate() const {
            return pred;
        }

        bool isMagic() const {
            return pred.isMagic();
        }

        VTerm getTermAtPos(const int pos) const {
            return tuple.get(pos);
        }

        size_t getTupleSize() const {
            return tuple.getSize();
        }

        VTuple getTuple() const {
            return tuple;
        }

        size_t getNBoundVariables() const {
            return pred.getNFields(pred.getAdornment());
        }

        bool isNegated() const {
            return negated;
        }

        //L. do we need this function?
        //static int mgu(Substitution *substitutions, const Literal &l, const Literal &m);

        static int subsumes(std::vector<Substitution> &substitutions, const Literal &from, const Literal &to);

        static int getSubstitutionsA2B(
                std::vector<Substitution> &substitutions, const Literal &a, const Literal &b);

        Literal substitutes(std::vector<Substitution> &substitions, int *nsubs = NULL) const;

        bool sameVarSequenceAs(const Literal &l) const;

        VLIBEXP uint8_t getNVars() const;

        VLIBEXP uint8_t getNConstants() const;

        uint8_t getNUniqueVars() const;

        bool hasRepeatedVars() const;

        // Returns for each variable the position in the tuple.
        std::vector<uint8_t> getPosVars() const;

        // Returns for each position in the tuple the variable number. If the position in the tuple
        // contains a constant, it gives -1.
        std::vector<int> getVarnumInLiteral() const;

        std::vector<uint8_t> getVarCount() const;

        std::vector<std::pair<uint8_t, uint8_t>> getRepeatedVars() const;

        std::vector<Var_t> getSharedVars(const std::vector<Var_t> &vars) const;

        std::vector<Var_t> getNewVars(std::vector<Var_t> &vars) const;

        std::vector<Var_t> getAllVars() const;
        bool containsVariable(Var_t variableId) const;

        std::string tostring(const Program *program, const EDBLayer *db) const;

        std::string toprettystring(const Program *program, const EDBLayer *db, bool replaceConstants = false) const;

        std::string tostring() const;

        bool operator ==(const Literal &other) const;
};

class Rule {
    private:
        const uint32_t ruleId;
        const std::vector<Literal> heads;
        const std::vector<Literal> body;
        const bool _isRecursive;
        const bool existential;
        const bool egd;

        static bool checkRecursion(const std::vector<Literal> &head,
                const std::vector<Literal> &body);

    public:
        bool doesVarAppearsInFollowingPatterns(int startingPattern, Var_t value) const;

        Rule(uint32_t ruleId, const std::vector<Literal> heads,
                std::vector<Literal> body, bool egd) :
            ruleId(ruleId),
            heads(heads),
            body(body),
            _isRecursive(checkRecursion(heads, body)),
            existential(!getExistentialVariables().empty()),
            egd(egd) {
                checkRule();
            }

        Rule(uint32_t ruleId, Rule &r) : ruleId(ruleId),
        heads(r.heads), body(r.body), _isRecursive(r._isRecursive),
        existential(r.existential), egd(r.egd) {
        }

        Rule createAdornment(uint8_t headAdornment) const;

        bool isRecursive() const {
            return this->_isRecursive;
        }

        uint32_t getId() const {
            return ruleId;
        }

        bool isEGD() const {
            return egd;
        }

        const std::vector<Literal> &getHeads() const {
            return heads;
        }

        Literal getFirstHead() const {
            // if (heads.size() > 1)
            //     LOG(WARNL) << "This method should be called only if we handle multiple heads properly...";
            return heads[0];
        }

        Literal getHead(unsigned pos) const {
            return heads[pos];
        }

        bool isExistential() const;

        std::vector<Var_t> getVarsInHead(PredId_t ignore = -1) const;
        std::vector<Var_t> getVarsInBody() const;
        std::vector<Var_t> getExistentialVariables() const;

        std::vector<Var_t> getFrontierVariables(PredId_t ignore = -1) const;

        std::map<Var_t, std::vector<Var_t>> calculateDependencies() const;

        const std::vector<Literal> &getBody() const {
            return body;
        }

        unsigned getNIDBPredicates() const {
            unsigned i = 0;
            for (std::vector<Literal>::const_iterator itr = body.begin(); itr != body.end();
                    ++itr) {
                if (itr->getPredicate().getType() == IDB) {
                    i++;
                }
            }
            return i;
        }

        unsigned getNIDBNotMagicPredicates() const {
            unsigned i = 0;
            for (std::vector<Literal>::const_iterator itr = body.begin(); itr != body.end();
                    ++itr) {
                if (itr->getPredicate().getType() == IDB && !itr->getPredicate().isMagic()) {
                    i++;
                }
            }
            return i;
        }

        unsigned getNEDBPredicates() const {
            unsigned i = 0;
            for (std::vector<Literal>::const_iterator itr = body.begin(); itr != body.end();
                    ++itr) {
                if (itr->getPredicate().getType() == EDB) {
                    i++;
                }
            }
            return i;
        }

        unsigned numberOfNegatedLiteralsInBody() {
            unsigned result = 0;
            for (std::vector<Literal>::const_iterator itr = getBody().begin();
                    itr != getBody().end(); ++itr) {
                if (itr->isNegated()){
                    ++result;
                }
            }
            return result;
        }

        void checkRule() const;

        Rule normalizeVars() const;

        std::string tostring() const;
        std::string tostring(Program *program, EDBLayer *db) const;
        std::string toprettystring(Program *program, EDBLayer *db, bool replaceConstants = false) const;

        ~Rule() {
        }
};

class Program {
    private:
        EDBLayer *kb;
        std::vector<std::vector<uint32_t>> rules; // [head_predicate_id (idx) : [rule_ids]]
        std::vector<Rule> allrules;
        int rewriteCounter;

        Dictionary dictPredicates;
        std::unordered_map<PredId_t, uint8_t> cardPredicates;

        void rewriteRule(std::vector<Literal> &heads, std::vector<Literal> &body);

        void addRule(Rule &rule);

    public:
        VLIBEXP Program(EDBLayer *kb);

        VLIBEXP Program(Program *p, EDBLayer *kb);

        EDBLayer *getKB() const {
            return kb;
        }

        VLIBEXP void setKB(EDBLayer *e) {
            kb = e;
        }

        uint64_t getMaxPredicateId() {
            return dictPredicates.getCounter();
        }

        static std::string prettifyName(std::string name);

        std::string parseRule(std::string rule, bool rewriteMultihead);

        VLIBEXP std::vector<PredId_t> getAllPredicateIDs() const;

        VLIBEXP Literal parseLiteral(std::string literal, Dictionary &dictVariables);

        VLIBEXP std::string readFromFile(std::string pathFile, bool rewriteMultihead = false);

        VLIBEXP std::string readFromString(std::string rules, bool rewriteMultihead = false);

        // Adds predicate if it doesn't exist yet
        PredId_t getPredicateID(const std::string &p, const uint8_t card);

        VLIBEXP std::string getPredicateName(const PredId_t id) const;

        // Adds predicate if it doesn't exist yet
        VLIBEXP Predicate getPredicate(const std::string &p);

        // Adds predicate if it doesn't exist yet
        VLIBEXP Predicate getPredicate(const std::string &p, uint8_t adornment);

        VLIBEXP Predicate getPredicate(const PredId_t id) const;

        VLIBEXP int64_t getOrAddPredicate(const std::string &p, uint8_t cardinality);

        VLIBEXP bool doesPredicateExist(const PredId_t id) const;

        std::vector<Rule> getAllRulesByPredicate(PredId_t predid) const;

        const std::vector<uint32_t> &getRulesIDsByPredicate(PredId_t predid) const;

        size_t getNRulesByPredicate(PredId_t predid) const;

        const Rule &getRule(uint32_t ruleid) const;

        VLIBEXP void sortRulesByIDBPredicates();

        VLIBEXP std::vector<Rule> getAllRules() const;

        VLIBEXP int getNRules() const;

        Program clone() const;

        std::shared_ptr<Program> cloneNew() const;

        void cleanAllRules();

        VLIBEXP void addRule(std::vector<Literal> heads,
                std::vector<Literal> body) {
            addRule(heads, body, false, false);
        }

        VLIBEXP void addRule(std::vector<Literal> heads,
                std::vector<Literal> body, bool rewriteMultihead, bool isEGD);

        void addAllRules(std::vector<Rule> &rules);

        VLIBEXP bool isPredicateIDB(const PredId_t id) const;

        std::string getAllPredicates() const;

        std::vector<std::string> getAllPredicateStrings() const;

        int getNEDBPredicates() const;

        int getNIDBPredicates() const;

        std::string tostring() const;

        VLIBEXP bool areExistentialRules() const;

        int getNPredicates() {
            return rules.size();
        }

        static std::string rewriteRDFOWLConstants(std::string input);

        static std::string compressRDFOWLConstants(std::string input);

        // Should be const
        VLIBEXP std::vector<PredId_t> getAllEDBPredicateIds();

        uint8_t getPredicateCard(PredId_t pred) {
            return cardPredicates[pred];
        }

        VLIBEXP std::vector<PredId_t> getAllIDBPredicateIds();
        //
        // Returns true if stratification succeeded, and then stores the
        // stratification in the parameter.
        // The result vector is indexed by predicate id, and then gives the
        // stratification class.
        // The number of stratification classes is also returned.
        bool stratify(std::vector<int> &stratification, int &nStatificationClasses);

        VLIBEXP void axiomatizeEquality();

        VLIBEXP void singulariseEquality();

        ~Program() {
        }
};
#endif
