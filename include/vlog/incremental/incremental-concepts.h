#ifndef INCREMENTAL__CONCEPTS_H__
#define INCREMENTAL__CONCEPTS_H__

#include <unordered_set>

#include <kognac/progargs.h>

#include <vlog/concepts.h>
#include <vlog/edb.h>
#include <vlog/seminaiver.h>
#include <vlog/inmemory/inmemorytable.h>


class IncrementalState {

protected:
    // const
    ProgramArgs &vm;
    const std::shared_ptr<SemiNaiver> fromSemiNaiver;
    const std::vector<std::string> &eMinus;

    EDBConf *conf;
    EDBLayer *layer;
    Program *program;

    std::string dredDir;
    std::shared_ptr<SemiNaiver> sn;

    std::unordered_map<PredId_t, const EDBRemoveLiterals *> rm;

    // cache vm[*]
    int nthreads;
    int interRuleThreads;

    IncrementalState(// const
                     ProgramArgs &vm,
                     const std::shared_ptr<SemiNaiver> from,
                     const std::vector<std::string> &eMinus);

    virtual ~IncrementalState();

    static std::string int2ABC(int x);

    static std::string printArgs(const Literal &lit, const EDBLayer *kb);

public:
    std::shared_ptr<SemiNaiver> getPrevSemiNaiver() const {
        return fromSemiNaiver;
    }

    static std::string name2dMinus(const std::string &name) {
        return name + "@dMinus";
    }

    static std::string name2eMinus(const std::string &name) {
        return name + "@eMinus";
    }

    void run() {
        sn->run();
    }

    const std::shared_ptr<SemiNaiver> getSN() const {
        return sn;
    }
};


class IncrOverdelete : public IncrementalState {
protected:
    std::string convertRules() const;

public:
    IncrOverdelete(// const
                   ProgramArgs &vm, const std::shared_ptr<SemiNaiver> from,
                   const std::vector<std::string> &eMinus);

    virtual ~IncrOverdelete() {
    }

    std::string confContents() const;
};


class IncrRederive : public IncrementalState {
protected:
    const IncrOverdelete &overdelete;

    std::string convertRules() const;

    static std::string name2dPlus(const std::string &pred) {
        return pred + "@dPlus";
    }

    static std::string name2v(const std::string &pred) {
        return pred + "@v";
    }

public:
    IncrRederive(// const
                 ProgramArgs vm,
                 const std::shared_ptr<SemiNaiver> from,
                 const std::vector<std::string> &eMinus,
                 const IncrOverdelete &overdelete);

    virtual ~IncrRederive() {
    }

    std::string confContents() const;
};


/*
class IncrAdd : public IncrementalState {
};
*/

class DRed {
};

#endif  // def INCREMENTAL__CONCEPTS_H__
