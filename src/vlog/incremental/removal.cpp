#include <vlog/incremental/removal.h>

// #include <climits>

#include <vlog/edb.h>
#if 0
#include <vlog/concepts.h>
#include <vlog/idxtupletable.h>
#include <vlog/column.h>
#endif

std::vector<uint8_t> dummy;

// Around a non-sorted Iterator
EDBRemovalIterator::EDBRemovalIterator(const Literal &query,
                                       const EDBRemoveLiterals &removeTuples,
                                       EDBIterator *itr) :
        query(query), fields(dummy),
        removeTuples(removeTuples), itr(itr),
        expectNext(false) {
    t_iterate = new HiResTimer("RemovalIterator " + query.tostring());
    t_iterate->start();
    // arity = query.getTuple().getSize();
    adornment = 0;
    current_term.resize(query.getTuple().getSize());
    term_ahead.resize(query.getTuple().getSize(), 0);
}

// Around a SortedIterator
EDBRemovalIterator::EDBRemovalIterator(const Literal &query,
                                       const std::vector<uint8_t> &fields,
                                       const EDBRemoveLiterals &removeTuples,
                                       EDBIterator *itr) :
                query(query), fields(fields), removeTuples(removeTuples),
                itr(itr), expectNext(false) {
    t_iterate = new HiResTimer("RemovalIterator/Sorted " + query.tostring());
    t_iterate->start();
    Predicate pred = query.getPredicate();
    VTuple tuple = query.getTuple();
    adornment = pred.calculateAdornment(tuple);
    // arity = tuple.getSize() - pred.getNFields(adornment);
    // arity = fields.size();
    term_ahead.resize(tuple.getSize(), 0);
    current_term.resize(tuple.getSize(), 0);
    for (int i = 0; i < tuple.getSize(); ++i) {
        if (adornment & (0x1 << i)) {
            term_ahead[i] = tuple.get(i).getValue();
            current_term[i] = tuple.get(i).getValue();
        }
    }
}


bool EDBRemovalIterator::hasNext() {
    if (expectNext) {
        return hasNext_ahead;
    }

    hasNext_ahead = false;
    while (true) {
        if (! itr->hasNext()) {
            return false;
        }
        itr->next();
        if (adornment == 0) {
            // fast path
            for (int i = 0; i < term_ahead.size(); ++i) {
                term_ahead[i] = itr->getElementAt(i);
            }
        } else {
            // optimize: don't call for constant elements
            for (int i = 0; i < term_ahead.size(); ++i) {
                if (! (adornment & (0x1 << i))) {
                    term_ahead[i] = itr->getElementAt(i);
                    LOG(TRACEL) << "removal: set elt[" << i << "] to " << term_ahead[i];
                }
            }
        }
        if (! removeTuples.present(term_ahead)) {
            break;
        }
        LOG(DEBUGL) << "***** OK: skip one row";
    }

    hasNext_ahead = true;
    expectNext = true;
    return hasNext_ahead;
}

void EDBRemovalIterator::next() {
    if (! expectNext) {
        (void)hasNext();
    }
    expectNext = false;
    hasNext_ahead = false;
    current_term.swap(term_ahead);

    ++ticks;
}


EDBRemoveLiterals::EDBRemoveLiterals(const std::string &file, EDBLayer *layer) :
        layer(layer), num_rows(0) {
    std::ifstream infile(file);
    std::string token;
    std::vector<Term_t> terms;
    PredId_t pred;
    while (infile >> token) {
        LOG(DEBUGL) << "Read token '" << token << "'";
        uint64_t val;
        if (token == ".") {
            insert(terms);
            terms.clear();
        } else {
            layer->getOrAddDictNumber(token.c_str(), token.size(), val);
            terms.push_back(val);
        }
    }
}

// Looks up the table in layer
EDBRemoveLiterals::EDBRemoveLiterals(PredId_t predid, EDBLayer *layer) :
        layer(layer), num_rows(0) {
    const std::shared_ptr<EDBTable> table = layer->getEDBTable(predid);
    uint8_t arity = table->getArity();
    Predicate pred(predid, 0, EDB, arity);
    VTuple t = VTuple(arity);
    for (uint8_t i = 0; i < t.getSize(); ++i) {
        t.set(VTerm(i + 1, 0), i);
    }
    Literal lit(pred, t);

    EDBIterator *itr = layer->getIterator(lit);
    std::vector<Term_t> terms(arity);
    while (itr->hasNext()) {
        itr->next();
        for (uint8_t m = 0; m < arity; ++m) {
            terms[m] = itr->getElementAt(m);
        }
        insert(terms);
    }
    layer->releaseIterator(itr);

    // dump(std::cerr, *layer);
}


void EDBRemoveLiterals::insert(const std::vector<Term_t> &terms) {
    EDBRemoveItem *m = &removed;
    for (size_t i = 0; i < terms.size(); ++i) {
        if (m->has.find(terms[i]) == m->has.end()) {
            EDBRemoveItem *next = new EDBRemoveItem();
            m->has[terms[i]] = next;
        }
        m = m->has[terms[i]];
    }
    ++num_rows;
}

static void dump_map(std::ostream &os,
                     const std::unordered_map<Term_t, EDBRemoveItem *> &m) {
    for (auto &r : m) {
        os << "    " << r.first << " -> " << r.second << std::endl;
    }
}

bool EDBRemoveLiterals::present(const std::vector<Term_t> &terms) const {
    const EDBRemoveItem *m = &removed;
    for (auto i = 0; i < terms.size(); ++i) {
        const auto f = m->has.find(terms[i]);
        if (f == m->has.end()) {
            return false;
        }
        m = f->second;
    }

#ifdef DEBUG
    std::ostringstream os;
    os << "Hit row in removed: ";
    os << "[";
    for (auto t : terms) {
        os << t << " ";
    }
    os << "]";
    LOG(DEBUGL) << os.str();
#endif

    return true;
}


std::ostream &EDBRemoveLiterals::dump_recursive_name(
        std::ostream &of,
        const EDBLayer &layer,
        const EDBRemoveItem *item,
        std::string prefix) const {
    if (item->has.empty()) {
        of << prefix << std::endl;
        return of;
    }

    for (auto rm = item->has.begin(); rm != item->has.end(); ++rm) {
        char name[1024];
        layer.getDictText(rm->first, name);
        dump_recursive_name(of, layer, rm->second, prefix + name + ",");
    }

    return of;
}


std::ostream &EDBRemoveLiterals::dump_recursive(
        std::ostream &of,
        const EDBRemoveItem *item,
        std::string prefix) const {
    if (item->has.empty()) {
        of << prefix << std::endl;
        return of;
    }

    for (auto rm = item->has.begin(); rm != item->has.end(); ++rm) {
        dump_recursive(of, rm->second,
                       prefix + std::to_string(rm->first) + ",");
    }

    return of;
}

std::ostream &EDBRemoveLiterals::dump(
        std::ostream &of,
        const EDBLayer &layer) const {
    dump_recursive(of, &removed, std::string());
    dump_recursive_name(of, layer, &removed, std::string());

    return of;
}
