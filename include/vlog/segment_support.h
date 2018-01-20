#include <vlog/column.h>

#include <vector>
#include <memory>

struct CompareRanges {
    const std::vector<std::pair<size_t, std::vector<Term_t>>> &r;
    CompareRanges(const std::vector<std::pair<size_t, std::vector<Term_t>>> &r) : r(r) {
    }

    bool operator()(const int &i, const int &j) const {
        return r[i].first < r[j].first;
    }
};

struct CreateColumns2 {
    const std::vector<size_t> &idxs;
    const Term_t *v1;
    const Term_t *v2;
    std::vector<Term_t> &out1;
    std::vector<Term_t> &out2;

    CreateColumns2(const std::vector<size_t> &idxs,
            const Term_t *v1,
            const Term_t *v2,
            std::vector<Term_t> &out1,
            std::vector<Term_t> &out2) : idxs(idxs), v1(v1), v2(v2),
    out1(out1), out2(out2) {
    }

    void operator()(const ParallelRange& r) const {
        for (size_t i = r.begin(); i != r.end(); ++i) {
            out1[i] = v1[idxs[i]];
            out2[i] = v2[idxs[i]];
        }

        //LOG(WARNL) << "Created columns from " << r.begin() << " " << r.end();
        /*m.lock();
          out1.push_back(std::make_pair(r.begin(), sortedColumnsInserters[0].getColumn()));
          out2.push_back(std::make_pair(r.begin(), sortedColumnsInserters[1].getColumn()));
          m.unlock();*/
    }
};

struct CreateColumns {
    const std::vector<size_t> &idxs;
    const std::vector<const std::vector<Term_t> *> &vectors;
    std::vector<std::vector<Term_t>> &out;

    CreateColumns(const std::vector<size_t> &idxs,
            const std::vector<const std::vector<Term_t> *> &vectors,
            std::vector<std::vector<Term_t>> &out) : idxs(idxs), vectors(vectors), out(out) {
    }

    void operator()(const ParallelRange& r) const {
        for (size_t i = r.begin(); i != r.end(); ++i) {
            for (int j = 0; j < out.size(); j++) {
                out[j][i] = (*vectors[j])[idxs[i]];
            }
        }
    }
};

struct CreateColumnsNoDupl2 {
    const std::vector<size_t> &idxs;
    const Term_t *v1;
    const Term_t *v2;
    std::vector<std::pair<size_t, std::vector<Term_t>>> &ranges1;
    std::vector<std::pair<size_t, std::vector<Term_t>>> &ranges2;
    std::mutex &m;

    CreateColumnsNoDupl2(const std::vector<size_t> &idxs,
            const Term_t *v1,
            const Term_t *v2,
            std::vector<std::pair<size_t, std::vector<Term_t>>> &ranges1,
            std::vector<std::pair<size_t, std::vector<Term_t>>> &ranges2,
            std::mutex &m) :
        idxs(idxs), v1(v1), v2(v2),
        ranges1(ranges1), ranges2(ranges2), m(m) {
        }

    void operator()(const ParallelRange& r) const {
        std::vector<Term_t> out1;
        std::vector<Term_t> out2;

        Term_t prev1 = (Term_t) - 1;
        Term_t prev2 = (Term_t) - 1;
        if (r.begin() > 0) {
            prev1 = v1[idxs[r.begin() - 1]];
            prev2 = v2[idxs[r.begin() - 1]];
        }

        for (size_t i = r.begin(); i != r.end(); ++i) {
            const Term_t value1 =  v1[idxs[i]];
            const Term_t value2 = v2[idxs[i]];
            if (value1 != prev1 || value2 != prev2) {
                out1.push_back(value1);
                out2.push_back(value2);
            }
            prev1 = value1;
            prev2 = value2;
        }

        m.lock();
        ranges1.push_back(std::make_pair(r.begin(), std::vector<Term_t>()));
        ranges2.push_back(std::make_pair(r.begin(), std::vector<Term_t>()));
        ranges1.back().second.swap(out1);
        ranges2.back().second.swap(out2);
        m.unlock();
    }
};

struct CopyPairs {
    const std::vector<Term_t> &v1;
    const std::vector<Term_t> &v2;
    std::vector<std::pair<Term_t, Term_t>> &out;

    CopyPairs(const std::vector<Term_t> &v1,
            const std::vector<Term_t> &v2,
            std::vector<std::pair<Term_t, Term_t>> &out) : v1(v1), v2(v2),
    out(out) {
    }

    void operator()(const ParallelRange& r) const {
        for (size_t i = r.begin(); i != r.end(); ++i) {
            out[i].first = v1[i];
            out[i].second = v2[i];
        }
    }
};

struct InitArray {
    std::vector<size_t> &out;
    InitArray(std::vector<size_t> &out) : out(out) {
    }

    void operator()(const ParallelRange& r) const {
        for (size_t i = r.begin(); i != r.end(); ++i) {
            out[i] = i;
        }
    }
};

struct PairComparator {
    const Term_t *v1;
    const Term_t *v2;

    PairComparator(const Term_t *v1,
            const Term_t *v2)
        : v1(v1), v2(v2) {
        }

    bool operator()(const size_t &x, const size_t &y) const {
        if (v1[x] == v1[y]) {
            return v2[x] < v2[y];
        }
        return v1[x] < v1[y];
    }
};
