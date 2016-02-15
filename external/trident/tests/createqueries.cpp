#include <trident/tests/common.h>
#include <trident/kb/kb.h>
#include <trident/kb/querier.h>
#include <tridentcompr/utils/lz4io.h>

#include <boost/chrono.hpp>

void _test_createqueries(string inputfile, string queryfile) {

    // Load the entire KB in main memory
    std::vector<_Triple> triples;
    LZ4Reader reader(inputfile);
    while (!reader.isEof()) {
        long s = reader.parseVLong();
        long p = reader.parseVLong();
        long o = reader.parseVLong();
        triples.push_back(_Triple(s, p, o));
        if (triples.size() > 1) {
            if (triples[triples.size() - 1].s < triples[triples.size() - 2].s)
                throw 10;
        }
    }

    //Sort the triples and launch the queries
    ofstream ofile(queryfile);
    for (int perm = 0; perm < 6; ++perm) {
        BOOST_LOG_TRIVIAL(info) << "Testing permutation " << perm;
        if (perm != IDX_SPO) {
            //Sort the vector
            if (perm == IDX_SOP) {
                std::sort(triples.begin(), triples.end(), _less_sop);
            } else if (perm == IDX_POS) {
                std::sort(triples.begin(), triples.end(), _less_pos);
            } else if (perm == IDX_PSO) {
                std::sort(triples.begin(), triples.end(), _less_pso);
            } else if (perm == IDX_OSP) {
                std::sort(triples.begin(), triples.end(), _less_osp);
            } else if (perm == IDX_OPS) {
                std::sort(triples.begin(), triples.end(), _less_ops);
            } else {
                throw 10;
            }
        }

        BOOST_LOG_TRIVIAL(info) << "Scan query...";
        ofile << perm << " " << -1 << " " << -1 << " " << -1 << " " << triples.size() << " " << 0 << endl;
        long prevEl = -1;
        long countDistinct = 0;
        for (auto const el : triples) {
            long first;
            switch (perm) {
            case IDX_SPO:
                first = el.s;
                break;
            case IDX_SOP:
                first = el.s;
                break;
            case IDX_POS:
                first = el.p;
                break;
            case IDX_PSO:
                first = el.p;
                break;
            case IDX_OSP:
                first = el.o;
                break;
            case IDX_OPS:
                first = el.o;
                break;
            }
            if (first != prevEl) {
                prevEl = first;
                countDistinct++;
            }
        }
        ofile << perm << " " << -1 << " " << -2 << " " << -2 << " " << countDistinct << " " << 1 << endl;

        //Test a scan without the second and third columns
        BOOST_LOG_TRIVIAL(info) << "Create detailed queries...";
        long currentFirst = -1;
        long currentSecond = -1;

        long countFirst1 = 0;
        long countFirst2 = 0;
        long countSecond = 0;
        for (auto const el : triples) {
            long first, second;
            switch (perm) {
            case IDX_SPO:
                first = el.s;
                second = el.p;
                break;
            case IDX_SOP:
                first = el.s;
                second = el.o;
                break;
            case IDX_POS:
                first = el.p;
                second = el.o;
                break;
            case IDX_PSO:
                first = el.p;
                second = el.s;
                break;
            case IDX_OSP:
                first = el.o;
                second = el.s;
                break;
            case IDX_OPS:
                first = el.o;
                second = el.p;
                break;
            }


            if (first != currentFirst) {
                if (currentFirst != -1) {
                    ofile << perm << " " << currentFirst << " " << -1 << " " << -1 << " " << countFirst1 << " " << 2 << endl;
                    ofile << perm << " " << currentFirst << " " << -1 << " " << -2 << " " << countFirst2 << " " << 3 << endl;
                    ofile << perm << " " << currentFirst << " " << currentSecond << " " << -1 << " " << countSecond << " " << 4 << endl;
                }
                currentFirst = first;
                currentSecond = second;
                countFirst1 = 1;
                countFirst2 = 1;
                countSecond = 1;
            } else if (second != currentSecond) {
                if (currentSecond != -1) {
                    ofile << perm << " " << currentFirst << " " << currentSecond << " " << -1 << " " << countSecond << " " << 4 << endl;
                    countSecond = 0;
                }
                currentSecond = second;
                countFirst1++;
                countFirst2++;
                countSecond++;
            } else {
                countFirst1++;
                countSecond++;
            }
        }
        if (currentFirst != -1) {
            ofile << perm << " " << currentFirst << " " << -1 << " " << -1 << " " << countFirst1 << " " << 2 << endl;
            ofile << perm << " " << currentFirst << " " << -1 << " " << -2 << " " << countFirst2 << " " << 3 << endl;
            ofile << perm << " " << currentFirst << " " << currentSecond << " " << -1 << " " << countSecond << " " << 4 << endl;
        }
    }
    ofile.close();
}
