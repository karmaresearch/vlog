#include <boost/chrono.hpp>
#include <boost/foreach.hpp>

#include <trident/tests/timings.h>

#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>

using namespace std;

void Timings::launchTests() {

    //statistics
    boost::chrono::duration<double> durationsC[6 * 5];
    boost::chrono::duration<double> durationsW[6 * 5];
    std::vector<double> medianC[6 * 5];
    std::vector<double> medianW[6 * 5];
    long nqueries[6 * 5];
    long totalResults[6 * 5];
    for (int i = 0; i < 6 * 5; ++i) {
        durationsC[i] =  boost::chrono::duration<double>::zero();
        durationsW[i] =  boost::chrono::duration<double>::zero();
        nqueries[i] = 0;
        totalResults[i] = 0;
    }

    long junk = 0;
    //end statistics

    //Init the DB
    init();

    ifstream infile(queryfile);
    string line;
    long succ = 0;
    while (std::getline(infile, line)) {
        //Parse the line
        std::stringstream ls(line);
        long tokens[6];
        int idx = 0;
        long temp;
        while (ls >> temp) {
            tokens[idx++] = temp;
        }

        //Launch the query
        const int perm = tokens[0];
        const long nElements = tokens[4];
        const int type = tokens[5];
        long s, p, o;
        switch (perm) {
        case T_SPO:
            s = tokens[1];
            p = tokens[2];
            o = tokens[3];
            break;
        case T_SOP:
            s = tokens[1];
            p = tokens[3];
            o = tokens[2];
            break;
        case T_POS:
            s = tokens[3];
            p = tokens[1];
            o = tokens[2];
            break;
        case T_PSO:
            s = tokens[2];
            p = tokens[1];
            o = tokens[3];
            break;
        case T_OSP:
            s = tokens[2];
            p = tokens[3];
            o = tokens[1];
            break;
        case T_OPS:
            s = tokens[3];
            p = tokens[2];
            o = tokens[1];
            break;
        default:
            throw 10;
        }
        int countIgnores = 0;
        int countVars = 0;
        if (s < 0) {
            countVars++;
            if (s == -2)
                countIgnores++;
            s = -1;
        }
        if (p < 0) {
            countVars++;
            if (p == -2)
                countIgnores++;
            p = -1;
        }
        if (o < 0) {
            countVars++;
            if (o == -2)
                countIgnores++;
            o = -1;
        }

        //launch the query
        boost::chrono::duration<double> durC;
        boost::chrono::duration<double> durW;

        long c = 0;
        //Cold run
        durC = launchQuery(perm, s, p, o, countIgnores, c, junk);
        //Warm run
        c = 0;
        durW = launchQuery(perm, s, p, o, countIgnores, c, junk);

        int subperm = perm * 5 + type;

        if (countVars > 0) {
            durationsC[subperm] += durC;
            durationsW[subperm] += durW;
            medianC[subperm].push_back(durC.count() * 1000);
            medianW[subperm].push_back(durW.count() * 1000);
        }
        nqueries[subperm]++;
        totalResults[subperm] += nElements;

        if (c != nElements) {
            cout << "Perm=" << perm << " type: " << type << " " << s << " " << p << " " << o << " " <<
                 c << " " << nElements << " " << countIgnores << " succ=" << succ << endl;
            throw 10;
        }
        succ++;
    }
    infile.close();

    //Print statistics

    cout << "PERM,TYPE,NQUERIES,AVGRESULTS,AVGC,AVGW,MEDC,MEDW" << endl;
    for (int i = 0; i < 6 * 5; ++i) {
//double time = durations[i].count() * 1000;
//        cout << "Permutation" << i / 5 << " type=" << i % 5 << " nqueries=" << nqueries[i] << " totalTime(sec)=" << time / 1000 << " avg(ms)=" << (time / nqueries[i]) << endl;

        sort(medianC[i].begin(), medianC[i].end());
        sort(medianW[i].begin(), medianW[i].end());
        double medC, medW;
        if (medianC[i].size() % 2 == 1) {
            medC = medianC[i][medianC[i].size() / 2];
        } else {
            medC = medianC[i][medianC[i].size() / 2] +
                medianC[i][medianC[i].size() / 2 - 1];
            medC = medC / 2;
        }
        if (medianW[i].size() % 2 == 1) {
            medW = medianW[i][medianW[i].size() / 2];
        } else {
            medW = medianW[i][medianW[i].size() / 2] +
                medianW[i][medianW[i].size() / 2 - 1];
            medW = medW / 2;
        }

        cout << i / 5 << "," << i % 5 << "," << nqueries[i] << "," <<
             totalResults[i] / nqueries[i] << "," <<
             durationsC[i].count() * 1000 / nqueries[i] <<
             "," << durationsW[i].count() * 1000 / nqueries[i] << ","
             << medC << "," << medW << endl;
    }


    cerr << "Junkvalue(used_to_avoid_compiler_opt)" << junk << endl;
}
