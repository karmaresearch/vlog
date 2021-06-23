#include <vlog/ml/training.h>
#include <vlog/utils.h>
#include <csignal>
#include <ctime>
#include <set>
#include <stack>
#include <numeric>
#include <climits>
#include <sys/wait.h>
#include <vlog/ml/helper.h>

std::string makeGenericQuery(Program& p, PredId_t predId, uint8_t predCard) {
    std::string query = p.getPredicateName(predId);
    query += "(";
    for (int i = 0; i < predCard; ++i) {
        query += "V" + to_string(i+1);
        if (i != predCard-1) {
            query += ",";
        }
    }
    query += ")";
    return query;
}

std::pair<std::string, int> makeComplexQuery(Program& p, Literal& l, vector<Substitution>& sub, EDBLayer& db, Dictionary& dictVariables) {
    std::string query = p.getPredicateName(l.getPredicate().getId());
    int card = l.getPredicate().getCardinality();
    query += "(";
    QueryType queryType;
    int countConst = 0;
    for (int i = 0; i < card; ++i) {
        std::string canV = "V" + to_string(i+1);
        uint8_t id = dictVariables.getOrAdd(canV); //I don't know how to convert this line
        bool found = false;
        for (int j = 0; j < sub.size(); ++j) {
            if (sub[j].origin == +id && sub[j].destination.getId() == 0) {
                char supportText[MAX_TERM_SIZE];
                //LOG(INFOL) << "dest value = " << sub[j].destination.getValue();
                db.getDictText(sub[j].destination.getValue(), supportText);
                //LOG(INFOL) << " support text = {" << supportText << "}";
                query += supportText;
                found = true;
                countConst++;
            }
        }
        if (!found) {
            query += canV;
        }
        if (i != card-1) {
            query += ",";
        }
    }
    query += ")";

    if (countConst == card) {
        queryType = QUERY_TYPE_BOOLEAN;
    } else if (countConst == 0) {
        queryType = QUERY_TYPE_GENERIC;
    } else {
        queryType = QUERY_TYPE_MIXED;
    }
    return std::make_pair(query, queryType);
}

template <typename Generic>
std::vector<std::vector<Generic>> powerset(std::vector<Generic>& set) {
    std::vector<std::vector<Generic>> output;
    uint16_t setSize = set.size();
    uint16_t powersetSize = pow((uint16_t)2, setSize) - 1;
    for (int i = 1; i <= powersetSize; ++i) {
        std::vector<Generic> element;
        for (int j = 0; j < setSize; ++j) {
            if (i & (1<<j)) {
                element.push_back(set[j]);
            }
        }
        output.push_back(element);
    }
    return output;
}

PredId_t getMatchingIDB(EDBLayer& db, Program &p, vector<uint64_t>& tuple) {
    //Check this tuple with all rules
    PredId_t idbPredicateId = 65535;
    vector<Rule> rules = p.getAllRules();
    vector<Rule>::iterator it = rules.begin();
    vector<pair<uint8_t, uint64_t>> ruleTuple;
    for (;it != rules.end(); ++it) {
        vector<Literal> body = (*it).getBody();
        if (body.size() > 1) {
            continue;
        }
        uint8_t nConstants = body[0].getNConstants();
        Predicate temp = body[0].getPredicate();
        if (!p.isPredicateIDB(temp.getId())){
            int matched = 0;
            for (int c = 0; c < temp.getCardinality(); ++c) {
                uint8_t tempid = body[0].getTermAtPos(c).getId();
                if(tempid == 0) {
                    uint64_t tempvalue = body[0].getTermAtPos(c).getValue();
                    char supportText[MAX_TERM_SIZE];
                    db.getDictText(tempvalue, supportText);
                    if (tempvalue == tuple[c]) {
                        matched++;
                    }
                }
            }
            if (matched == nConstants) {
                idbPredicateId = (*it).getFirstHead().getPredicate().getId();
                return idbPredicateId;
            }
        }
    }
    return idbPredicateId;
}

bool isSimilar(string& query1, string& query2, EDBLayer& layer) {
    vector<string> tokens1 = split(query1, '(');
    vector<string> tokens2 = split(query2, '(');

    string predicate1 = tokens1[0];
    string predicate2 = tokens2[0];
    if (predicate1 != predicate2) {
        return false;
    }
    string tuple1 = tokens1[1];
    string tuple2 = tokens2[1];
    // Remove trailing closing paranthesis (')')
    tuple1.pop_back();
    tuple2.pop_back();
    vector<string> terms1 = split(tuple1, ',');
    vector<string> terms2 = split(tuple2, ',');
    if (terms1.size() != terms2.size()) {
        return false;
    }
    for (int i = 0; i < terms1.size(); ++i){
        uint64_t value1;
        bool isConstant1 = layer.getDictNumber((char*) terms1[i].c_str(), terms1[i].size(), value1);
        uint64_t value2;
        bool isConstant2 = layer.getDictNumber((char*) terms2[i].c_str(), terms2[i].size(), value2);
        if (!isConstant1 && isConstant2) {
            return false;
        }
    }
    return true;
}

int foundSubsumingQuery(string& testQuery,
    vector<pair<string, int>>& trainingQueriesAndResult,
    Program& p,
    EDBLayer& layer) {

    Dictionary dictVariables;
    for (auto qr: trainingQueriesAndResult) {
        // If the test query has occurred in the training, then return the result of that query
        if (testQuery == qr.first) {
            return qr.second;
        }

        // If the test query is similar to the training query, then return the result of that query
        if (isSimilar(testQuery, qr.first, layer)){
            return qr.second;
        }
    }
    return -1;
}

string printSubstitutions(vector<Substitution>& subs, EDBLayer& db) {
    string result = "{";
    for (int i = 0; i < subs.size(); ++i) {
        string temp = to_string(+subs[i].origin);
        temp += "=>";
        uint8_t destId = subs[i].destination.getId();
        if (destId == 0) {
            char supportText[MAX_TERM_SIZE];
            db.getDictText(subs[i].destination.getValue(), supportText);
            temp += supportText;
        } else {
            temp += to_string(destId);
        }
        result += temp;
        if (i != subs.size()-1) {
            result += ",";
        }
    }
    result += "}";
    return result;
}

void getAllPaths(uint16_t source, vector<Edge>& path, vector<vector<Edge>>& paths, int& maxDepth, DepGraph& graph, Program& p) {
    if (!p.isPredicateIDB(source)) {
        paths.push_back(path);
    } else if (paths.size() >= maxDepth) {
        return;
    } else {
        for (auto e : graph[source]) {
            vector<Edge> newPath (path);
            newPath.push_back(e);
            getAllPaths(e.endpoint.first, newPath, paths, maxDepth, graph, p);
        }
    }
}
std::vector<std::pair<std::string, int>> Training::generateTrainingQueriesAllPaths(EDBConf &conf,
        EDBLayer &db,
        Program &p,
        int depth,
        uint64_t maxTuples,
        std::vector<uint8_t>& vt
        ) {
    std::unordered_map<string, int> allQueries;
    std::set<string> setOfUniquePredicates;
    std::unordered_map<string, vector<pair<string, int>>> queryMap;

    DepGraph graph;

    std::vector<Rule> rules = p.getAllRules();
    for (int i = 0; i < rules.size(); ++i) {
        Rule ri = rules[i];
        Predicate ph = ri.getFirstHead().getPredicate();
        std::vector<Substitution> sigmaH;
        for (int j = 0; j < ph.getCardinality(); ++j) {
            VTerm dest = ri.getFirstHead().getTuple().get(j);
            sigmaH.push_back(Substitution(vt[j], dest));
        }
        std::vector<Literal> body = ri.getBody();
        for (std::vector<Literal>::const_iterator itr = body.begin(); itr != body.end(); ++itr) {
            Predicate pb = itr->getPredicate();
            std::vector<Substitution> sigmaB;
            for (int j = 0; j < pb.getCardinality(); ++j) {
                VTerm dest = itr->getTuple().get(j);
                sigmaB.push_back(Substitution(vt[j], dest));
            }

            Edge edge;
            if (!p.isPredicateIDB(pb.getId())) {
                edge.backEdge = sigmaB;
            }

            // Calculate sigmaH * sigmaB
            std::vector<Substitution> edge_label = inverse_concat(sigmaB, sigmaH);
            edge.endpoint = std::make_pair(pb.getId(), edge_label);
            graph[ph.getId()].push_back(edge);
        }
    }

    std::vector<PredId_t> ids = p.getAllIDBPredicateIds();
    vector<string> genericQueriesVector;
    std::ofstream allPredicatesLog("allPredicatesInQueries.log");
    Dictionary dictVariables;
    for (int i = 0; i < ids.size(); ++i) {
        vector<Edge> pathTemp;
        vector<vector<Edge>> paths;
        getAllPaths(ids[i], pathTemp, paths, depth, graph, p);

        int neighbours = graph[ids[i]].size();
        LOG(DEBUGL) << i+1 << ") " <<p.getPredicateName(ids[i]) << " is IDB  with " << neighbours << " neighbours";
        LOG(INFOL) << i+1 << ") " <<p.getPredicateName(ids[i]) << " is IDB  with " << paths.size() << " paths";

        for (auto ap : paths) {
            stringstream pathString;
            pathString.str("");
            for (auto node: ap) {
                pathString << p.getPredicateName(node.endpoint.first) << "=> ";
            }
            LOG(DEBUGL) << pathString.str();
        }

        Predicate idbPred = p.getPredicate(ids[i]);

        // This generic query could be added to list/set of all queries we generate
        std::string query = makeGenericQuery(p, idbPred.getId(), idbPred.getCardinality());
        genericQueriesVector.push_back(query);
        Literal literal = p.parseLiteral(query, dictVariables);
        int nVars = literal.getNVars();

        PredId_t predId = idbPred.getId();
        int n = 1;
        for(auto path: paths) {
            bool reachedEDB = false;
            for (auto edge: path){
                PredId_t qId = edge.endpoint.first;
                vector<Substitution> sigmaN = edge.endpoint.second;
                vector<Substitution> backEdge = edge.backEdge;

                predId = qId;
                n++;
                if (!p.isPredicateIDB(predId)) {
                    reachedEDB = true;
                    break;
                }
            } // for each edge of the path

            string pathlog = p.getPredicateName(idbPred.getId());
            pathlog += "=>";
            int pathLength = 0;
            for(auto node : path) {
                string substi = printSubstitutions(node.endpoint.second, db);
                string nodestr = p.getPredicateName(node.endpoint.first);
                pathlog += nodestr + "," + substi;
                if (pathLength != path.size()-1)
                    pathlog += "=>";
                pathLength++;
            }
            LOG(DEBUGL) << pathlog;
            vector<vector<Substitution>> tuplesSubstitution;
            if (reachedEDB == true) {
                // Construct the query to find EDB tuples
                Predicate edbPred = p.getPredicate(predId);
                vector<Substitution> subLiteral;
                //LOG(INFOL) << "last node in path : " << p.getPredicateName(path.back().endpoint.first);
                subLiteral = path.back().backEdge;
                string workingQuery = makeGenericQuery(p, edbPred.getId(), edbPred.getCardinality());
                Literal workingLiteral = p.parseLiteral(workingQuery, dictVariables);
                pair<string, int> queryAndType = makeComplexQuery(p, workingLiteral, subLiteral, db, dictVariables);
                LOG(DEBUGL) << ":: complex Query : " << queryAndType.first << " working query =  " << workingQuery ;
                Literal literal = p.parseLiteral(queryAndType.first, dictVariables);
                int nVars = literal.getNVars();

                EDBIterator* table = db.getIterator(literal);

                // Pop the EDB predicate id and substitution from the top
                vector<Substitution> sigmaH = path.back().endpoint.second;
                PredId_t nextNode;
                if (path.size() == 1) {
                    nextNode = idbPred.getId();
                } else {
                    nextNode = path[path.size()-2].endpoint.first;
                }
                int nextNodeCard = p.getPredicate(nextNode).getCardinality();
                // This nextNode must be an IDB predicate
                // We will check whether this predicate appeared as the head of any rule
                // such that the body contained only one atom (with EDB predicate)
                uint64_t rowNumber = 0;
                int count = 0;
                while (table->hasNext()) {
                    table->next();
                    count++;
                    if (count > maxTuples) {
                        break;
                    }
                    string strTuple = "";
                    vector<Substitution> subs;
                    for (int j = 0; j < edbPred.getCardinality(); ++j) {
                        Term_t term = table->getElementAt(j);
                        subs.push_back(Substitution(vt[j], VTerm(0, term)));
                        char supportText[MAX_TERM_SIZE];
                        db.getDictText(term, supportText);
                        strTuple += supportText;
                    }
                    tuplesSubstitution.push_back(subs);
                }

                for (auto ts: tuplesSubstitution) {
                    vector<Substitution> workingSigma = ts;
                    PredId_t workingIDB;
                    int workingIDBCard;
                    for(int k = path.size()-1; k >= 0; --k) {
                        vector<Substitution> sigmaH = path[k].endpoint.second;
                        vector<Substitution> result = concat(sigmaH, workingSigma);
                        if (k != 0) {
                            workingIDB = path[k-1].endpoint.first;
                            workingIDBCard = p.getPredicate(workingIDB).getCardinality();
                        } else {
                            workingIDB = idbPred.getId();
                            workingIDBCard = idbPred.getCardinality();
                        }
                        allPredicatesLog<< p.getPredicateName(workingIDB) << std::endl;

                        string qQuery = makeGenericQuery(p, workingIDB, workingIDBCard);
                        setOfUniquePredicates.insert(p.getPredicateName(workingIDB));
                        Literal qLiteral = p.parseLiteral(qQuery, dictVariables);
                        pair<string,int> finalQueryResult = makeComplexQuery(p, qLiteral, result, db, dictVariables);
                        int type = finalQueryResult.second + ((n > 4) ? 4 : n);
                        if (allQueries.find(finalQueryResult.first) == allQueries.end()) {
                            string key = p.getPredicateName(workingIDB);
                            queryMap[key].push_back(make_pair(finalQueryResult.first, type));
                            LOG(DEBUGL) << ":: " << finalQueryResult.first << " added";
                            allQueries.insert(make_pair(finalQueryResult.first,type));
                        }
                        workingSigma = result;
                    }
                }
                delete table;
            }
        } // for all paths are traversed

    }//For all IDB predicates
    allPredicatesLog.close();
    LOG(DEBUGL) << " # of unique predicate = "<< setOfUniquePredicates.size();
    std::vector<std::pair<std::string,int>> queries;
    for (auto it = allQueries.begin(); it !=  allQueries.end(); ++it) {
        queries.push_back(std::make_pair(it->first, it->second));
        LOG(INFOL) << "Query: " << it->first << " type : " << it->second ;
    }
    int nQueriesPerPredicate = 20;

    for (auto it = queryMap.begin(); it !=  queryMap.end(); ++it) {
        //LOG(INFOL)<< it->first << " : ";
        int countPerPredicate = 0;
        for (auto q : it->second) {
            //LOG(INFOL) << q.first << ", " << q.second;
            queries.push_back(make_pair(q.first, q.second));
            if (++countPerPredicate > nQueriesPerPredicate) {
                break;
            }
        }
    }

    // Uncomment this code to add generic queries explicitly
    for (auto gq: genericQueriesVector) {
        queries.push_back(make_pair(gq, QUERY_TYPE_GENERIC + 1));
    }
    return queries;
}

std::vector<std::pair<std::string, int>> Training::generateNewTrainingQueries(EDBConf &conf,
        EDBLayer &db,
        Program &p,
        int depth,
        uint64_t maxTuples,
        std::vector<uint8_t>& vt
        ) {
    std::unordered_map<string, int> allQueries;
    std::set<string> setOfUniquePredicates;
    std::unordered_map<string, vector<pair<string, int>>> queryMap;

    DepGraph graph;

    std::vector<Rule> rules = p.getAllRules();
    for (int i = 0; i < rules.size(); ++i) {
        Rule ri = rules[i];
        Predicate ph = ri.getFirstHead().getPredicate();
        std::vector<Substitution> sigmaH;
        for (int j = 0; j < ph.getCardinality(); ++j) {
            VTerm dest = ri.getFirstHead().getTuple().get(j);
            sigmaH.push_back(Substitution(vt[j], dest));
        }
        std::vector<Literal> body = ri.getBody();
        for (std::vector<Literal>::const_iterator itr = body.begin(); itr != body.end(); ++itr) {
            Predicate pb = itr->getPredicate();
            std::vector<Substitution> sigmaB;
            for (int j = 0; j < pb.getCardinality(); ++j) {
                VTerm dest = itr->getTuple().get(j);
                sigmaB.push_back(Substitution(vt[j], dest));
            }

            Edge edge;
            if (!p.isPredicateIDB(pb.getId())) {
                edge.backEdge = sigmaB;
            }

            // Calculate sigmaH * sigmaB
            std::vector<Substitution> edge_label = inverse_concat(sigmaB, sigmaH);
            edge.endpoint = std::make_pair(pb.getId(), edge_label);
            graph[ph.getId()].push_back(edge);
        }
    }

    // Try printing graph
    //for (auto it = graph.begin(); it != graph.end(); ++it) {
    //    uint16_t id = it->first;
    //    LOG(INFOL) << p.getPredicateName(id) << " : ";
    //        std::vector<Edge> nei = it->second;
    //        for (int i = 0; i < nei.size(); ++i) {
    //            Predicate pred = p.getPredicate(nei[i].endpoint.first);
    //            std::vector<Substitution> sub = nei[i].endpoint.second;
    //            string atomStr = p.getPredicateName(nei[i].endpoint.first) + " : ";
    //            atomStr += printSubstitutions(nei[i].endpoint.second, db);
    //            LOG(INFOL) << atomStr;
    //            if (nei[i].backEdge.size() != 0) {
    //                LOG(INFOL) << "%%%%%%%%%%%%%%%%%%%%%%%%%%";
    //                LOG(INFOL) << printSubstitutions(nei[i].backEdge, db);
    //            }
    //        }
    //    LOG(INFOL) << "=====";
    //}
    //std::vector<std::pair<std::string,int>> queries2;
    //return queries2;

    std::vector<PredId_t> ids = p.getAllIDBPredicateIds();
    vector<string> genericQueriesVector;
    std::ofstream allPredicatesLog("allPredicatesInQueries.log");
    Dictionary dictVariables;
    for (int i = 0; i < ids.size(); ++i) {
        /*
        For every IDB predicate, we will start from its node in the dependency graph
        and try to traverse randomly so that we generate queries that induce reasoning.
        */

        int neighbours = graph[ids[i]].size();
        LOG(DEBUGL) << i+1 << ") " <<p.getPredicateName(ids[i]) << " is IDB  with " << neighbours << " neighbours";

        Predicate idbPred = p.getPredicate(ids[i]);

        // This generic query could be added to list/set of all queries we generate
        std::string query = makeGenericQuery(p, idbPred.getId(), idbPred.getCardinality());
        genericQueriesVector.push_back(query);
        Literal literal = p.parseLiteral(query, dictVariables);
        int nVars = literal.getNVars();

        PredId_t predId = idbPred.getId();
        bool reachedEDB = false;
        int n = 1;
        //srand(time(0));
        //vector<pair<PredId_t, vector<Substitution>>> path;
        vector<Edge> path;
        while (n != depth+1) {
            uint32_t nNeighbours = graph[predId].size();
            if (0 == nNeighbours) {
                break;
            }
            uint32_t randomNeighbour = -1;
            if (n == depth) {
                // Try to find out the EDB node neighbour
                int index = 0;
                for(auto neighbour : graph[predId]) {
                    if (!p.isPredicateIDB(neighbour.endpoint.first)) {
                        randomNeighbour = index;
                    }
                    index++;
                }
            }
            if (randomNeighbour == -1) {
                srand(time(0) + (n + i));
                randomNeighbour = rand() % nNeighbours;
            }
            PredId_t qId = graph[predId][randomNeighbour].endpoint.first;
            vector<Substitution> sigmaN = graph[predId][randomNeighbour].endpoint.second;
            vector<Substitution> backEdge = graph[predId][randomNeighbour].backEdge;
            Edge e;
            e.endpoint = make_pair(qId, sigmaN);
            e.backEdge = backEdge;
            path.push_back(e);

            predId = qId;
            n++;
            if (!p.isPredicateIDB(predId)) {
                reachedEDB = true;
                break;
            }

        } // while the depth of exploration is reached

        string pathlog = p.getPredicateName(idbPred.getId());
        pathlog += "=>";
        int pathLength = 0;
        for(auto node : path) {
            string substi = printSubstitutions(node.endpoint.second, db);
            string nodestr = p.getPredicateName(node.endpoint.first);
            pathlog += nodestr + "," + substi;
            if (pathLength != path.size()-1)
                pathlog += "=>";
            pathLength++;
        }
        LOG(DEBUGL) << pathlog;
        vector<vector<Substitution>> tuplesSubstitution;
        if (reachedEDB == true) {
            // Construct the query to find EDB tuples
            Predicate edbPred = p.getPredicate(predId);
            vector<Substitution> subLiteral;
            //LOG(INFOL) << "last node in path : " << p.getPredicateName(path.back().endpoint.first);
            subLiteral = path.back().backEdge;
            string workingQuery = makeGenericQuery(p, edbPred.getId(), edbPred.getCardinality());
            Literal workingLiteral = p.parseLiteral(workingQuery, dictVariables);
            pair<string, int> queryAndType = makeComplexQuery(p, workingLiteral, subLiteral, db, dictVariables);
            LOG(DEBUGL) << ":: complex Query : " << queryAndType.first;
            Literal literal = p.parseLiteral(queryAndType.first, dictVariables);
            int nVars = literal.getNVars();

            EDBIterator* table = db.getIterator(literal);

            // Pop the EDB predicate id and substitution from the top
            vector<Substitution> sigmaH = path.back().endpoint.second;
            PredId_t nextNode;
            if (path.size() == 1) {
                nextNode = idbPred.getId();
            } else {
                nextNode = path[path.size()-2].endpoint.first;
            }
            int nextNodeCard = p.getPredicate(nextNode).getCardinality();
            // This nextNode must be an IDB predicate
            // We will check whether this predicate appeared as the head of any rule
            // such that the body contained only one atom (with EDB predicate)
            uint64_t rowNumber = 0;
            int count = 0;
            while (table->hasNext()) {
                table->next();
                count++;
                if (count > maxTuples) {
                    break;
                }
                string strTuple = "";
                vector<Substitution> subs;
                for (int j = 0; j < edbPred.getCardinality(); ++j) {
                    Term_t term = table->getElementAt(j);
                    subs.push_back(Substitution(vt[j], VTerm(0, term)));
                    char supportText[MAX_TERM_SIZE];
                    db.getDictText(term, supportText);
                    strTuple += supportText;
                }
                tuplesSubstitution.push_back(subs);
            }

            for (auto ts: tuplesSubstitution) {
                vector<Substitution> workingSigma = ts;
                PredId_t workingIDB;
                int workingIDBCard;
                for(int k = path.size()-1; k >= 0; --k) {
                    vector<Substitution> sigmaH = path[k].endpoint.second;
                    vector<Substitution> result = concat(sigmaH, workingSigma);
                    if (k != 0) {
                        workingIDB = path[k-1].endpoint.first;
                        workingIDBCard = p.getPredicate(workingIDB).getCardinality();
                    } else {
                        workingIDB = idbPred.getId();
                        workingIDBCard = idbPred.getCardinality();
                    }
                    string qQuery = makeGenericQuery(p, workingIDB, workingIDBCard);
                    setOfUniquePredicates.insert(p.getPredicateName(workingIDB));
                    Literal qLiteral = p.parseLiteral(qQuery, dictVariables);
                    pair<string,int> finalQueryResult = makeComplexQuery(p, qLiteral, result, db, dictVariables);
                    int type = finalQueryResult.second + ((n > 4) ? 4 : n);
                    if (allQueries.find(finalQueryResult.first) == allQueries.end()) {
                        string key = p.getPredicateName(workingIDB);
                        queryMap[key].push_back(make_pair(finalQueryResult.first, type));
                        LOG(DEBUGL) << ":: " << finalQueryResult.first << " added";
                        allQueries.insert(make_pair(finalQueryResult.first,type));
                    }
                    workingSigma = result;
                }
            }
            delete table;
        }

    }//For all IDB predicates
    allPredicatesLog.close();
    LOG(DEBUGL) << " # of unique predicate = "<< setOfUniquePredicates.size();
    std::vector<std::pair<std::string,int>> queries;
    //for (auto it = allQueries.begin(); it !=  allQueries.end(); ++it) {
    //    queries.push_back(std::make_pair(it->first, it->second));
        //LOG(INFOL) << "Query: " << it->first << " type : " << it->second ;
    //}
    int nQueriesPerPredicate = 20;

    for (auto it = queryMap.begin(); it !=  queryMap.end(); ++it) {
        //LOG(INFOL)<< it->first << " : ";
        int countPerPredicate = 0;
        for (auto q : it->second) {
            //LOG(INFOL) << q.first << ", " << q.second;
            queries.push_back(make_pair(q.first, q.second));
            if (++countPerPredicate > nQueriesPerPredicate) {
                break;
            }
        }
    }

    // Uncomment this code to add generic queries explicitly
    for (auto gq: genericQueriesVector) {
        queries.push_back(make_pair(gq, QUERY_TYPE_GENERIC + 1));
    }
    return queries;
}

std::vector<std::pair<std::string, int>> Training::generateTrainingQueries(EDBConf &conf,
        EDBLayer &db,
        Program &p,
        int depth,
        uint64_t maxTuples,
        std::vector<uint8_t>& vt
        ) {
    std::unordered_map<string, int> allQueries;
    std::set<string> setOfUniquePredicates;

    typedef pair<PredId_t, vector<Substitution>> EndpointWithEdge;
    typedef unordered_map<uint16_t, vector<EndpointWithEdge>> DepGraph;
    DepGraph graph;

    std::vector<Rule> rules = p.getAllRules();
    for (int i = 0; i < rules.size(); ++i) {
        Rule ri = rules[i];
        Predicate ph = ri.getFirstHead().getPredicate();
        std::vector<Substitution> sigmaH;
        for (int j = 0; j < ph.getCardinality(); ++j) {
            VTerm dest = ri.getFirstHead().getTuple().get(j);
            sigmaH.push_back(Substitution(vt[j], dest));
        }
        std::vector<Literal> body = ri.getBody();
        for (std::vector<Literal>::const_iterator itr = body.begin(); itr != body.end(); ++itr) {
            Predicate pb = itr->getPredicate();
            std::vector<Substitution> sigmaB;
            for (int j = 0; j < pb.getCardinality(); ++j) {
                VTerm dest = itr->getTuple().get(j);
                sigmaB.push_back(Substitution(vt[j], dest));
            }
            // Calculate sigmaB * sigmaH
            std::vector<Substitution> edge_label = inverse_concat(sigmaB, sigmaH);
            EndpointWithEdge neighbour = std::make_pair(ph.getId(), edge_label);
            graph[pb.getId()].push_back(neighbour);
        }
    }

    // Try printing graph
    for (auto it = graph.begin(); it != graph.end(); ++it) {
        uint16_t id = it->first;
        LOG(DEBUGL) << p.getPredicateName(id) << " : ";
        std::vector<EndpointWithEdge> nei = it->second;
        for (int i = 0; i < nei.size(); ++i) {
            Predicate pred = p.getPredicate(nei[i].first);
            std::vector<Substitution> sub = nei[i].second;
            for (int j = 0; j < sub.size(); ++j){
                LOG(DEBUGL) << p.getPredicateName(nei[i].first) << "{" << sub[j].origin << "->"
                    << sub[j].destination.getId() << " , " << sub[j].destination.getValue() << "}";
            }
        }
        LOG(DEBUGL) << "=====";
    }

    // Gather all predicates
    std::vector<PredId_t> ids = p.getAllEDBPredicateIds();
    std::ofstream allPredicatesLog("allPredicatesInQueries.log");
    Dictionary dictVariables;
    for (int i = 0; i < ids.size(); ++i) {
        int neighbours = graph[ids[i]].size();
        LOG(DEBUGL) << p.getPredicateName(ids[i]) << " is EDB : " << neighbours << "neighbours";
        Predicate edbPred = p.getPredicate(ids[i]);
        int card = edbPred.getCardinality();
        std::string query = makeGenericQuery(p, edbPred.getId(), edbPred.getCardinality());
        Literal literal = p.parseLiteral(query, dictVariables);
        int nVars = literal.getNVars();
        QSQQuery qsqQuery(literal);
        TupleTable *table = new TupleTable(nVars);
        db.query(&qsqQuery, table, NULL, NULL);
        uint64_t nRows = table->getNRows();
        std::vector<std::vector<uint64_t>> output;
        /**
         * RP1(A,B) :- TE(A, <studies>, B)
         * RP2(A,B) :- TE(A, <worksFor>, B)
         *
         * Tuple <jon, studies, VU> can match with RP2 (because TE has RP1 and RP2 both as neighbours)
         * , but it should not match
         *
         * All EDB tuples should be carefully matched with rules
         * */
        PredId_t predId = edbPred.getId();
        vector<int> tupleIndexes(maxTuples);
        getRandomTupleIndexes(maxTuples, nRows, tupleIndexes);

        uint64_t rowNumber = 0;
        if (maxTuples > nRows) {
            maxTuples = nRows;
        }
        while (rowNumber < maxTuples) {
            std::vector<uint64_t> tuple;
            std::string tupleString("<");
            for (int j = 0; j < nVars; ++j) {
                uint64_t value = table->getPosAtRow(tupleIndexes[rowNumber], j);
                tuple.push_back(value);
                char supportText[MAX_TERM_SIZE];
                db.getDictText(value, supportText);
                tupleString += supportText;
                tupleString += ",";
            }
            tupleString += ">";
            PredId_t idbPredId = getMatchingIDB(db, p, tuple);
            if (65535 == idbPredId) {
                rowNumber++;
                continue;
            }
            std::string predName = p.getPredicateName(idbPredId);

            //LOG(INFOL) << "Matched : " << tupleString << " ==> " << predName << " : " << +idbPredId;
            vector<Substitution> subs;
            for (int k = 0; k < card; ++k) {
                subs.push_back(Substitution(vt[k], VTerm(0, tuple[k])));
            }
            // Find powerset of subs here
            std::vector<std::vector<Substitution>> options =  powerset<Substitution>(subs);
            //unsigned int seed = (unsigned int) ((clock() ^ 413711) % 105503);
            srand(time(0));
            for (int l = 0; l < options.size(); ++l) {
                vector<Substitution> sigma = options[l];
                PredId_t predId = edbPred.getId();
                int n = 1;
                while (n != depth+1) {
                    uint32_t nNeighbours = graph[predId].size();
                    if (0 == nNeighbours) {
                        break;
                    }
                    uint32_t randomNeighbour;
                    if (1 == n) {
                        int index = 0;
                        bool found = false;
                        for (auto it = graph[predId].begin(); it != graph[predId].end(); ++it,++index) {
                            if (it->first == idbPredId) {
                                randomNeighbour = index;
                                found = true;
                                break;
                            }
                        }
                        assert(found == true);
                    } else {
                        randomNeighbour = rand() % nNeighbours;
                    }
                    std::vector<Substitution>sigmaN = graph[predId][randomNeighbour].second;
                    std::vector<Substitution> result = concat(sigmaN, sigma);
                    //LOG(INFOL) << "Sub:" << printSubstitutions(result, db);
                    PredId_t qId  = graph[predId][randomNeighbour].first;
                    uint8_t qCard = p.getPredicate(graph[predId][randomNeighbour].first).getCardinality();
                    std::string qQuery = makeGenericQuery(p, qId, qCard);
                    //LOG(INFOL) << " Generic query made: " << qQuery;
                    Literal qLiteral = p.parseLiteral(qQuery, dictVariables);
                    setOfUniquePredicates.insert(p.getPredicateName(qId));
                    allPredicatesLog << p.getPredicateName(qId) << std::endl;
                    std::pair<string, int> finalQueryResult = makeComplexQuery(p, qLiteral, result, db, dictVariables);
                    std::string qFinalQuery = finalQueryResult.first;
                    //LOG(INFOL) << "complex query made  " << finalQueryResult.first;
                    int type = finalQueryResult.second + ((n > 4) ? 4 : n);
                    if (allQueries.find(qFinalQuery) == allQueries.end()) {
                        allQueries.insert(std::make_pair(qFinalQuery, type));
                    }

                    predId = qId;
                    sigma = result;
                    n++;
                } // while the depth of exploration is reached
            } // for each partial substitution
            rowNumber++;
        }

        delete table;
    } // all EDB predicate ids
    allPredicatesLog.close();
    LOG(DEBUGL) << " # of unique predicate = "<< setOfUniquePredicates.size();
    std::vector<std::pair<std::string,int>> queries;
    for (std::unordered_map<std::string,int>::iterator it = allQueries.begin(); it !=  allQueries.end(); ++it) {
        queries.push_back(std::make_pair(it->first, it->second));
        //LOG(INFOL) << "Query: " << it->first << " type : " << it->second ;
    }
    return queries;
}
pid_t pid;
bool timedOut;
void alarmHandler(int signalNumber) {
    if (signalNumber == SIGALRM) {
        kill(pid, SIGKILL);
        timedOut = true;
    }
}

double Training::runAlgo(string& algo,
        Reasoner& reasoner,
        EDBLayer& edb,
        Program& p,
        Literal& literal,
        stringstream& ss,
        uint64_t timeoutMillis) {

    int ret;

    std::chrono::duration<double> durationQuery;
    signal(SIGALRM, alarmHandler);
    timedOut = false;

    double* queryTime = (double*) mmap(NULL, sizeof(double), PROT_READ|PROT_WRITE, MAP_SHARED|MAP_ANONYMOUS, -1, 0);

    pid = fork();
    if (pid < 0) {
        LOG(ERRORL) << "Could not fork";
        return 0.0L;
    } else if (pid == 0) {
        //Child work begins
        //+
        std::chrono::system_clock::time_point queryStartTime = std::chrono::system_clock::now();
        bool printResults = false;
        int nVars = literal.getNVars();
        bool onlyVars = nVars > 0;
        TupleIterator *iter;
        if (algo == "magic"){
            iter = reasoner.getMagicIterator(literal, NULL, NULL, edb, p, onlyVars, NULL);
        } else if (algo == "qsqr") {
            iter = reasoner.getTopDownIterator(literal, NULL, NULL, edb, p, onlyVars, NULL);
        } else {
            LOG(ERRORL) << "Algorithm not supported : " << algo;
            return 0;
        }
        long count = 0;
        int sz = iter->getTupleSize();
        if (nVars == 0) {
            ss << (iter->hasNext() ? "TRUE" : "FALSE") << endl;
            count = (iter->hasNext() ? 1 : 0);
        } else {
            while (iter->hasNext()) {
                iter->next();
                count++;
                if (printResults) {
                    for (int i = 0; i < sz; i++) {
                        char supportText[MAX_TERM_SIZE];
                        uint64_t value = iter->getElementAt(i);
                        if (i != 0) {
                            ss << " ";
                        }
                        if (!edb.getDictText(value, supportText)) {
                            LOG(ERRORL) << "Term " << value << " not found";
                        } else {
                            ss << supportText;
                        }
                    }
                    ss << endl;
                }
            }
        }
        std::chrono::system_clock::time_point queryEndTime = std::chrono::system_clock::now();
        durationQuery = queryEndTime - queryStartTime;
        *queryTime = durationQuery.count()*1000;
        exit(0);
        //-
        //Child work ends
    } else {
        uint64_t l =  timeoutMillis / 1000;
        alarm(l);
        int status;
        ret = waitpid(pid, &status, 0);
        alarm(0);
        if (timedOut) {
            LOG(DEBUGL) << "TIMED OUT";
            munmap(queryTime, sizeof(double));
            return timeoutMillis;
        }
        double time = *queryTime;
        munmap(queryTime, sizeof(double));
        return time;
    }
}


void parseQueriesLog(vector<string>& testQueriesLog,
        vector<string>& testQueries,
        vector<Metrics>& testFeaturesVector,
        vector<int>& expectedDecisions) {

    for (auto line: testQueriesLog) {
        // A line looks like this (An underscore (_) represents a space)
        // query_fe,a,t,u,r,es_QSQTime_MagicTime_Decision
        //RP29(<http://www.Department4.University60.edu/FullProfessor5>,B) 4.000000,4,1,1,2,0 1.290000 2.069000 1
        // some queries can contain spaces, commas. To address these, we split tokens by scanning the string from the end
        //RP1052(A,"(A)National Security (B) Public Accounts(C)Rules,Business & Privliges (D) Foreign Affairs (E) Kashmir"@en)
        vector<string> tokens;
        tokens = rsplit(line); // by default rsplit gives 5 tokens from the back
        assert (tokens.size() == 5);
        vector<string> features = split(tokens[1], ',');
        testQueries.push_back(tokens[0]);
        Metrics metrics;
        metrics.cost = stod(features[0]);
        metrics.estimate = stoul(features[1]);
        metrics.countRules = stoi(features[2]);
        metrics.countUniqueRules = stoi(features[3]);
        metrics.countIntermediateQueries = stoi(features[4]);
        testFeaturesVector.push_back(metrics);
        expectedDecisions.push_back(stoi(tokens[4]));
    }
}

double computeAccuracy(vector<Instance>& testInst, LogisticRegression& lr, double threshold, vector<int>& heuristics, bool useHeuristics) {

    int hit = 0;
    int totalQsqr = 0;
    int totalMagic = 0;
    int hitQsqr = 0;
    int hitMagic = 0;
    for (int i = 0; i < testInst.size(); ++i){
        int myDecision = 0;
        double probability = lr.classify(testInst[i].x);
        if (probability > threshold) {
            myDecision = 1;
        }

        if (testInst[i].label == 0) {
            totalQsqr++;
        } else {
            totalMagic++;
        }
        if (myDecision == testInst[i].label) {
            if (myDecision == 0) {
                hitQsqr++;
            } else {
                hitMagic++;
            }
            hit++;
        } else {
            // miss
            if(testInst[i].label == 1) {
                if (useHeuristics) {
                    if (heuristics[i] != -1) {
                        myDecision = heuristics[i];
                    }
                }
                if (myDecision == testInst[i].label) {
                    hitMagic++;
                } /*else {
                    LOG(INFOL) << "Probability= "<< probability;
                }*/
            }
        }
    }
    double accuracy = (double)hit/(double)testInst.size();
    double qsqrAccuracy = (double)hitQsqr / (double) totalQsqr;
    double magicAccuracy = (double)hitMagic / (double) totalMagic;
    LOG(INFOL) << "Overall Accuracy : " << accuracy;
    LOG(INFOL) << "Probability Threshold : " << threshold;
    LOG(INFOL) << "QSQR Accuracy : " << hitQsqr << " / " << totalQsqr << " = " <<  (double)hitQsqr/(double)totalQsqr;
    LOG(INFOL) << "Magic Accuracy : " << hitMagic << " / " << totalMagic << " = " <<  (double)hitMagic/(double)totalMagic;
    LOG(INFOL) << "================================================================";
    return accuracy;
}

void Training::runQueries(vector<string>& trainingQueriesVector,
        EDBLayer& edb,
        Program& p,
        uint64_t timeout,
        uint8_t repeatQuery,
        vector<Metrics>& featuresVector,
        vector<int>& decisionVector,
        vector<double>& featuresTimesVector,
        int& nMagicQueries,
        string& logFileName,
        int featureDepth) {

    vector<string> strResults;
    vector<string> strFeatures;
    vector<string> strQsqrTime;
    vector<string> strMagicTime;
    ofstream logTraining(logFileName);
    int i = 1;
    vector<string> workingQueries = trainingQueriesVector;
    vector<uint64_t> timeouts = {10000,60000,300000};
    for (auto time : timeouts) {
        LOG(INFOL) << "For timeout = " << time << " dealing with " << workingQueries.size() << " queries";
        vector<string> timedOutQueries;
        for (auto q : workingQueries) {
            LOG(INFOL) << i++ << ") " << q;
            // Execute the literal query
            string results="";
            string features="";
            string qsqrTime="";
            string magicTime="";
            Training::execLiteralQuery(q,
                    edb,
                    p,
                    results,
                    features,
                    qsqrTime,
                    magicTime,
                    timeout,
                    repeatQuery,
                    featureDepth,
                    featuresVector,
                    decisionVector,
                    featuresTimesVector);
            strResults.push_back(results);
            features += ",0";
            strFeatures.push_back(features);
            strQsqrTime.push_back(qsqrTime);
            logTraining << q <<" " << features << " " << qsqrTime << " " << magicTime << " " << decisionVector.back() << endl;
            LOG(INFOL) << q <<" " << features << " " << qsqrTime << " " << magicTime << " " << decisionVector.back();
            if (stoull(qsqrTime) == time && stoull(magicTime) == time) {
                LOG(INFOL) << "Query timed out : " << q;
                timedOutQueries.push_back(q);
            }
            strMagicTime.push_back(magicTime);
            if (decisionVector.back() == 1) {
                nMagicQueries++;
            }
        }
        workingQueries = timedOutQueries;
    }

    if (logTraining.fail()) {
        LOG(ERRORL) << "Error writing to file";
    }
    logTraining.close();
}

void Training::trainAndTestModel(vector<string>& trainingQueriesVector,
        vector<string>& testQueriesLog,
        EDBLayer& edb,
        Program& p,
        double& accuracy,
        uint64_t timeout,
        uint8_t repeatQuery,
        string& logFileName,
        int featureDepth) {

    vector<Metrics> featuresVector;
    vector<int> decisionVector;
    vector<double> featuresTimesVector;
    int nMagicQueries = 0;
    Training::runQueries(trainingQueriesVector, edb, p, timeout, repeatQuery, featuresVector, decisionVector, featuresTimesVector, nMagicQueries, logFileName, featureDepth);

    double totalFeaturesTime = accumulate(featuresTimesVector.begin(), featuresTimesVector.end(), 0.0);
    LOG(INFOL) << "Total time to generate features : " << totalFeaturesTime;
    //TODO: do not test in cpp
    return;
    vector<Metrics> balancedFeaturesVector;
    vector<int> balancedDecisionVector;
    vector<string> balancedTrainingQueriesVector;
    int nQsqrQueries = 0;
    for (int i = 0; i < featuresVector.size(); ++i) {
        if (decisionVector[i] == 0) {
            if (nQsqrQueries < nMagicQueries) {
                balancedFeaturesVector.push_back(featuresVector[i]);
                balancedDecisionVector.push_back(decisionVector[i]);
                balancedTrainingQueriesVector.push_back(trainingQueriesVector[i]);
                nQsqrQueries++;
            }
        } else {
            balancedFeaturesVector.push_back(featuresVector[i]);
            balancedDecisionVector.push_back(decisionVector[i]);
            balancedTrainingQueriesVector.push_back(trainingQueriesVector[i]);
        }
    }

    if(balancedFeaturesVector.size() != (2 * nMagicQueries)) {
        LOG(INFOL) << "More magic queries than QSQR!!";
    }

    vector<pair<string, int>> trainingQueriesAndResult;

    bool use5050 = false;
    vector<Metrics>* ptrFeatures = NULL;
    vector<int>* ptrDecisions = NULL;
    vector<string>* ptrQueries = NULL;
    if (use5050){
        ptrFeatures = &balancedFeaturesVector;
        ptrDecisions = &balancedDecisionVector;
        ptrQueries = &balancedTrainingQueriesVector;
    } else {
        ptrFeatures = &featuresVector;
        ptrDecisions = &decisionVector;
        ptrQueries = &trainingQueriesVector;
    }

    int trainingQsqr = 0;
    int trainingMagic = 0;
    vector<Instance> instances;
    for (int i = 0; i < ptrFeatures->size(); ++i) {
        vector<double> features;
        features.push_back((*ptrFeatures)[i].cost);
        features.push_back((*ptrFeatures)[i].estimate);
        features.push_back((*ptrFeatures)[i].countRules);
        features.push_back((*ptrFeatures)[i].countUniqueRules);
        features.push_back((*ptrFeatures)[i].countIntermediateQueries);
        int label = (*ptrDecisions)[i];
        if (label == 0) {
            trainingQsqr++;
        } else {
            trainingMagic++;
            trainingQueriesAndResult.push_back(std::make_pair((*ptrQueries)[i], label));
        }
        Instance instance(label, features);
        instances.push_back(instance);
    }


    // Normalization
    for (int i =0; i < instances.size(); ++i) {
        instances[i].x[0] = std::log1p(instances[i].x[0]);
        instances[i].x[1] = std::log1p(instances[i].x[1]);
        instances[i].x[2] = std::log1p(instances[i].x[2]);
        instances[i].x[3] = std::log1p(instances[i].x[3]);
        instances[i].x[4] = std::log1p(instances[i].x[4]);
    }
    int totalTraining = instances.size();
    double qsqrPercent = ((double)trainingQsqr / (double)totalTraining);//*100;
    LogisticRegression lr(5);
    lr.train(instances);

    //ofstream logCorrectlyGuessedMagic("correctly-guessed-magic.log");
    vector<Metrics> testMetrics;
    vector<string> testQueries;
    vector<int> testDecisions;
    vector<int> heuristicDecisions;
    parseQueriesLog(testQueriesLog, testQueries, testMetrics, testDecisions);
    LOG(INFOL) << "# Test queries = " << testMetrics.size();
    vector<Instance> testInst;
    for (int i = 0; i < testMetrics.size(); ++i) {
        vector<double> features;
        features.push_back(testMetrics[i].cost);
        features.push_back(testMetrics[i].estimate);
        features.push_back(testMetrics[i].countRules);
        features.push_back(testMetrics[i].countUniqueRules);
        features.push_back(testMetrics[i].countIntermediateQueries);

        int heurLabel = foundSubsumingQuery(testQueries[i], trainingQueriesAndResult, p, edb);
        heuristicDecisions.push_back(heurLabel);
        int label = testDecisions[i];
        Instance instance(label, features);
        testInst.push_back(instance);
    }
    for (int i =0; i < testInst.size(); ++i) {
        testInst[i].x[0] = std::log1p(testInst[i].x[0]);
        testInst[i].x[1] = std::log1p(testInst[i].x[1]);
        testInst[i].x[2] = std::log1p(testInst[i].x[2]);
        testInst[i].x[3] = std::log1p(testInst[i].x[3]);
        testInst[i].x[4] = std::log1p(testInst[i].x[4]);
    }

    vector<double> probabilities = {0.1, 0.2, 0.3, 0.4, 0.45, 0.5, 0.55, 0.6, 0.7, 0.8, 0.9, qsqrPercent};
    accuracy = 0;
    double effectiveProb = 0.0;
    for (auto prob : probabilities) {
        double acc = computeAccuracy(testInst, lr, prob, heuristicDecisions, false);
        if (acc > accuracy) {
            accuracy = acc;
            effectiveProb = prob;
        }
    }

    vector<double> weights;
    lr.getWeights(weights);
    LOG(INFOL) << "*****************************************";
    for (int i = 0; i < weights.size(); ++i) {
        LOG(INFOL) << weights[i];
    }
    LOG(INFOL) << "*****************************************";
    //if (logCorrectlyGuessedMagic.fail()) {
    //    LOG(ERRORL) << "Error writing to file";
    //}
    //logCorrectlyGuessedMagic.close();

    LOG(INFOL) << "QSQR favouring Training Queries = " << trainingQsqr;
    LOG(INFOL) << "Magic favouring Training Queries = " << trainingMagic;
}

void Training::execLiteralQueries(vector<string>& queryVector,
        EDBLayer& edb,
        Program& p,
        JSON* jsonResults,
        JSON* jsonFeatures,
        JSON* jsonQsqrTime,
        JSON* jsonMagicTime,
        uint64_t timeout,
        uint8_t repeatQuery) {

    vector<Metrics> featuresVector;
    vector<int> decisionVector;
    vector<double> featuresTimesVector;
    int i = 1;
    vector<string> strResults;
    vector<string> strFeatures;
    vector<string> strQsqrTime;
    vector<string> strMagicTime;
    vector<string> workingQueries = queryVector;
    ofstream logFile("queries-execution.log");
    vector<uint64_t> timeouts = {10000,60000,300000};
    int featureDepth = 5;
    for (auto time : timeouts) {
        LOG(INFOL) << "For timeout = " << time << " dealing with " << workingQueries.size() << " queries";
        vector<string> timedOutQueries;
        for (auto q : workingQueries) {
            LOG(INFOL) << i++ << ") " << q;
            // Execute the literal query
            string results="";
            string features="";
            string qsqrTime="";
            string magicTime="";
            Training::execLiteralQuery(q,
                    edb,
                    p,
                    results,
                    features,
                    qsqrTime,
                    magicTime,
                    time,
                    repeatQuery,
                    featureDepth,
                    featuresVector,
                    decisionVector,
                    featuresTimesVector);
            strResults.push_back(results);
            features += ",0";
            strFeatures.push_back(features);
            strQsqrTime.push_back(qsqrTime);
            strMagicTime.push_back(magicTime);
            logFile << q <<" " << features << " " << qsqrTime << " " << magicTime << " " << decisionVector.back() << endl;
            if (stoull(qsqrTime) == time && stoull(magicTime) == time) {
                LOG(INFOL) << "Query timed out : " << q;
                timedOutQueries.push_back(q);
            }
        }
        workingQueries = timedOutQueries;
    }
    if (logFile.fail()) {
        LOG(ERRORL) << "Error writing to the log file";
    }
    logFile.close();
    string allResults = stringJoin(strResults);
    jsonResults->put("results", allResults);
    string allFeatures = stringJoin(strFeatures);
    jsonFeatures->put("features", allFeatures);
    string allQsqrTimes = stringJoin(strQsqrTime);
    jsonQsqrTime->put("qsqrtimes", allQsqrTimes);
    string allMagicTimes = stringJoin(strMagicTime);
    jsonMagicTime->put("magictimes", allMagicTimes);
}

void Training::execLiteralQuery(string& literalquery,
        EDBLayer& edb,
        Program& p,
        string& strResults,
        string& strFeatures,
        string& strQsqrTime,
        string& strMagicTime,
        uint64_t timeout,
        uint8_t repeatQuery,
        int featureDepth,
        vector<Metrics>& featuresVector,
        vector<int>& decisionVector,
        vector<double>& featuresTimesVector) {

    Dictionary dictVariables;
    Literal literal = p.parseLiteral(literalquery, dictVariables);
    Reasoner reasoner(1000000);

    Metrics metrics;
    string idbFeatures;
    std::chrono::duration<double> durationMetrics;
    std::chrono::system_clock::time_point startMetrics = std::chrono::system_clock::now();
    reasoner.getMetrics(literal, NULL, NULL, edb, p, metrics, featureDepth, idbFeatures);
    std::chrono::system_clock::time_point endMetrics = std::chrono::system_clock::now();

    durationMetrics = endMetrics - startMetrics;
    featuresTimesVector.push_back(durationMetrics.count() * 1000);

    featuresVector.push_back(metrics);
    stringstream strMetrics;
    strMetrics  << std::to_string(metrics.cost) << ","
        << std::to_string(metrics.estimate) << ","
        << std::to_string(metrics.countRules) << ","
        << std::to_string(metrics.countUniqueRules) << ","
        << std::to_string(metrics.countIntermediateQueries);
    strFeatures += strMetrics.str();

    string algo="";
    uint8_t reps = 1;
    vector<double> qsqrTimes;
    vector<double> magicTimes;
    stringstream ssMagic;
    stringstream ssQsqr;
    while (reps <= repeatQuery) {
        algo = "magic";
        double durationMagic = Training::runAlgo(algo, reasoner, edb, p, literal, ssMagic, timeout);
        magicTimes.push_back(durationMagic);

        algo = "qsqr";
        double durationQsqr = Training::runAlgo(algo, reasoner, edb, p, literal, ssQsqr, timeout);
        qsqrTimes.push_back(durationQsqr);
        LOG(INFOL) << "Repetition " << reps << ") " << "Magic time = " << durationMagic << " , QSQR time = " << durationQsqr;
        reps++;
        if (durationMagic == timeout || durationQsqr == timeout) {
            break;
        }
    }

    double avgQsqrTime = accumulate(qsqrTimes.begin(), qsqrTimes.end(), 0.0)/qsqrTimes.size();
    double avgMagicTime = accumulate(magicTimes.begin(), magicTimes.end(), 0.0)/magicTimes.size();
    LOG(INFOL) << "Qsqr time : " << avgQsqrTime;
    LOG(INFOL) << "magic time: " << avgMagicTime;
    int winner = 0; // 0 is for QSQR
    if (avgMagicTime <= avgQsqrTime) {
        winner = 1;
    }
    decisionVector.push_back(winner);
    strResults += ssQsqr.str();
    strQsqrTime += to_string(avgQsqrTime);
    strMagicTime += to_string(avgMagicTime);
}
