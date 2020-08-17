#include <vlog/ml/ml.h>
#include <vlog/edb.h>

std::string ML::makeGenericQuery(Program& p, PredId_t predId, uint8_t predCard) {
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

std::pair<std::string, int> ML::makeComplexQuery(
        Program& p,
        Literal& l,
        vector<Substitution>& sub, EDBLayer& db) {
    std::string query = p.getPredicateName(l.getPredicate().getId());
    int card = l.getPredicate().getCardinality();
    query += "(";
    QueryType queryType;
    int countConst = 0;
    for (int i = 0; i < card; ++i) {
        std::string canV = "V" + to_string(i+1);
        //FIXME: uint8_t id = p.getIDVar(canV); //I don't know how to convert this line
        uint8_t id = 0;
        bool found = false;
        for (int j = 0; j < sub.size(); ++j) {
            if (sub[j].origin == id) {
                char supportText[MAX_TERM_SIZE];
                db.getDictText(sub[j].destination.getValue(), supportText);
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

PredId_t ML::getMatchingIDB(EDBLayer& db, Program &p, vector<uint64_t>& tuple) {
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
                Var_t tempid = body[0].getTermAtPos(c).getId();
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

std::vector<std::pair<std::string, int>> ML::generateTrainingQueries(EDBLayer &db,
        Program &p,
        std::vector<uint8_t>& vt,
        ProgramArgs &vm
        ) {
    std::unordered_map<string, int> allQueries;

    typedef std::pair<PredId_t, vector<Substitution>> EndpointWithEdge;
    typedef std::unordered_map<uint16_t, std::vector<EndpointWithEdge>> Graph;
    Graph graph;

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

#if DEBUG
    // Try printing graph
    for (auto it = graph.begin(); it != graph.end(); ++it) {
        uint16_t id = it->first;
        std::cout << p.getPredicateName(id) << " : " << std::endl;
        std::vector<EndpointWithEdge> nei = it->second;
        for (int i = 0; i < nei.size(); ++i) {
            Predicate pred = p.getPredicate(nei[i].first);
            std::vector<Substitution> sub = nei[i].second;
            for (int j = 0; j < sub.size(); ++j){
                std::cout << p.getPredicateName(nei[i].first) << "{" << sub[j].origin << "->"
                    << sub[j].destination.getId() << " , " << sub[j].destination.getValue() << "}" << std::endl;
            }
        }
        std::cout << "=====" << std::endl;
    }
#endif

    // Gather all predicates
    std::vector<PredId_t> ids = p.getAllEDBPredicateIds();
    std::ofstream allPredicatesLog("allPredicatesInQueries.log");
    Dictionary dictVariables;
    for (int i = 0; i < ids.size(); ++i) {
        int neighbours = graph[ids[i]].size();
        LOG(INFOL) << p.getPredicateName(ids[i]) << " is EDB : " << neighbours << "neighbours";
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
        uint64_t maxTuples = vm["maxTuples"].as<unsigned int>();
        /**
         * RP1(A,B) :- TE(A, <studies>, B)
         * RP2(A,B) :- TE(A, <worksFor>, B)
         *
         * Tuple <jon, studies, VU> can match with RP2, which it should not
         *
         * All EDB tuples should be carefully matched with rules
         * */
        PredId_t predId = edbPred.getId();
        uint64_t rowNumber = 0;
        if (maxTuples > nRows) {
            maxTuples = nRows;
        }
        while (rowNumber < maxTuples) {
            std::vector<uint64_t> tuple;
            std::string tupleString("<");
            for (int j = 0; j < nVars; ++j) {
                uint64_t value = table->getPosAtRow(rowNumber, j);
                tuple.push_back(value);
                char supportText[MAX_TERM_SIZE];
                db.getDictText(value, supportText);
                tupleString += supportText;
                tupleString += ",";
            }
            tupleString += ">";
            LOG(INFOL) << "Tuple # " << rowNumber << " : " << tupleString;
            PredId_t idbPredId = getMatchingIDB(db, p, tuple);
            if (65535 == idbPredId) {
                rowNumber++;
                continue;
            }
            std::string predName = p.getPredicateName(idbPredId);

            LOG(INFOL) << tupleString << " ==> " << predName << " : " << +idbPredId;
            vector<Substitution> subs;
            for (int k = 0; k < card; ++k) {
                subs.push_back(Substitution(vt[k], VTerm(0, tuple[k])));
            }
            // Find powerset of subs here
            std::vector<std::vector<Substitution>> options =  powerset<Substitution>(subs);
            unsigned int seed = (unsigned int) ((clock() ^ 413711) % 105503);
            srand(seed);
            for (int l = 0; l < options.size(); ++l) {
                int depth = vm["depth"].as<unsigned int>();
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
                    PredId_t qId  = graph[predId][randomNeighbour].first;
                    uint8_t qCard = p.getPredicate(graph[predId][randomNeighbour].first).getCardinality();
                    std::string qQuery = makeGenericQuery(p, qId, qCard);
                    Literal qLiteral = p.parseLiteral(qQuery, dictVariables);
                    allPredicatesLog << p.getPredicateName(qId) << std::endl;
                    std::pair<string, int> finalQueryResult = makeComplexQuery(p, qLiteral, result, db);
                    std::string qFinalQuery = finalQueryResult.first;
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
    } // all EDB predicate ids
    allPredicatesLog.close();
    std::vector<std::pair<std::string,int>> queries;
    for (std::unordered_map<std::string,int>::iterator it = allQueries.begin(); it !=  allQueries.end(); ++it) {
        queries.push_back(std::make_pair(it->first, it->second));
        LOG(INFOL) << "Query: " << it->first << " type : " << it->second ;
    }
    return queries;
}


