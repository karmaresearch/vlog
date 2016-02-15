#include <vlog/materialization.h>
#include <vlog/concepts.h>
#include <vlog/edb.h>
#include <vlog/qsqr.h>

#include <trident/model/table.h>

#include <boost/thread.hpp>

#include <fstream>
#include <string>
#include <unistd.h>
#include <signal.h>

#include <sys/types.h>
#include <sys/wait.h>

namespace timens = boost::chrono;

void Materialization::loadLiteralsFromFile(Program &p, std::string filePath) {
    std::ifstream stream(filePath);
    std::string line;
    while (std::getline(stream, line)) {
        Literal l = p.parseLiteral(line);
        prematerializedLiterals.push_back(l);
    }
    stream.close();
}

bool atomSorterCriterion(const std::pair<Literal, int> *el1,
                         const std::pair<Literal, int> *el2) {
    if (el1->first.getPredicate().getId() !=
            el2->first.getPredicate().getId()) {
        //Choose the one with smaller card
        if (el1->first.getPredicate().getCardinality() !=
                el2->first.getPredicate().getCardinality()) {
            return el1->first.getPredicate().getCardinality() <
                   el2->first.getPredicate().getCardinality();
        } else {
            //Order by ID
            return el1->first.getPredicate().getId() <
                   el2->first.getPredicate().getId();
        }
    } else {
        //Choose first the ones with most constants
        if (el1->first.getNVars() != el2->first.getNVars()) {
            return el1->first.getNVars() < el2->first.getNVars();
        } else {
            //Order by popularity
            return el1->second < el2->second;
        }
    }
}

bool Materialization::cardIsTooLarge(const Literal &lit, Program &p,
                                     EDBLayer &layer) {
    //Search among the rules is there is one without IDB and whose head matches
    //lit
    Substitution subs[10];
    for (auto &rule : p.getAllRules()) {
        if (rule.getNIDBPredicates() == 0 && rule.getBody().size() == 1) {
            int nsubs = Literal::subsumes(subs, rule.getHead(), lit);
            if (nsubs != -1) {
                Literal query = rule.getBody()[0].substitutes(subs, nsubs);
                size_t card = layer.estimateCardinality(query);
                BOOST_LOG_TRIVIAL(debug) << "Card for literal " << query.tostring(NULL, NULL) << " is " << card;
                if (card > 10000) {
                    BOOST_LOG_TRIVIAL(debug) << "Query " << query.tostring(NULL, NULL) << " is ignored";
                    return true;
                }
            }
        }
    }
    return false;
}

void Materialization::guessLiteralsFromRules(Program &p, EDBLayer &layer) {
    //Potential literals
    std::vector<std::pair<Literal, int>> allLiterals;
    Substitution subs[18];

    //Create a list of potential atoms that should be materialized
    std::vector<Rule> rules = p.getAllRules();
    for (auto &rule : rules) {
        const std::vector<Literal> bodyLiterals = rule.getBody();
        if (rule.getNIDBPredicates() > 0) {
            for (auto &literal : bodyLiterals) {
                if (literal.getPredicate().getType() == IDB) {
                    bool isNew = true;
                    for (auto &existingLit : allLiterals) {
                        if (Literal::subsumes(subs, literal,
                                              existingLit.first) != -1 &&
                                literal.getNUniqueVars() ==
                                existingLit.first.getNUniqueVars()) {
                            isNew = false;
                            existingLit.second++;
                            break;
                        }
                    }

                    if (isNew)
                        allLiterals.push_back(make_pair(literal, 1));
                }
            }
        }
    }

    //Initial sorting of the atoms based on heuristics. First we sort on the
    //predicates by cardinality, then by number of constants, then by popularity
    std::vector<std::pair<Literal, int>*> pointers;
    int nIgnoredQueries = 0;
    for (int i = 0; i < allLiterals.size(); ++i) {
        //Is it reasonable to try to answer this query or do I have
        //already hints that the query is too large?
        if (cardIsTooLarge(allLiterals[i].first, p, layer)) {
            nIgnoredQueries++;
        } else {
            pointers.push_back(&allLiterals[i]);
        }
    }
    sort(pointers.begin(), pointers.end(), atomSorterCriterion);

    for (auto &p : pointers) {
        prematerializedLiterals.push_back(p->first);
    }
    repeatPrematerialization = false;
    BOOST_LOG_TRIVIAL(debug) << "Ignored queries because too large: "
                             << nIgnoredQueries;

#ifdef DEBUG
    for (auto &l : pointers) {
        BOOST_LOG_TRIVIAL(debug) << "Literal " <<
                                 l->first.tostring(&p, &layer) <<
                                 " " << l->second;
    }
#endif
}

int *myPipe;

void alrmHandler(int signal) {
    BOOST_LOG_TRIVIAL(debug) << "Got alarm signal";
    // close(myPipe[0]);
    // close(myPipe[1]);
    BOOST_LOG_TRIVIAL(debug) << "Exiting ...";
    exit(1);
}

bool Materialization::evaluateQueryThreadedVersion(EDBLayer *kb,
        Program *p,
        QSQQuery *q,
        TupleTable **output,
        long timeoutSeconds) {
    //Create a pipe between the two processes
    int pipeID[2];
    pipe(pipeID);
    //Fork the process.
    pid_t pid = fork();

    int status;
    if (pid == (pid_t) 0) { //Child
        QSQR qsqr(*kb, p);
        myPipe = &pipeID[0];
        // if (signal(SIGALRM, alrmHandler) == SIG_ERR) {
        // Could not set alarm signal handler
        // exit(1);
        // }
        BOOST_LOG_TRIVIAL(debug) << "Installed alarm signal handler; Now setting alarm over " << timeoutSeconds << " usec";
        ualarm((useconds_t)timeoutSeconds, (useconds_t)timeoutSeconds); //Wait one second
        TupleTable *tmpTable = qsqr.evaluateQuery(QSQR_EVAL, q, NULL, NULL, true);
        //After the query is computed, I don't care anymore of the alarm
        signal(SIGALRM, SIG_IGN);

        //Copy the output to the parent process
        close(pipeID[0]);
        //Serialize TupleTable
        char buffer[24];
        //First write the row size and n of rows
        Utils::encode_int(buffer, 0, tmpTable->getSizeRow());
        Utils::encode_int(buffer, 4, tmpTable->getNRows());
        write(pipeID[1], buffer, 8);

        //Write the bindings, one after the other
        const size_t rowSize = tmpTable->getSizeRow();
        for (size_t i = 0; i < tmpTable->getNRows(); ++i) {
            const uint64_t *row = tmpTable->getRow(i);
            for (size_t j = 0; j < rowSize; ++j) {
                Utils::encode_long(buffer, 8 * j, row[j]);
            }
            write(pipeID[1], buffer, 8 * rowSize);
        }
        close(pipeID[1]);

        exit(EXIT_SUCCESS);
    } else if (pid < (pid_t)0) {
        BOOST_LOG_TRIVIAL(error) << "The fork has failed!";
        exit(1);
    } else { //Parent
        while (waitpid(pid, &status, 0) != pid) {
        }
        close(pipeID[1]);
        //Check the status flag
        if (WIFEXITED(status)) {
            if (WEXITSTATUS(status) == 0) {
                //Query terminated with success
                //Read tuple table
                char buffer[24];
                //Read rowsize and n. of rows
                read(pipeID[0], buffer, 8);
                const size_t rowSize = Utils::decode_int(buffer, 0);
                const size_t nRows = Utils::decode_int(buffer, 4);
                *output = new TupleTable(rowSize);
                uint64_t row[3];
                for (size_t i = 0; i < nRows; ++i) {
                    read(pipeID[0], buffer, 8 * rowSize);
                    for (size_t j = 0; j < rowSize; ++j) {
                        row[j] = Utils::decode_long(buffer, j * 8);
                    }
                    (*output)->addRow(row);
                }
                close(pipeID[0]);
                return true;
            }
            close(pipeID[0]);
            return false;
        } else {
            close(pipeID[0]);
            //Process was terminated
            if (!WIFSIGNALED(status) || (WTERMSIG(status) != SIGALRM)) {
                BOOST_LOG_TRIVIAL(warning) << "The process terminated with a weird return code";
            }
            return false;
        }
    }
    return true;
}

bool Materialization::execMatQuery(Literal &l, bool timeout, EDBLayer &kb,
                                   Program &p, int &predIdx,
                                   long timeoutSeconds) {
    bool failed = false;
    QSQQuery q(l);
    BOOST_LOG_TRIVIAL(debug) << "Getting query " << l.tostring(&p, &kb);
    timens::system_clock::time_point start = timens::system_clock::now();
    TupleTable *output = NULL;

    if (!timeout || timeoutSeconds == 0) {
        QSQR qsqr(kb, &p);
        output = qsqr.evaluateQuery(QSQR_EVAL, &q, NULL, NULL,
                                    true);
    } else {
        failed = !evaluateQueryThreadedVersion(&kb, &p, &q,
                                               &output,
                                               timeoutSeconds);
    }

    boost::chrono::duration<double> sec =
        boost::chrono::system_clock::now() - start;
    if (failed) {
        BOOST_LOG_TRIVIAL(debug) << "Query " << l.tostring(&p, &kb) <<
                                 " failed after " <<
                                 sec.count() * 1000 << " ms";
    } else { // Not failed
        BOOST_LOG_TRIVIAL(debug) << "Got " << output->getNRows() <<
                                 " results in " << sec.count() *
                                 1000 << " ms";
        IndexedTupleTable *idxOutput = new IndexedTupleTable(output);
        VTuple newTuple(l.getNVars());
        int j = 0;
        for (int i = 0; i < l.getTupleSize(); ++i) {
            if (l.getTermAtPos(i).isVariable()) {
                newTuple.set(l.getTermAtPos(i), j);
                j++;
            }
        }
        std::string predName = std::string("TSP") + std::to_string(predIdx)
                               + std::string("E");
        try {
            PredId_t pi = p.getPredicateID(predName, (uint8_t) newTuple.getSize());
            Predicate newPred(pi, 0, EDB, (uint8_t) newTuple.getSize());
            edbLiterals.push_back(Literal(newPred, newTuple));
            predIdx++;

            Predicate pred = edbLiterals.back().getPredicate();
            BOOST_LOG_TRIVIAL(debug) << "Add results to relation " <<
                                     p.getPredicateName(pred.getId());
            kb.addTmpRelation(pred, idxOutput);
            delete output;
        } catch (int v) {
            delete output;
            throw v;
        }

    }
    return failed;
}

void Materialization::getAndStorePrematerialization(EDBLayer & kb, Program & p,
        bool timeout, long timeoutSeconds) {
    int predIdx = 0;
    std::vector<Literal> failedQueries;
    int successQueries = 0;
    // try {
        for (std::vector<Literal>::iterator itr =  prematerializedLiterals.begin();
                itr != prematerializedLiterals.end(); ++itr) {
            bool failed = execMatQuery(*itr, timeout, kb, p, predIdx, timeoutSeconds);
            if (!failed) {
                rewriteLiteralInProgram(*itr, edbLiterals.back(), kb, p);
                successQueries++;
            } else {
                failedQueries.push_back(*itr);
            }
        }

        while (repeatPrematerialization && failedQueries.size() > 0) {
            BOOST_LOG_TRIVIAL(debug) <<
                                     "Try to rerun " << failedQueries.size() <<
                                     " queries";
            bool success = false;
            std::vector<Literal> newFailedQueries;
            for (auto &el : failedQueries) {
                bool failed = execMatQuery(el, timeout, kb, p, predIdx,
                                           timeoutSeconds);
                if (!failed) {
                    rewriteLiteralInProgram(el, edbLiterals.back(), kb, p);
                    success = true;
                } else {
                    newFailedQueries.push_back(el);
                }
            }
            if (!success) {
                break;
            } else {
                failedQueries.clear();
                for (auto &l : newFailedQueries)
                    failedQueries.push_back(l);
            }
        }
        if (timeout) {
            BOOST_LOG_TRIVIAL(debug) << "Failed Queries: " << failedQueries.size()
                                     << " Preprocessed Queries: "
                                     << successQueries;
        }

        for (auto &rule : p.getAllRules()) {
            BOOST_LOG_TRIVIAL(debug) << "Rule: " << rule.tostring(&p, &kb);
        }
    // } catch (int v) {
      //   if (v == OUT_OF_PREDICATES) {
        //     BOOST_LOG_TRIVIAL(debug) << "Aborted prematerialization, out of predicates";
       //  } else {
        //     throw v;
       //  }
   //  }
}

void Materialization::rewriteLiteralInProgram(Literal & prematLiteral, Literal & rewrittenLiteral, EDBLayer & kb, Program & p) {
    std::vector<Rule> rewrittenRules;
    Substitution subs[3];
    for (PredId_t m = 0; m < MAX_NPREDS; ++m) {
        for (int i = 0; i < p.getAllRulesByPredicate(m)->size(); ++i ) {
            Rule r = p.getAllRulesByPredicate(m)->at(i);

            bool toBeAdded = true;

            //If head is precomputed toBeAdded = false
            if (Literal::subsumes(subs, prematLiteral, r.getHead()) != -1) {
                toBeAdded = false;
                continue;
            }

            //Replace the literals in the body
            std::vector<Literal> newBody;
            for (std::vector<Literal>::const_iterator itr1 = r.getBody().begin();
                    itr1 != r.getBody().end(); itr1++) {
                int nsubs = 0;
                bool toRewrite = false;
                nsubs = Literal::subsumes(subs, prematLiteral, *itr1);
                if (nsubs != -1) {
                    toRewrite = true;
                    Predicate pred = rewrittenLiteral.getPredicate();
                    if (kb.isTmpRelationEmpty(pred)) {
                        toBeAdded = false;
                        break;
                    }
                }
                if (!toRewrite) {
                    newBody.push_back(*itr1);
                } else {
                    newBody.push_back(rewrittenLiteral.substitutes(subs, nsubs));
                }
            }

            if (toBeAdded)
                rewrittenRules.push_back(Rule(r.getHead(), newBody));
        }
    }

    for (PredId_t m = 0; m < MAX_NPREDS; ++m) {
        p.getAllRulesByPredicate(m)->clear();
    }

    for (std::vector<Rule>::iterator itr = rewrittenRules.begin(); itr != rewrittenRules.end();
            ++itr) {
        p.getAllRulesByPredicate(itr->getHead().getPredicate().getId())->push_back(*itr);
    }

    //Add all rules that map the new edb relations
    Predicate kbPredicate = kb.getDBPredicate(1);

    Predicate pred = rewrittenLiteral.getPredicate();
    if (!kb.isTmpRelationEmpty(pred) && prematLiteral.getTupleSize() == 3) {

        //I should add this rule only if the are inferred triples in the new relation
        QSQQuery q(Literal(kbPredicate, prematLiteral.getTuple()));
        TupleTable *output = new TupleTable(prematLiteral.getNVars());
        kb.query(&q, output, NULL, NULL);
        bool newTuples = kb.getSizeTmpRelation(pred) > output->getNRows();
        delete output;

        //Add a simple rule that maps the head to the literal
        if (newTuples) {
            std::vector<Literal> body;
            body.push_back(rewrittenLiteral);
            Rule r(prematLiteral, body);
            p.getAllRulesByPredicate(prematLiteral.getPredicate().getId())->push_back(r);
        }
    }
}
