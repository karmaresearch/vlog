#include <vlog/resultjoinproc.h>
#include <vlog/ruleexecdetails.h>

ExistentialRuleProcessor::ExistentialRuleProcessor(
        std::vector<std::pair<uint8_t, uint8_t>> &posFromFirst,
        std::vector<std::pair<uint8_t, uint8_t>> &posFromSecond,
        std::vector<FCBlock> &listDerivations,
        FCTable *t,
        Literal &head,
        const RuleExecutionDetails *detailsRule,
        const uint8_t ruleExecOrder,
        const size_t iteration,
        const bool addToEndTable,
        const int nthreads,
        std::shared_ptr<ChaseMgmt> chaseMgmt) :
    FinalRuleProcessor(posFromFirst, posFromSecond, listDerivations,
            t, head, detailsRule, ruleExecOrder, iteration,
            addToEndTable, nthreads), chaseMgmt(chaseMgmt) {
    }

void ExistentialRuleProcessor::processResults(const int blockid,
        const bool unique, std::mutex *m) {
    //TODO: Chase...
    FinalRuleProcessor::processResults(blockid, unique, m);
}

void ExistentialRuleProcessor::addColumns(const int blockid,
        FCInternalTableItr *itr, const bool unique,
        const bool sorted, const bool lastInsert) {
    enlargeBuffers(blockid + 1);

    if (tmpt[blockid] == NULL) {
        tmpt[blockid] = new SegmentInserter(rowsize);
    }

    uint8_t columns[128];
    for (uint32_t i = 0; i < nCopyFromSecond; ++i) {
        columns[i] = posFromSecond[i].second;
    }
    std::vector<std::shared_ptr<Column>> c =
        itr->getColumn(nCopyFromSecond, columns);
    assert(c.size() <= rowsize);

    if (nCopyFromSecond > 1) { //Rearrange the columns depending on the order
        //in the head
        std::vector<std::shared_ptr<Column>> c2;
        int rowID = 0;
        int largest = -1;
        for (int i = 0; i < nCopyFromSecond; ++i) {
            //Get the row with the smallest ID
            int minID = INT_MAX;
            for (int j = 0; j <  nCopyFromSecond; ++j) {
                if (posFromSecond[j].first > largest &&
                        posFromSecond[j].first < minID) {
                    rowID = j;
                    minID = posFromSecond[j].first;
                }
            }
            c2.push_back(c[rowID]);
            largest = posFromSecond[rowID].first;
        }
        assert(c2.size() == c.size());
        c = c2;
    }

    if (c.size() < rowsize) {
        //The head contains also constants. We must add fields in the vector
        // of created columns.
        std::vector<std::shared_ptr<Column>> newc;
        int idxVar = 0;
        for (int i = 0; i < rowsize; ++i) {
            auto t = literal.getTermAtPos(i);
            if (!t.isVariable()) {
                newc.push_back(std::shared_ptr<Column>(
                            new CompressedColumn(row[i],
                                c[0]->size())));
            } else {
                //Does the variable appear in the body? Then copy it
                if (idxVar < nCopyFromSecond &&
                        posFromSecond[idxVar].first == i) {
                    newc.push_back(c[idxVar++]);
                } else {
                    //Existential variable
                    uint64_t sizecolumn = 0;
                    if (c.size() > 0) {
                        sizecolumn = c[0]->size();
                    }
                    newc.push_back(
                            chaseMgmt->getNewOrExistingIDs(
                                ruleDetails->rule.getId(),
                                t.getId(),
                                c,
                                sizecolumn));
                    //newc.push_back(std::shared_ptr<Column>
                    //        (new FunctionalColumn(chaseMgmt, c)));
                }
            }
        }
        c = newc;
    }
    tmpt[blockid]->addColumns(c, sorted, lastInsert);
}
