#ifndef _COSTESTIMATOR_H
#define _COSTESTIMATOR_H

class CostEstimator {
private:
    long cost;
public:
    CostEstimator() : cost(0) {
    }

    void addCost(long c) {
        cost += c;
    }

    long getCost() {
        return cost;
    }
};

#endif
