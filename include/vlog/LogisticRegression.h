#ifndef ML_LOGISTICREGRESSION_H
#define ML_LOGISTICREGRESSION_H

#include <iostream>
#include <vector>

using namespace std;

class Instance {
    public:
        int label;
        vector<double> x;

        Instance(int label, vector<double> x) {
            this->label = label;
            this->x = x;
        }
};

class LogisticRegression {

    private:
        // Learning rate
        double rate; // default 0.0001
        int ITERATIONS; // default 3000
        // Weights to learn
        vector<double> weights;

        static double sigmoid(double z);

    public:
        LogisticRegression(int n);
        void train(vector<Instance>& instances);
        double classify(vector<double>&); // predict() function
        static vector<Instance> readDataset(std::string file);
};

#endif // ML_LOGISTICREGRESSION_H
