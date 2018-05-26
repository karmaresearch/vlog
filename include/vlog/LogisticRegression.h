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

        friend ostream& operator << (ostream& out, Instance& instance) {
            for (int i = 0; i < instance.x.size(); ++i) {
                out << instance.x[i] << " , ";
            }
            out << instance.label;
            return out;
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
        void getWeights(vector<double>& wt);
        static vector<Instance> readDataset(std::string file);
        static vector<Instance> readLogfile(std::string file, vector<string>& allQueries);
};

#endif // ML_LOGISTICREGRESSION_H
