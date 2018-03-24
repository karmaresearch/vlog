#include <cmath>
#include <fstream>
#include <iostream>
#include <sstream>
#include <vlog/LogisticRegression.h>

using namespace std;

LogisticRegression::LogisticRegression(int n) {
    this->rate = 0.0001;
    this->ITERATIONS = 3000;
    for (int i = 0; i < n; ++i) {
        weights.push_back(0.0);
    }
}

double LogisticRegression::sigmoid(double z) {
    return (double)1.0 /(double) (1.0 + exp(-z));
}

void LogisticRegression::train(vector<Instance>& instances) {
    for (int n = 0; n < ITERATIONS; ++n) {
        //double lik = 0.0;
        for (int i = 0; i < instances.size(); ++i) {
            vector<double> x (instances.at(i).x);
            double predicted = classify(x);
            int label = instances.at(i).label;
            for (int j = 0; j < weights.size(); ++j) {
                weights[j] = weights[j] + rate * (label - predicted) * x[j];
            }

            // Not necessary for learning
            //lik += label * log(classify(x)) + (1-label) * log(1 - classify(x));
        }

        //std::ostringstream strs;
        //strs << lik;
        //std::string strDouble = strs.str();
    }
}

double LogisticRegression::classify(vector<double>& x) {
    double logit = 0.0;
    for (int i = 0; i < weights.size(); ++i) {
        logit += weights[i] * x[i];
    }

    return sigmoid(logit);// > 0.5) ? 0.0 : 1.0;
}

void LogisticRegression::getWeights(vector<double>& wt) {
    for (int i = 0; i < weights.size(); ++i) {
        wt.push_back(weights[i]);
    }
}

vector<Instance> LogisticRegression::readDataset(std::string fileName) {
    vector<Instance> dataset;
    ifstream file(fileName);
    string line;
    // skip the first line
    getline(file, line);
    while (file && getline(file, line)) {
        if (line.size() == 0) {
            continue;
        }

        // Split the line
        std::istringstream linebuffer(line);
        vector<string> columns;
        string token;
        while (getline(linebuffer, token, ' ')) {
            columns.push_back(token);
        }

        // Skip the last column (label)
        int i = 0;
        vector<double> data(columns.size() - 1, 0);
        for (i = 0; i < columns.size() - 1; ++i) {
            data[i-1] = atof(columns[i].c_str());
        }
        int label = atoi(columns[i].c_str());
        Instance instance(label, data);
        dataset.push_back(instance);
    }

    return dataset;
}
