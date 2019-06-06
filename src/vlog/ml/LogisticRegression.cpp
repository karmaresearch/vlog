#include <cmath>
#include <fstream>
#include <iostream>
#include <sstream>
#include <vlog/ml/LogisticRegression.h>
#include <cstdlib>
#include <cassert>
#include <vlog/ml/helper.h>

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
        for (int i = 0; i < instances.size(); ++i) {
            vector<double> x (instances.at(i).x);
            double predicted = classify(x);
            // TODO: This predicted value will be a probability value
            // We are subtracting it from 0 or 1 (label)
            int label = instances.at(i).label;
            for (int j = 0; j < weights.size(); ++j) {
                weights[j] = weights[j] + rate * (label - predicted) * x[j];
            }
        }
    }
}

double LogisticRegression::classify(vector<double>& x) {
    double logit = 0.0;
    for (int i = 0; i < weights.size(); ++i) {
        logit += weights[i] * x[i];
    }

    return sigmoid(logit);// > 0.5) ? 0.0 : 1.0;
    //return (sigmoid(logit) > 0.5) ? 1.0 : 0.0;
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

vector<Instance> LogisticRegression::readLogfile(std::string fileName, vector<string>& allQueries) {
    vector<Instance> dataset;
    ifstream file(fileName);
    string line;
    while (file && getline(file, line)) {
        if (line.size() == 0) {
            continue;
        }
        // A line looks like this (An underscore (_) represents a space)
        // query_fe,a,t,u,r,es_QSQTime_MagicTime_Decision

        // Ex(1). RP29(<http://www.Department4.University60.edu/FullProfessor5>,B) 4.000000,4,1,1,2,0 1.290000 2.069000 1
        // some queries can contain spaces, commas. To address these, we split tokens by scanning the string from the end
        // Ex(2). RP1052(A,"(A)National Security (B) Public Accounts(C)Rules,Business & Privliges (D) Foreign Affairs (E) Kashmir"@en)

        vector<string> tokens;
        tokens = rsplit(line); // by default gives 5 tokens from the back of string
        vector<string> features = split(tokens[1], ',');
        vector<double> data;
        for (auto feat : features) {
            data.push_back(std::stof(feat));
        }
        assert(data.size() == 6);
        int label = std::stoi(tokens[4]);
        Instance instance(label, data);
        dataset.push_back(instance);
        allQueries.push_back(line);
    }

    return dataset;
}
