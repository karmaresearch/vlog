#include <vector>

// This function generates m random numbers from the range (0, n) and stores them in indexes vector
void getRandomTupleIndexes(uint64_t m, uint64_t n, std::vector<int>& indexes);
std::vector<std::string> split( std::string str, char sep = ' ' );
std::string stringJoin(std::vector<std::string>& vec, char delimiter=',');
std::vector<std::string> rsplit(std::string logLine, char sep = ' ', int maxSplits = 4);
