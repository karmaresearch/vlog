#include <iostream>
#include <cstdlib>
#include <cstring>

#include <parser/ruledriver.h>

int main( const int argc, const char **argv ) {
    /** check for the right # of arguments **/
    MC::RuleDriver driver;
    //std::cout << "Filepath: " << argv[1] << std::endl;
    driver.parse(argv[1]);
    MC::RuleAST *root = driver.get_root();
    root->print();

    return 0;
}
