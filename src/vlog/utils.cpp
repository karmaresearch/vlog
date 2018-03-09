#include <vlog/utils.h>
#include <string>

// Utility to convert a string to escaped, as an entry for a CSV file.
std::string VLogUtils::csvString(std::string s) {
    auto pos = s.find_first_of(" \",\n\t\r");
    if (pos == std::string::npos) {
        return s;
    }
    // Now, we need to escape the string, which means quoting, and doubling every quote in the string.
    pos = s.find_first_of("\"");
    if (pos == std::string::npos) {
        // Just quoting is good enough.
        return "\"" + s + "\"";
    }
    std::string result = "\"";
    size_t beginpos = 0;
    while (pos != std::string::npos) {
        result += s.substr(beginpos, (pos-beginpos)+1) + "\"";
        beginpos = pos + 1;
        pos = s.find_first_of("\"", beginpos);
    }
    result += s.substr(beginpos, pos) + "\"";
    return result;
}
