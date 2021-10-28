#pragma once

#include <sstream>
#include <string>
#include <vector>

namespace faabric::util {
bool isAllWhitespace(const std::string& input);

bool startsWith(const std::string& input, const std::string& subStr);

bool endsWith(const std::string& input, const std::string& subStr);

bool contains(const std::string& input, const std::string& subStr);

std::string removeSubstr(const std::string& input, const std::string& toErase);

bool stringIsInt(const std::string& input);

template<class T>
std::string vectorToString(std::vector<T> vec)
{
    std::stringstream ss;

    ss << "[";
    for (int i = 0; i < vec.size(); i++) {
        ss << vec.at(i);

        if (i < vec.size() - 1) {
            ss << ", ";
        }
    }
    ss << "]";

    return ss.str();
}
}
