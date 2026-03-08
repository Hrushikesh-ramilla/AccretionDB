#pragma once
#include <string>

class FaultInjection {
public:
    static void check(const std::string& point);
    static void arm(const std::string& point);
};
