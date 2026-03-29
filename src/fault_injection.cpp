#include "fault_injection.h"
#include <cstdlib>
#include <iostream>
#ifdef _WIN32
#include <windows.h>
#endif

static std::string g_armed_point = "";

void FaultInjection::check(const std::string& point) {
    if (!g_armed_point.empty() && g_armed_point == point) {
        std::cerr << "[FaultInjection] Triggering deliberate crash at point: " << point << std::endl;
        std::fflush(stderr);
#ifdef _WIN32
        // Hardest possible termination on Windows
        TerminateProcess(GetCurrentProcess(), 1);
#endif
        std::_Exit(1);
    }
}

void FaultInjection::arm(const std::string& point) {
    g_armed_point = point;
}
