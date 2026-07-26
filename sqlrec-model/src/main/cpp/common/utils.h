#pragma once

#include <csignal>
#include <fstream>
#include <functional>
#include <sstream>
#include <stdexcept>
#include <string>

#include "httplib.h"
#include "json.hpp"

// --- File I/O ---

inline std::string read_file(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        throw std::runtime_error("Failed to open file: " + path);
    }
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

// --- JSON value extraction ---

// Extract a float from a JSON field. Returns default_val if the field is
// missing, null, or unparseable. Handles number→float and string→stof.
inline float get_float_value(const nlohmann::json& row, const std::string& col,
                             float default_val = 0.0f) {
    if (!row.contains(col) || row[col].is_null()) return default_val;
    const auto& v = row[col];
    if (v.is_number()) return v.get<float>();
    try {
        return std::stof(v.is_string() ? v.get<std::string>() : v.dump());
    } catch (...) {
        return default_val;
    }
}

// Extract a string from a JSON field. Returns default_val if the field is
// missing or null. For integers, converts via std::to_string (matching
// CatBoost's internal int→string conversion). For other non-string types,
// uses dump() as fallback.
inline std::string get_string_value(const nlohmann::json& row, const std::string& col,
                                    const std::string& default_val = "") {
    if (!row.contains(col) || row[col].is_null()) return default_val;
    const auto& v = row[col];
    if (v.is_string()) return v.get<std::string>();
    if (v.is_number_integer()) return std::to_string(v.get<int64_t>());
    return v.dump();
}

// --- Server utilities ---

inline void setup_graceful_shutdown(httplib::Server& svr) {
    static httplib::Server* ptr = nullptr;
    ptr = &svr;
    std::signal(SIGTERM, [](int) { if (ptr) ptr->stop(); });
    std::signal(SIGINT, [](int) { if (ptr) ptr->stop(); });
}

inline void register_health_endpoint(httplib::Server& svr) {
    svr.Get("/health", [](const httplib::Request&, httplib::Response& res) {
        res.set_content("{\"status\":\"ok\"}", "application/json");
    });
}

// Register POST /predict with standard error handling.
// predict_fn takes a parsed JSON value and returns a JSON result.
// If the result contains "error", responds with 400; exceptions → 500.
inline void register_predict_endpoint(httplib::Server& svr,
                                      std::function<nlohmann::json(const nlohmann::json&)> predict_fn) {
    svr.Post("/predict", [predict_fn](const httplib::Request& req, httplib::Response& res) {
        try {
            auto body = nlohmann::json::parse(req.body);
            auto result = predict_fn(body);
            if (result.contains("error")) {
                res.status = 400;
            }
            res.set_content(result.dump(), "application/json");
        } catch (const nlohmann::json::parse_error& e) {
            res.status = 400;
            res.set_content(nlohmann::json{{"error", std::string("JSON parse error: ") + e.what()}}.dump(),
                            "application/json");
        } catch (const std::exception& e) {
            res.status = 500;
            res.set_content(nlohmann::json{{"error", e.what()}}.dump(), "application/json");
        }
    });
}
