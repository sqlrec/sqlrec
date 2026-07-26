#pragma once

#include <csignal>
#include <fstream>
#include <functional>
#include <set>
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

// Normalize request data to row-wise format.
// Accepts both row-wise [{"f1":1,"f2":"a"}, ...] and columnar {"f1":[1,...], "f2":["a",...]}.
// Columnar input is converted to row-wise; row-wise input is passed through.
// Lists of length 1 (in columnar format) are broadcast to all rows.
inline nlohmann::json parse_request_data(const nlohmann::json& data) {
    if (data.is_array()) return data;
    if (!data.is_object()) {
        throw std::runtime_error("Input data must be a JSON array or a map with string keys and list values");
    }
    if (data.empty()) return nlohmann::json::array();

    // Validate: all values must be arrays.
    for (auto it = data.begin(); it != data.end(); ++it) {
        if (!it.value().is_array()) {
            throw std::runtime_error("Map values must be lists (key: " + it.key() + ")");
        }
    }

    // Determine row count following the Python columnar_to_row logic:
    //   - If all lists share the same length, use it (may be 0).
    //   - Otherwise, only length-1 lists may differ (they are broadcast);
    //     all other lengths must agree on a single value.
    std::set<size_t> unique_lengths;
    for (auto it = data.begin(); it != data.end(); ++it) {
        unique_lengths.insert(it.value().size());
    }

    size_t n;
    if (unique_lengths.size() == 1) {
        n = *unique_lengths.begin();
    } else {
        if (unique_lengths.count(1) == 0) {
            throw std::runtime_error(
                "All lists in columnar format must have the same length or some can have length 1");
        }
        // Exactly one non-1 length is allowed (the row count); all others must be 1.
        size_t non_1_count = 0;
        size_t non_1_len = 0;
        for (size_t l : unique_lengths) {
            if (l != 1) {
                ++non_1_count;
                non_1_len = l;
            }
        }
        if (non_1_count != 1) {
            throw std::runtime_error(
                "All non-length-1 lists in columnar format must have the same length");
        }
        n = non_1_len;
    }

    // Build rows, broadcasting length-1 lists.
    nlohmann::json rows = nlohmann::json::array();
    for (size_t i = 0; i < n; ++i) {
        nlohmann::json row = nlohmann::json::object();
        for (auto it = data.begin(); it != data.end(); ++it) {
            const auto& arr = it.value();
            row[it.key()] = (arr.size() == 1) ? arr[0] : arr[i];
        }
        rows.push_back(row);
    }
    return rows;
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
            auto parsed = parse_request_data(body);
            auto result = predict_fn(parsed);
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
