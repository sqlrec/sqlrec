#pragma once

#include <csignal>
#include <cmath>
#include <fstream>
#include <functional>
#include <limits>
#include <regex>
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

// Preserve missing numeric features as NaN, matching the training frameworks.
// Invalid values must not silently become zero or a numeric prefix.
inline float get_float_value(const nlohmann::json& row, const std::string& col,
                             float default_val = std::numeric_limits<float>::quiet_NaN()) {
    if (!row.contains(col) || row[col].is_null()) return default_val;
    const auto& v = row[col];
    double number;
    try {
        if (v.is_number()) {
            number = v.get<double>();
        } else if (v.is_string()) {
            static const std::regex decimal(
                R"(\s*[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?\s*)");
            const auto& text = v.get_ref<const std::string&>();
            if (!std::regex_match(text, decimal)) throw std::invalid_argument("Invalid decimal");
            number = std::stod(text);
        } else {
            throw std::invalid_argument("Expected a number");
        }
    } catch (const std::invalid_argument&) {
        throw std::invalid_argument("Feature '" + col + "' must be a finite float32 number or null");
    } catch (const std::out_of_range&) {
        throw std::invalid_argument("Feature '" + col + "' is outside the finite float32 range");
    }
    if (!std::isfinite(number) || std::abs(number) > std::numeric_limits<float>::max()) {
        throw std::invalid_argument("Feature '" + col + "' is outside the finite float32 range");
    }
    return static_cast<float>(number);
}

// Extract a string from a JSON field. Returns default_val if the field is
// missing or null. For integers, converts via std::to_string (matching
// CatBoost's training int→string conversion). Other types are rejected.
inline std::string get_string_value(const nlohmann::json& row, const std::string& col,
                                    const std::string& default_val = "") {
    if (!row.contains(col) || row[col].is_null()) return default_val;
    const auto& v = row[col];
    if (v.is_string()) return v.get<std::string>();
    if (v.is_number_unsigned()) return std::to_string(v.get<uint64_t>());
    if (v.is_number_integer()) return std::to_string(v.get<int64_t>());
    throw std::invalid_argument("Categorical feature '" + col + "' must be an integer, string or null");
}

// Normalize request data to row-wise format.
// Accepts both row-wise [{"f1":1,"f2":"a"}, ...] and columnar {"f1":[1,...], "f2":["a",...]}.
// Columnar input is converted to row-wise; row-wise input is passed through.
// Lists of length 1 (in columnar format) are broadcast to all rows.
inline nlohmann::json parse_request_data(const nlohmann::json& data) {
    if (data.is_array()) {
        for (const auto& row : data) {
            if (!row.is_object()) throw std::invalid_argument("Each array element must be a JSON object");
        }
        return data;
    }
    if (!data.is_object()) {
        throw std::invalid_argument("Input data must be a JSON array or a map with string keys and list values");
    }
    if (data.empty()) return nlohmann::json::array();

    // Validate: all values must be arrays.
    for (auto it = data.begin(); it != data.end(); ++it) {
        if (!it.value().is_array()) {
            throw std::invalid_argument("Map values must be lists (key: " + it.key() + ")");
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
            throw std::invalid_argument(
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
            throw std::invalid_argument(
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
// Error results, malformed JSON and invalid arguments return 400; runtime failures return 500.
inline void register_predict_endpoint(httplib::Server& svr,
                                      std::function<nlohmann::json(const nlohmann::json&)> predict_fn) {
    svr.Post("/predict", [predict_fn](const httplib::Request& req, httplib::Response& res) {
        nlohmann::json body;
        try {
            body = nlohmann::json::parse(req.body);
        } catch (const nlohmann::json::exception& e) {
            res.status = 400;
            res.set_content(nlohmann::json{{"error", std::string("JSON parse error: ") + e.what()}}.dump(),
                            "application/json");
            return;
        }
        try {
            auto parsed = parse_request_data(body);
            auto result = predict_fn(parsed);
            if (result.contains("error")) {
                res.status = 400;
            }
            res.set_content(result.dump(), "application/json");
        } catch (const std::invalid_argument& e) {
            res.status = 400;
            res.set_content(nlohmann::json{{"error", e.what()}}.dump(), "application/json");
        } catch (const std::exception& e) {
            res.status = 500;
            res.set_content(nlohmann::json{{"error", e.what()}}.dump(), "application/json");
        }
    });
}
