#pragma once

#include <cctype>
#include <algorithm>
#include <cmath>
#include <map>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include "farmhash.h"
#include "json.hpp"

namespace tzrec {
using json = nlohmann::json;

struct Feature {
    std::string name;
    std::string input_name;
    bool hashed;
    uint64_t bucket_count;
    std::string default_value;
    std::string separator;
    bool raw = false;
    size_t value_dim = 1;
    std::vector<float> boundaries;
    std::string method;
    std::map<std::string, float> normalizer;
};

struct SparseFeature {
    std::string name;
    std::vector<int64_t> values;
    std::vector<int32_t> lengths;
    std::vector<float> dense_values;
    size_t value_dim = 0;
};

class FeatureParser {
public:
    explicit FeatureParser(const json& config) {
        if (!config.is_object() || config.size() != 1 || !config.contains("features") ||
            !config.at("features").is_array() || config.at("features").empty()) {
            throw std::invalid_argument("fg.json must contain a non-empty features array only");
        }
        const std::set<std::string> allowed = {
            "feature_type", "feature_name", "expression", "default_value", "value_type",
            "need_prefix", "value_dim", "num_buckets", "hash_bucket_size", "separator", "boundaries", "normalizer"};
        std::set<std::string> names;
        for (const auto& raw : config.at("features")) {
            if (!raw.is_object()) throw std::invalid_argument("Feature config must be an object");
            std::string name = raw.value("feature_name", std::string());
            if (name.empty() || !names.insert(name).second) {
                throw std::invalid_argument("Feature names must be non-empty and unique: " + name);
            }
            for (auto it = raw.begin(); it != raw.end(); ++it) {
                if (!allowed.count(it.key())) fail(name, "unsupported config key: " + it.key());
            }
            bool numeric = raw.value("feature_type", "") == "raw_feature";
            if (numeric) {
                if (raw.value("value_type", "float") != "float" || raw.contains("num_buckets") ||
                    raw.contains("hash_bucket_size") || raw.contains("need_prefix")) fail(name, "invalid raw_feature config");
            } else if (raw.contains("boundaries") || raw.contains("normalizer")) {
                fail(name, "numeric options require raw_feature");
            }
            if (!numeric && (raw.value("feature_type", "") != "id_feature" ||
                raw.value("value_type", "string") != "string" ||
                raw.value("need_prefix", false) || raw.value("value_dim", 0) != 0)) {
                fail(name, "only ordinary unweighted string-valued ID features are supported");
            }
            std::string expression = raw.value("expression", std::string());
            if (expression.rfind("item:", 0) != 0 || expression.size() == 5 ||
                expression.find(':', 5) != std::string::npos) {
                fail(name, "expected expression item:<input_column>");
            }
            if (numeric) {
                Feature feature{name, expression.substr(5), false, 0, raw.value("default_value", std::string("0")),
                                raw.value("separator", std::string(1, '\x1d'))};
                feature.raw = true;
                const auto& dim = raw.contains("value_dim") ? raw.at("value_dim") : json(1);
                if (!dim.is_number_integer() || dim.get<int64_t>() <= 0 || dim.get<uint64_t>() > 65536) fail(name, "invalid value_dim");
                feature.value_dim = dim.get<size_t>();
                if (feature.separator.empty()) fail(name, "separator must not be empty");
                if (raw.contains("boundaries")) {
                    if (!raw.at("boundaries").is_array()) fail(name, "boundaries must be an array");
                    for (const auto& boundary : raw.at("boundaries")) {
                        float value = number(feature, boundary);
                        if (!feature.boundaries.empty() && value <= feature.boundaries.back()) fail(name, "boundaries must be strictly increasing");
                        feature.boundaries.push_back(value);
                    }
                }
                set_normalizer(feature, raw.value("normalizer", std::string()));
                if (feature.default_value.empty() && feature.boundaries.empty()) fail(name, "dense features need a default_value");
                numeric_values(feature, feature.default_value, false);
                features_.push_back(std::move(feature));
                continue;
            }
            bool hashed = raw.contains("hash_bucket_size");
            if (hashed) {
                const char* setting = std::getenv("USE_FARM_HASH_TO_BUCKETIZE");
                std::string enabled = setting ? setting : "true";
                std::transform(enabled.begin(), enabled.end(), enabled.begin(), [](unsigned char c) { return std::tolower(c); });
                if (enabled != "true") fail(name, "hash features require USE_FARM_HASH_TO_BUCKETIZE=true");
            }
            if (hashed == raw.contains("num_buckets")) {
                fail(name, "exactly one of num_buckets and hash_bucket_size is required");
            }
            const auto& count = raw.at(hashed ? "hash_bucket_size" : "num_buckets");
            if (!count.is_number_integer() ||
                (!count.is_number_unsigned() && count.get<int64_t>() <= 0) ||
                count.get<uint64_t>() == 0 || count.get<uint64_t>() > static_cast<uint64_t>(INT64_MAX)) {
                fail(name, "bucket count must be a positive int64");
            }
            std::string separator = raw.value("separator", std::string(1, '\x1d'));
            if (separator.empty()) fail(name, "separator must not be empty");
            features_.push_back({name, expression.substr(5), hashed, count.get<uint64_t>(),
                                 raw.value("default_value", std::string()), separator});
            // Validate default tokenization and integer ranges before accepting the model.
            std::vector<int64_t> defaults;
            encode(features_.back(), features_.back().default_value, defaults);
        }
        if (util::Fingerprint64("1", 1) % 100 != 49 ||
            util::Fingerprint64("2", 1) % 100 != 59 ||
            util::Fingerprint64("3", 1) % 100 != 21) {
            throw std::runtime_error("FarmHash Fingerprint64 self-check failed");
        }
    }

    std::vector<SparseFeature> parse(const json& rows) const {
        if (!rows.is_array() || rows.empty()) {
            throw std::invalid_argument("Input must be a non-empty array of JSON objects");
        }
        for (const auto& row : rows) {
            if (!row.is_object()) throw std::invalid_argument("Every input row must be a JSON object");
        }
        std::vector<SparseFeature> result;
        size_t total_values = 0;
        for (const auto& feature : features_) {
            bool present = false;
            for (const auto& row : rows) present |= row.contains(feature.input_name);
            if (!present) fail(feature.name, "missing input column: " + feature.input_name);
            SparseFeature parsed{feature.name, {}, {}};
            if (feature.raw && feature.boundaries.empty()) parsed.value_dim = feature.value_dim;
            parsed.lengths.reserve(rows.size());
            for (const auto& row : rows) {
                const auto value = row.value(feature.input_name, json());
                if (feature.raw) {
                    bool missing = value.is_null() || (value.is_string() && value.get_ref<const std::string&>().empty()) ||
                                   (value.is_array() && value.empty());
                    auto numbers = numeric_values(feature, missing ? json(feature.default_value) : value, !missing);
                    if (total_values + numbers.size() > 65536) fail(feature.name, "Feature value count exceeds 65536");
                    total_values += numbers.size();
                    if (feature.boundaries.empty()) {
                        parsed.dense_values.insert(parsed.dense_values.end(), numbers.begin(), numbers.end());
                    } else {
                        for (float number : numbers) parsed.values.push_back(std::upper_bound(feature.boundaries.begin(), feature.boundaries.end(), number) - feature.boundaries.begin());
                        parsed.lengths.push_back(static_cast<int32_t>(numbers.size()));
                    }
                    continue;
                }
                size_t before = parsed.values.size();
                if (value.is_null() || (value.is_string() && value.get_ref<const std::string&>().empty()) ||
                    (value.is_array() && value.empty())) {
                    encode(feature, feature.default_value, parsed.values);
                } else {
                    encode(feature, value, parsed.values);
                }
                size_t length = parsed.values.size() - before;
                if (total_values + length > 65536) fail(feature.name, "Feature value count exceeds 65536");
                total_values += length;
                if (length > static_cast<size_t>(INT32_MAX)) fail(feature.name, "too many tokens");
                parsed.lengths.push_back(static_cast<int32_t>(length));
            }
            result.push_back(std::move(parsed));
        }
        return result;
    }

    json warmup_row() const {
        json row = json::object();
        for (const auto& feature : features_) row[feature.input_name] = feature.raw ? json(nullptr) : json("0");
        return row;
    }

private:
    std::vector<Feature> features_;

    [[noreturn]] static void fail(const std::string& name, const std::string& reason) {
        throw std::invalid_argument("Feature '" + name + "': " + reason);
    }

    static float number(const Feature& feature, const json& value) {
        double parsed;
        if (value.is_number()) parsed = value.get<double>();
        else if (value.is_string()) {
            const auto& text = value.get_ref<const std::string&>();
            if (text.find_first_not_of(" +-0123456789.eE\t\r\n\v\f") != std::string::npos) fail(feature.name, "expected a decimal number");
            char* tail = nullptr;
            // Decimal underflow rounds to zero, as in Python/Arrow float32 conversion.
            parsed = std::strtod(text.c_str(), &tail);
            size_t end = static_cast<size_t>(tail - text.c_str());
            if (end == 0) fail(feature.name, "expected a finite number");
            while (end < text.size() && std::isspace(static_cast<unsigned char>(text[end]))) ++end;
            if (end != text.size()) fail(feature.name, "expected a complete number");
        } else fail(feature.name, "expected a number or numeric string");
        float result = static_cast<float>(parsed);
        if (!std::isfinite(parsed) || std::abs(parsed) > static_cast<double>(std::numeric_limits<float>::max()) || !std::isfinite(result)) fail(feature.name, "number must be finite float32");
        return result;
    }

    static void set_normalizer(Feature& feature, const std::string& text) {
        if (text.empty()) return;
        size_t start = 0;
        std::set<std::string> seen;
        while (true) {
            size_t end = text.find(',', start);
            std::string part = text.substr(start, end == std::string::npos ? end : end - start);
            auto trim = [](std::string str) {
                size_t first = str.find_first_not_of(" \t\n\r"), last = str.find_last_not_of(" \t\n\r");
                return first == std::string::npos ? std::string() : str.substr(first, last - first + 1);
            };
            part = trim(part);
            size_t equal = part.find('=');
            if (equal == std::string::npos || part.find('=', equal + 1) != std::string::npos) fail(feature.name, "invalid normalizer");
            std::string key = part.substr(0, equal), value = part.substr(equal + 1);
            if (!seen.insert(key).second) fail(feature.name, "duplicate normalizer parameter");
            if (key == "method") feature.method = value;
            else feature.normalizer[key] = number(feature, value);
            if (end == std::string::npos) break;
            start = end + 1;
        }
        std::set<std::string> required;
        if (feature.method == "zscore") required = {"mean", "standard_deviation"};
        else if (feature.method == "minmax") required = {"min", "max"};
        else if (feature.method == "log10") {
            required = {"threshold", "default"};
            feature.normalizer.emplace("threshold", 1e-10f);
            feature.normalizer.emplace("default", -10.0f);
        } else fail(feature.name, "normalizer must use zscore, minmax or log10");
        if (feature.normalizer.size() != required.size()) fail(feature.name, "invalid normalizer parameters");
        for (const auto& key : required) if (!feature.normalizer.count(key)) fail(feature.name, "missing normalizer parameter: " + key);
        if ((feature.method == "zscore" && feature.normalizer.at("standard_deviation") <= 0) ||
            (feature.method == "minmax" && feature.normalizer.at("max") <= feature.normalizer.at("min")) ||
            (feature.method == "log10" && feature.normalizer.at("threshold") <= 0)) fail(feature.name, "normalizer scale must be positive");
    }

    static std::vector<float> numeric_values(const Feature& feature, const json& input, bool normalize) {
        std::vector<json> tokens;
        if (input.is_string()) {
            const auto& text = input.get_ref<const std::string&>();
            if (text.empty()) return {};
            size_t start = 0;
            while (true) {
                size_t end = text.find(feature.separator, start);
                tokens.push_back(text.substr(start, end == std::string::npos ? end : end - start));
                if (end == std::string::npos) break;
                start = end + feature.separator.size();
            }
        } else if (input.is_array()) {
            for (const auto& token : input) tokens.push_back(token);
        } else tokens.push_back(input);
        if (tokens.size() != feature.value_dim) fail(feature.name, "value count must match value_dim");
        std::vector<float> result;
        for (const auto& token : tokens) {
            float value = number(feature, token);
            if (normalize && !feature.method.empty()) {
                const auto& p = feature.normalizer;
                if (feature.method == "zscore") value = (value - p.at("mean")) / p.at("standard_deviation");
                else if (feature.method == "minmax") value = (value - p.at("min")) / (p.at("max") - p.at("min"));
                else value = value > p.at("threshold") ? std::log10(value) : p.at("default");
                if (!std::isfinite(value)) fail(feature.name, "normalized value must be finite float32");
            }
            result.push_back(value);
        }
        return result;
    }

    static void token(const Feature& feature, const std::string& value, std::vector<int64_t>& output) {
        if (output.size() >= 65536) fail(feature.name, "Feature value count exceeds 65536");
        if (value.empty()) fail(feature.name, "empty token inside multi-value input");
        if (feature.hashed) {
            output.push_back(static_cast<int64_t>(util::Fingerprint64(value.data(), value.size()) %
                                                 feature.bucket_count));
            return;
        }
        int64_t id;
        try {
            size_t end = 0;
            id = std::stoll(value, &end, 10);
            while (end < value.size() && std::isspace(static_cast<unsigned char>(value[end]))) ++end;
            if (end != value.size()) fail(feature.name, "expected an integer ID: " + value);
        } catch (const std::exception&) {
            fail(feature.name, "expected an int64 ID: " + value);
        }
        if (id < 0 || static_cast<uint64_t>(id) >= feature.bucket_count) {
            fail(feature.name, "ID outside [0, " + std::to_string(feature.bucket_count) + "): " + value);
        }
        output.push_back(id);
    }

    static void encode(const Feature& feature, const json& value, std::vector<int64_t>& output) {
        if (value.is_string()) {
            const auto& text = value.get_ref<const std::string&>();
            if (text.empty()) return;
            size_t start = 0;
            while (true) {
                size_t end = text.find(feature.separator, start);
                token(feature, text.substr(start, end == std::string::npos ? end : end - start), output);
                if (end == std::string::npos) break;
                start = end + feature.separator.size();
            }
        } else if (value.is_number_integer()) {
            token(feature, value.dump(), output);
        } else if (value.is_array()) {
            for (const auto& item : value) {
                if (!item.is_string()) fail(feature.name, "list IDs must be non-empty strings");
                token(feature, item.get_ref<const std::string&>(), output);
            }
        } else {
            fail(feature.name, "expected an integer, string, or list of strings");
        }
    }
};
}  // namespace tzrec
