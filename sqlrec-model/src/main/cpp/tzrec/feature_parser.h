#pragma once

#include <cctype>
#include <cstdint>
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
};

struct SparseFeature {
    std::string name;
    std::vector<int64_t> values;
    std::vector<int32_t> lengths;
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
            "need_prefix", "value_dim", "num_buckets", "hash_bucket_size", "separator"};
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
            if (raw.value("feature_type", "") != "id_feature" ||
                raw.value("value_type", "string") != "string" ||
                raw.value("need_prefix", false) || raw.value("value_dim", 0) != 0) {
                fail(name, "only ordinary unweighted string-valued ID features are supported");
            }
            std::string expression = raw.value("expression", std::string());
            if (expression.rfind("item:", 0) != 0 || expression.size() == 5 ||
                expression.find(':', 5) != std::string::npos) {
                fail(name, "expected expression item:<input_column>");
            }
            bool hashed = raw.contains("hash_bucket_size");
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
        for (const auto& feature : features_) {
            bool present = false;
            for (const auto& row : rows) present |= row.contains(feature.input_name);
            if (!present) fail(feature.name, "missing input column: " + feature.input_name);
            SparseFeature parsed{feature.name, {}, {}};
            parsed.lengths.reserve(rows.size());
            for (const auto& row : rows) {
                const auto value = row.value(feature.input_name, json());
                size_t before = parsed.values.size();
                if (value.is_null() || (value.is_string() && value.get_ref<const std::string&>().empty()) ||
                    (value.is_array() && value.empty())) {
                    encode(feature, feature.default_value, parsed.values);
                } else {
                    encode(feature, value, parsed.values);
                }
                size_t length = parsed.values.size() - before;
                if (length > static_cast<size_t>(INT32_MAX)) fail(feature.name, "too many tokens");
                parsed.lengths.push_back(static_cast<int32_t>(length));
            }
            result.push_back(std::move(parsed));
        }
        return result;
    }

    json warmup_row() const {
        json row = json::object();
        for (const auto& feature : features_) row[feature.input_name] = "0";
        return row;
    }

private:
    std::vector<Feature> features_;

    [[noreturn]] static void fail(const std::string& name, const std::string& reason) {
        throw std::invalid_argument("Feature '" + name + "': " + reason);
    }

    static void token(const Feature& feature, const std::string& value, std::vector<int64_t>& output) {
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
