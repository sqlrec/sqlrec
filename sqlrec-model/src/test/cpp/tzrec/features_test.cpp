#include <fstream>
#include <iostream>
#include <functional>
#include "feature_parser.h"

using tzrec::json;

void require(bool condition, const std::string& message) {
    if (!condition) throw std::runtime_error(message);
}

void rejects(const std::function<void()>& operation) {
    try {
        operation();
    } catch (const std::invalid_argument&) {
        return;
    }
    throw std::runtime_error("Expected invalid input to be rejected");
}

json config(const std::string& kind, const std::string& default_value = "") {
    return {{"features", json::array({{{"feature_type", "id_feature"}, {"feature_name", "encoded"},
               {"expression", "item:input"}, {kind, 100}, {"default_value", default_value}}})}};
}

int main(int argc, char** argv) {
    try {
        // Used by the FG differential test to inspect actual C++ parser output.
        if (argc == 2) {
            std::ifstream file(argv[1]);
            json fg, rows;
            file >> fg;
            std::cin >> rows;
            tzrec::FeatureParser parser(fg);
            json output = json::object();
            for (const auto& feature : parser.parse(rows)) {
                if (feature.value_dim) {
                    json values = json::array();
                    for (size_t i = 0; i < feature.dense_values.size(); i += feature.value_dim) {
                        values.push_back(std::vector<float>(feature.dense_values.begin() + i, feature.dense_values.begin() + i + feature.value_dim));
                    }
                    output[feature.name] = {{"values", values}};
                } else output[feature.name] = {{"values", feature.values}, {"lengths", feature.lengths}};
            }
            std::cout << output.dump() << std::endl;
            return 0;
        }
        for (const auto& kind : {"num_buckets", "hash_bucket_size"}) {
            tzrec::FeatureParser parser(config(kind, "3"));
            auto result = parser.parse(json::array({{{"input", "1\x1d" "2"}}, {{"input", ""}},
                          {{"input", nullptr}}, {{"input", json::array()}}, {{"input", 3}}, json::object()}));
            std::vector<int64_t> expected = std::string(kind) == "num_buckets"
                ? std::vector<int64_t>{1, 2, 3, 3, 3, 3, 3}
                : std::vector<int64_t>{49, 59, 21, 21, 21, 21, 21};
            require(result[0].name == "encoded", "expression mapping");
            require(result[0].values == expected, "bucketization and defaults");
            require(result[0].lengths == std::vector<int32_t>({2, 1, 1, 1, 1, 1}), "row lengths");
            rejects([&] { parser.parse(json::array({{{"input", true}}})); });
            rejects([&] { parser.parse(json::array({{{"input", 1.5}}})); });
            rejects([&] { parser.parse(json::array({{{"input", json::array({"1", nullptr})}}})); });
            rejects([&] { parser.parse(json::array({{{"input", json::array({"1", ""})}}})); });
            rejects([&] { parser.parse(json::array({{{"other", 1}}})); });
        }
        tzrec::FeatureParser integer(config("num_buckets"));
        setenv("USE_FARM_HASH_TO_BUCKETIZE", "false", 1);
        rejects([&] { tzrec::FeatureParser hashed(config("hash_bucket_size")); });
        setenv("USE_FARM_HASH_TO_BUCKETIZE", "true", 1);
        for (const json value : {json(-1), json(100), json("9223372036854775808"), json("1.2")}) {
            rejects([&] { integer.parse(json::array({{{"input", value}}})); });
        }
        auto empty = integer.parse(json::array({{{"input", nullptr}}, {{"input", json::array()}}}));
        require(empty[0].values.empty() && empty[0].lengths == std::vector<int32_t>({0, 0}), "all empty column");
        auto list = integer.parse(json::array({{{"input", json::array({"0", "99"})}}}));
        require(list[0].values == std::vector<int64_t>({0, 99}), "integer string list");
        auto separated = config("num_buckets", "3|4");
        separated["features"][0]["separator"] = "|";
        auto defaults = tzrec::FeatureParser(separated).parse(json::array({{{"input", nullptr}}}));
        require(defaults[0].values == std::vector<int64_t>({3, 4}), "multi-value default");
        for (const auto& key : {"weighted", "vocab_list", "sequence_fields", "unknown"}) {
            auto invalid = config("num_buckets");
            invalid["features"][0][key] = true;
            rejects([&] { tzrec::FeatureParser parser(invalid); });
        }
        for (const json bucket : {json(0), json(-1), json(1.5), json(true), json(UINT64_MAX)}) {
            auto invalid = config("num_buckets");
            invalid["features"][0]["num_buckets"] = bucket;
            rejects([&] { tzrec::FeatureParser parser(invalid); });
        }
        rejects([&] { tzrec::FeatureParser parser(config("num_buckets", "100")); });
        rejects([&] { integer.parse(json::array({1})); });
        rejects([&] { integer.parse(json::array()); });
        auto duplicate = config("num_buckets");
        duplicate["features"].push_back(duplicate["features"][0]);
        rejects([&] { tzrec::FeatureParser parser(duplicate); });
        json raw = {{"features", json::array({{{"feature_type", "raw_feature"}, {"feature_name", "price"},
                   {"expression", "item:input"}, {"default_value", "0.1"},
                   {"normalizer", "method=zscore,mean=0.1,standard_deviation=10"}}})}};
        tzrec::FeatureParser dense(raw);
        auto numbers = dense.parse(json::array({{{"input", 0.2}}, {{"input", 0.3}}, {{"input", nullptr}}}));
        require(numbers[0].value_dim == 1 && numbers[0].lengths.empty(), "dense tensor shape contract");
        require(std::abs(numbers[0].dense_values[0] - 0.01f) < 1e-7f && std::abs(numbers[0].dense_values[2] - 0.1f) < 1e-7f, "normalizer/default parity");
        for (const json value : {json(true), json("NaN"), json("12oops"), json(1e39), json::array({1,2})}) {
            rejects([&] { dense.parse(json::array({{{"input", value}}})); });
        }
        raw["features"][0].erase("normalizer");
        auto underflow = tzrec::FeatureParser(raw).parse(json::array({{{"input", "1e-999"}}}));
        require(underflow[0].dense_values == std::vector<float>({0}), "decimal underflow parity");
        rejects([&] { tzrec::FeatureParser(raw).parse(json::array({{{"input", 3.4028235e38}}})); });
        raw["features"][0]["value_dim"] = 2;
        raw["features"][0]["separator"] = "|";
        raw["features"][0]["default_value"] = "0.1|0.4";
        auto vector = tzrec::FeatureParser(raw).parse(json::array({{{"input", json::array({0.2, 0.3})}}, {{"input", nullptr}}}));
        require(vector[0].value_dim == 2 && vector[0].dense_values == std::vector<float>({0.2f,0.3f,0.1f,0.4f}), "float vectors");
        raw["features"][0]["boundaries"] = json::array({0.1,0.2,0.3});
        auto buckets = tzrec::FeatureParser(raw).parse(json::array({{{"input", json::array({0.1, 0.3})}}}));
        require(buckets[0].values == std::vector<int64_t>({1,3}) && buckets[0].lengths == std::vector<int32_t>({2}), "boundary equality");
        std::cout << "Feature parser tests passed" << std::endl;
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << std::endl;
        return 1;
    }
}
