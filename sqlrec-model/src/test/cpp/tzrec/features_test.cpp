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
                output[feature.name] = {{"values", feature.values}, {"lengths", feature.lengths}};
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
        std::cout << "Feature parser tests passed" << std::endl;
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << std::endl;
        return 1;
    }
}
