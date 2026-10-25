#include <cmath>
#include <fstream>
#include <functional>
#include <iostream>
#include <limits>
#include <utility>
#include "feature_parser.h"

using tzrec::json;

void require(bool condition, const std::string& message) {
    if (!condition) throw std::runtime_error(message);
}

template <typename Exception = std::invalid_argument>
void rejects(const std::function<void()>& operation, const std::string& reason = "") {
    try {
        operation();
    } catch (const Exception& error) {
        require(std::string(error.what()).find(reason) != std::string::npos,
                "Unexpected rejection: " + std::string(error.what()) + "; expected: " + reason);
        return;
    }
    throw std::runtime_error("Expected invalid input to be rejected");
}

json config(const std::string& kind, const std::string& default_value = "") {
    return {{"features", json::array({{{"feature_type", "id_feature"}, {"feature_name", "encoded"},
               {"expression", "item:input"}, {kind, 100}, {"default_value", default_value}}})}};
}

json raw_config(const std::string& default_value = "0.25") {
    return {{"features", json::array({{{"feature_type", "raw_feature"}, {"feature_name", "price"},
               {"expression", "item:input"}, {"default_value", default_value}}})}};
}

void require_dense(const tzrec::SparseFeature& feature, size_t dim,
                   const std::vector<float>& expected, const std::string& message) {
    require(feature.value_dim == dim && feature.values.empty() && feature.lengths.empty(), message + ": shape");
    require(feature.dense_values.size() == expected.size(), message + ": value count");
    for (size_t i = 0; i < expected.size(); ++i) {
        require(std::isfinite(feature.dense_values[i]) && std::abs(feature.dense_values[i] - expected[i]) < 1e-6f,
                message + ": value " + std::to_string(i));
    }
}

void test_config_validation() {
    for (const auto& invalid : std::vector<json>{nullptr, json::array(), json::object(),
             {{"other", json::array()}}, {{"features", nullptr}}, {{"features", json::array()}},
             {{"features", json::object()}}, {{"features", json::array()}, {"other", true}}}) {
        rejects([&] { tzrec::FeatureParser parser(invalid); }, "non-empty features array only");
    }
    auto invalid = config("num_buckets");
    invalid["features"][0] = 1;
    rejects([&] { tzrec::FeatureParser parser(invalid); }, "must be an object");
    for (const auto& key : {"feature_name", "feature_type", "expression"}) {
        invalid = config("num_buckets");
        invalid["features"][0].erase(key);
        rejects([&] { tzrec::FeatureParser parser(invalid); });
    }
    for (const auto& override : std::vector<json>{
             {{"feature_name", ""}}, {{"feature_type", "sequence_feature"}}, {{"value_type", "int64"}},
             {{"need_prefix", true}}, {{"value_dim", 1}}, {{"expression", "user:input"}},
             {{"expression", "item:"}}, {{"expression", "item:input:other"}}, {{"separator", ""}},
             {{"boundaries", json::array({1})}}, {{"normalizer", "method=log10"}}}) {
        invalid = config("num_buckets");
        invalid["features"][0].update(override);
        rejects([&] { tzrec::FeatureParser parser(invalid); },
                override.contains("feature_name") ? "Feature names must be non-empty" : "Feature '");
    }
    invalid = config("num_buckets");
    invalid["features"][0].erase("num_buckets");
    rejects([&] { tzrec::FeatureParser parser(invalid); }, "exactly one");
    invalid["features"][0]["num_buckets"] = 100;
    invalid["features"][0]["hash_bucket_size"] = 100;
    rejects([&] { tzrec::FeatureParser parser(invalid); }, "exactly one");
    for (const json bucket : {json(0), json(uint64_t(0)), json(-1), json(1.5), json(true), json(UINT64_MAX)}) {
        invalid = config("hash_bucket_size");
        invalid["features"][0]["hash_bucket_size"] = bucket;
        rejects([&] { tzrec::FeatureParser parser(invalid); }, "positive int64");
    }
    for (const auto& override : std::vector<json>{
             {{"value_type", "string"}}, {{"num_buckets", 100}}, {{"hash_bucket_size", 100}},
             {{"need_prefix", false}}, {{"separator", ""}}, {{"default_value", ""}},
             {{"boundaries", 1}}, {{"boundaries", json::array({1, 1})}},
             {{"boundaries", json::array({2, 1})}}, {{"boundaries", json::array({"NaN"})}},
             {{"boundaries", json::array({1e39})}}, {{"boundaries", json::array({nullptr})}},
             {{"value_dim", 2}}, {{"default_value", "oops"}}}) {
        invalid = raw_config();
        invalid["features"][0].update(override);
        rejects([&] { tzrec::FeatureParser parser(invalid); }, "Feature 'price'");
    }
    for (const json dim : {json(0), json(-1), json(1.5), json(true), json(65537), json(UINT64_MAX)}) {
        invalid = raw_config();
        invalid["features"][0]["value_dim"] = dim;
        rejects([&] { tzrec::FeatureParser parser(invalid); }, "invalid value_dim");
    }
    // Wrong JSON field types are also rejected, via nlohmann's type_error.
    for (const auto& override : std::vector<json>{
             {{"feature_name", false}}, {{"feature_type", 1}}, {{"expression", nullptr}},
             {{"separator", json::array()}}, {{"default_value", 0}}, {{"need_prefix", "false"}}}) {
        invalid = config("num_buckets");
        invalid["features"][0].update(override);
        rejects<json::type_error>([&] { tzrec::FeatureParser parser(invalid); });
    }
    invalid = raw_config();
    invalid["features"][0]["normalizer"] = true;
    rejects<json::type_error>([&] { tzrec::FeatureParser parser(invalid); });
}

void test_normalizers() {
    auto cfg = raw_config();
    cfg["features"][0]["normalizer"] = " max=2,method=minmax,min=0 ";
    auto parsed = tzrec::FeatureParser(cfg).parse(json::array({{{"input", 1}}, {{"input", -1}},
                  {{"input", 3}}, {{"input", nullptr}}, {{"input", ""}}, {{"input", json::array()}}, json::object()}));
    require_dense(parsed[0], 1, {0.5f, -0.5f, 1.5f, 0.25f, 0.25f, 0.25f, 0.25f}, "minmax without clipping/default bypass");

    cfg["features"][0]["normalizer"] = "method=log10";
    parsed = tzrec::FeatureParser(cfg).parse(json::array({{{"input", 100}}, {{"input", 1e-10}},
                 {{"input", 0}}, {{"input", -1}}, {{"input", nullptr}}}));
    require_dense(parsed[0], 1, {2, 0, 0, 0, 0.25f}, "log10 implicit parameters and threshold equality");
    for (const auto& normalizer : {"method=log10", "method=log10,threshold=0.05", "method=log10,default=-5"}) {
        cfg["features"][0]["normalizer"] = normalizer;
        parsed = tzrec::FeatureParser(cfg).parse(json::array({{{"input", 0}}, {{"input", 0.05}},
                     {{"input", 0.1}}, {{"input", 1}}, {{"input", 10}}, {{"input", nullptr}}}));
        const std::vector<float> expected = std::string(normalizer) == "method=log10"
            ? std::vector<float>{0, 0, 0, 0, 1, 0.25f}
            : std::string(normalizer) == "method=log10,threshold=0.05"
                ? std::vector<float>{0, 0, -1, 0, 1, 0.25f}
                : std::vector<float>{-5, -5, -5, -5, 1, 0.25f};
        require_dense(parsed[0], 1, expected, "log10 omitted parameters match original FG");
    }
    cfg["features"][0]["normalizer"] = "default=-7,threshold=1,method=log10";
    parsed = tzrec::FeatureParser(cfg).parse(json::array({{{"input", 1}}, {{"input", 0.5}}, {{"input", 100}}}));
    require_dense(parsed[0], 1, {-7, -7, 2}, "log10 explicit parameters");

    cfg = raw_config("2|4");
    cfg["features"][0].update({{"value_dim", 2}, {"separator", "|"},
                              {"normalizer", "method=zscore,mean=2,standard_deviation=2"}});
    parsed = tzrec::FeatureParser(cfg).parse(json::array({{{"input", "4|6"}}, {{"input", json::array({6, 8})}},
                 {{"input", nullptr}}, {{"input", ""}}, {{"input", json::array()}}, json::object()}));
    require_dense(parsed[0], 2, {1, 2, 2, 3, 2, 4, 2, 4, 2, 4, 2, 4}, "vector normalization and defaults");

    for (const auto& normalizer : {"method", "method=zscore=bad", " ", "method=log10,",
             "method=log10,method=log10", "method=zscore,mean=0,mean=1,standard_deviation=1",
             "method=unknown", "mean=0,standard_deviation=1", "method=zscore,mean=0",
             "method=zscore,mean=0,other=1", "method=log10,other=1", "method=log10,threshold=NaN",
             "method=zscore,mean=0,standard_deviation=0", "method=zscore,mean=0,standard_deviation=-1",
             "method=minmax,min=1,max=1", "method=minmax,min=2,max=1",
             "method=log10,threshold=0", "method=log10,threshold=-1"}) {
        cfg = raw_config();
        cfg["features"][0]["normalizer"] = normalizer;
        rejects([&] { tzrec::FeatureParser parser(cfg); }, "Feature 'price'");
    }
    for (const auto& normalizer : {"method=zscore,mean=-3e38,standard_deviation=1",
                                  "method=minmax,min=-3e38,max=0"}) {
        cfg = raw_config();
        cfg["features"][0]["normalizer"] = normalizer;
        tzrec::FeatureParser parser(cfg);
        rejects([&] { parser.parse(json::array({{{"input", 3e38}}})); }, "normalized value must be finite");
    }
}

void test_input_edges() {
    tzrec::FeatureParser parser(raw_config());
    for (const json value : {json(" "), json("+"), json("1e"), json("1 2"), json("0x1"),
             json("Infinity"), json("1e999"), json::object(), json::array({nullptr}),
             json::array({true}), json::array({json::array({1})})}) {
        rejects([&] { parser.parse(json::array({{{"input", value}}})); }, "Feature 'price'");
    }
    for (const json rows : {json(nullptr), json::object(), json(1), json("input")}) {
        rejects([&] { parser.parse(rows); }, "non-empty array");
    }
    auto parsed = parser.parse(json::array({{{"input", " \t+1.25e1 \r\n"}}, {{"input", "-.5"}},
                 {{"input", std::numeric_limits<float>::max()}}, {{"input", -std::numeric_limits<float>::max()}}}));
    require_dense(parsed[0], 1, {12.5f, -0.5f, std::numeric_limits<float>::max(), -std::numeric_limits<float>::max()},
                  "decimal syntax and float32 endpoints");
    auto cfg = raw_config();
    cfg["features"][0]["boundaries"] = json::array();
    require_dense(tzrec::FeatureParser(cfg).parse(json::array({{{"input", 1}}}))[0], 1, {1}, "empty boundaries stay dense");

    cfg = config("num_buckets");
    cfg["features"][0]["separator"] = "|";
    tzrec::FeatureParser ids(cfg);
    parsed = ids.parse(json::array({{{"input", " 0 | +99 \t"}}}));
    require(parsed[0].values == std::vector<int64_t>({0, 99}) && parsed[0].lengths == std::vector<int32_t>({2}),
            "separator and integer whitespace");
    for (const json value : {json("|1"), json("1|"), json("1||2"), json("oops"),
             json(UINT64_MAX), json::object(), json::array({1})}) {
        rejects([&] { ids.parse(json::array({{{"input", value}}})); }, "Feature 'encoded'");
    }
    cfg = config("num_buckets");
    cfg["features"][0]["num_buckets"] = INT64_MAX;
    tzrec::FeatureParser large_ids(cfg);
    parsed = large_ids.parse(json::array({{{"input", std::to_string(INT64_MAX - 1)}}}));
    require(parsed[0].values == std::vector<int64_t>({INT64_MAX - 1}), "maximum bucket count");
    rejects([&] { large_ids.parse(json::array({{{"input", std::to_string(INT64_MAX)}}})); }, "ID outside");
    unsetenv("USE_FARM_HASH_TO_BUCKETIZE");
    tzrec::FeatureParser default_hash(config("hash_bucket_size"));
    setenv("USE_FARM_HASH_TO_BUCKETIZE", "TrUe", 1);
    tzrec::FeatureParser explicit_hash(config("hash_bucket_size"));
    const auto rows = json::array({{{"input", "Action\x1d" "Comedy"}}, {{"input", json::array({"Action", "Comedy"})}}});
    auto hashes = default_hash.parse(rows);
    require(hashes[0].values.size() == 4 && hashes[0].values[0] == hashes[0].values[2] &&
            hashes[0].values[1] == hashes[0].values[3] && hashes[0].lengths == std::vector<int32_t>({2, 2}) &&
            hashes[0].values == explicit_hash.parse(rows)[0].values, "hash input forms and environment defaults");
    setenv("USE_FARM_HASH_TO_BUCKETIZE", "true", 1);
}

void test_boundaries() {
    auto cfg = raw_config("0.5");
    cfg["features"][0]["boundaries"] = json::array({0, 1, 2});
    auto parsed = tzrec::FeatureParser(cfg).parse(json::array({{{"input", -1}}, {{"input", 0}}, {{"input", 0.5}},
                  {{"input", 1}}, {{"input", 1.5}}, {{"input", 2}}, {{"input", 3}}, {{"input", nullptr}}}));
    require(parsed[0].value_dim == 0 && parsed[0].dense_values.empty() &&
            parsed[0].values == std::vector<int64_t>({0, 1, 1, 2, 2, 3, 3, 1}) &&
            parsed[0].lengths == std::vector<int32_t>(8, 1), "all bucket intervals and equalities");
    cfg["features"][0]["normalizer"] = "method=zscore,mean=1,standard_deviation=2";
    parsed = tzrec::FeatureParser(cfg).parse(json::array({{{"input", -1}}, {{"input", 1}}, {{"input", 3}},
                  {{"input", 5}}, {{"input", nullptr}}}));
    require(parsed[0].values == std::vector<int64_t>({0, 1, 2, 3, 1}) &&
            parsed[0].lengths == std::vector<int32_t>(5, 1), "normalize before bucketization/default bypass");
    cfg["features"][0]["default_value"] = "";
    parsed = tzrec::FeatureParser(cfg).parse(json::array({{{"input", nullptr}}, {{"input", ""}},
                  {{"input", json::array()}}, json::object()}));
    require(parsed[0].values == std::vector<int64_t>(4, 1) && parsed[0].lengths == std::vector<int32_t>(4, 1) &&
            parsed[0].value_dim == 0 && parsed[0].dense_values.empty(), "scalar raw empty default uses unnormalized zero");
    cfg["features"][0]["value_dim"] = 2;
    parsed = tzrec::FeatureParser(cfg).parse(json::array({{{"input", nullptr}}, {{"input", json::array()}}}));
    require(parsed[0].values.empty() && parsed[0].lengths == std::vector<int32_t>({0, 0}), "vector raw empty default stays empty");
    cfg["features"][0]["value_dim"] = 1;
    cfg["features"][0]["boundaries"] = json::array({0});
    parsed = tzrec::FeatureParser(cfg).parse(json::array({{{"input", nullptr}}, {{"input", 0}}}));
    require(parsed[0].values == std::vector<int64_t>({1, 0}) && parsed[0].lengths == std::vector<int32_t>({1, 1}),
            "missing zero bypasses normalization while explicit zero is normalized");
}

void test_id_defaults() {
    for (const auto& kind : {"num_buckets", "hash_bucket_size"}) {
        for (const auto& separator : {"||", "::", "\xe4\xb8\xad"}) {
            auto cfg = config(kind);
            cfg["features"][0]["separator"] = separator;
            rejects([&] { tzrec::FeatureParser parser(cfg); }, "single ASCII");
            cfg = raw_config();
            cfg["features"][0]["separator"] = separator;
            rejects([&] { tzrec::FeatureParser parser(cfg); }, "single ASCII");
        }
    }
    auto cfg = config("hash_bucket_size", "1|2");
    cfg["features"][0]["separator"] = "|";
    auto parsed = tzrec::FeatureParser(cfg).parse(json::array({{{"input", "3|4"}}, {{"input", ""}},
                  {{"input", nullptr}}, {{"input", json::array()}}, json::object()}));
    require(parsed[0].values == std::vector<int64_t>({21, 43, 11, 11, 11, 11}) &&
            parsed[0].lengths == std::vector<int32_t>({2, 1, 1, 1, 1}), "hash default is one token");
    cfg = config("num_buckets", "1");
    cfg["features"][0]["separator"] = "1";
    parsed = tzrec::FeatureParser(cfg).parse(json::array({{{"input", "2"}}, {{"input", nullptr}}}));
    require(parsed[0].values == std::vector<int64_t>({2, 1}), "default containing separator is not split");
}

void test_mixed_features_and_warmup() {
    auto cfg = config("num_buckets", "3");
    auto dense = raw_config("1|2")["features"][0];
    dense.update({{"expression", "item:vector"}, {"value_dim", 2}, {"separator", "|"}});
    auto bucket = raw_config("0")["features"][0];
    bucket.update({{"feature_name", "bucket"}, {"expression", "item:score"}, {"boundaries", json::array({0, 1})}});
    cfg["features"].push_back(dense);
    cfg["features"].push_back(bucket);
    tzrec::FeatureParser parser(cfg);
    auto parsed = parser.parse(json::array({{{"input", "1\x1d" "2"}, {"vector", json::array({3, 4})},
                  {"score", 1}, {"metadata", json::object()}}, json::object()}));
    require(parsed.size() == 3 && parsed[0].name == "encoded" && parsed[1].name == "price" && parsed[2].name == "bucket",
            "mixed features retain config order and ignore metadata");
    require(parsed[0].values == std::vector<int64_t>({1, 2, 3}) && parsed[0].lengths == std::vector<int32_t>({2, 1}),
            "mixed sparse rows");
    require_dense(parsed[1], 2, {3, 4, 1, 2}, "mixed dense rows");
    require(parsed[2].values == std::vector<int64_t>({2, 1}) && parsed[2].lengths == std::vector<int32_t>({1, 1}),
            "mixed bucket rows");
    auto warmup = parser.warmup_row();
    require(warmup == json({{"input", "0"}, {"vector", nullptr}, {"score", nullptr}}), "warmup input mapping");
    parsed = parser.parse(json::array({warmup}));
    require(parsed[0].values == std::vector<int64_t>({0}) && parsed[0].lengths == std::vector<int32_t>({1}), "warmup ID");
    require_dense(parsed[1], 2, {1, 2}, "warmup dense default");
    require(parsed[2].values == std::vector<int64_t>({1}), "warmup bucket default");
    rejects([&] { parser.parse(json::array({{{"input", 1}, {"vector", json::array({1, 2})}}})); }, "missing input column: score");
}

void test_value_limits() {
    const std::string limit_error = "Feature value count exceeds 65536";
    tzrec::FeatureParser ids(config("num_buckets"));
    json full = std::vector<std::string>(65536, "1");
    auto parsed = ids.parse(json::array({{{"input", full}}}));
    require(parsed[0].values == std::vector<int64_t>(65536, 1) && parsed[0].lengths == std::vector<int32_t>({65536}),
            "ID count at limit");
    full.push_back("1");
    rejects([&] { ids.parse(json::array({{{"input", full}}})); }, limit_error);
    json half = std::vector<std::string>(32768, "1");
    parsed = ids.parse(json::array({{{"input", half}}, {{"input", half}}}));
    require(parsed[0].values.size() == 65536 && parsed[0].lengths == std::vector<int32_t>({32768, 32768}), "ID limit across rows");
    rejects([&] { ids.parse(json::array({{{"input", half}}, {{"input", half}}, {{"input", "1"}}})); }, limit_error);

    auto cfg = config("num_buckets");
    auto second = cfg["features"][0];
    second.update({{"feature_name", "second"}, {"expression", "item:other"}});
    cfg["features"].push_back(second);
    tzrec::FeatureParser two_ids(cfg);
    parsed = two_ids.parse(json::array({{{"input", half}, {"other", half}}}));
    require(parsed.size() == 2 && parsed[0].values.size() == 32768 && parsed[1].values.size() == 32768,
            "ID limit across features");
    auto over_half = half;
    over_half.push_back("1");
    rejects([&] { two_ids.parse(json::array({{{"input", half}, {"other", over_half}}})); }, "Feature 'second': " + limit_error);

    std::string defaults = "0";
    for (size_t i = 1; i < 32768; ++i) defaults += "|0";
    cfg = config("num_buckets");
    auto dense = raw_config(defaults)["features"][0];
    dense.update({{"expression", "item:vector"}, {"value_dim", 32768}, {"separator", "|"}});
    cfg["features"].push_back(dense);
    tzrec::FeatureParser mixed(cfg);
    parsed = mixed.parse(json::array({{{"input", half}, {"vector", nullptr}}}));
    require(parsed[0].values.size() == 32768, "mixed sparse count at limit");
    require_dense(parsed[1], 32768, std::vector<float>(32768, 0), "mixed dense count at limit");
    rejects([&] { mixed.parse(json::array({{{"input", over_half}, {"vector", nullptr}}})); }, "Feature 'price': " + limit_error);
    std::swap(cfg["features"][0], cfg["features"][1]);
    rejects([&] { tzrec::FeatureParser(cfg).parse(json::array({{{"input", over_half}, {"vector", nullptr}}})); },
            "Feature 'encoded': " + limit_error);

    cfg = raw_config(defaults + "|" + defaults);
    cfg["features"][0].update({{"value_dim", 65536}, {"separator", "|"}});
    tzrec::FeatureParser max_dim(cfg);
    parsed = max_dim.parse(json::array({{{"input", nullptr}}}));
    require_dense(parsed[0], 65536, std::vector<float>(65536, 0), "maximum dense dimension");
    rejects([&] { max_dim.parse(json::array({{{"input", nullptr}}, json::object()})); }, limit_error);
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
        setenv("USE_FARM_HASH_TO_BUCKETIZE", "true", 1);
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
        rejects([&] { tzrec::FeatureParser parser(separated); }, "expected an int64 ID");
        for (const auto& key : {"weighted", "vocab_list", "sequence_fields", "unknown"}) {
            auto invalid = config("num_buckets");
            invalid["features"][0][key] = true;
            rejects([&] { tzrec::FeatureParser parser(invalid); });
        }
        for (const json bucket : {json(0), json(uint64_t(0)), json(-1), json(1.5), json(true), json(UINT64_MAX)}) {
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
        require_dense(numbers[0], 1, {0.01f, 0.02f, 0.1f}, "normalizer/default parity");
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
        test_config_validation();
        test_normalizers();
        test_input_edges();
        test_boundaries();
        test_id_defaults();
        test_mixed_features_and_warmup();
        test_value_limits();
        std::cout << "Feature parser tests passed" << std::endl;
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << std::endl;
        return 1;
    }
}
