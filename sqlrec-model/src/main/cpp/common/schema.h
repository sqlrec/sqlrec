#pragma once

#include <set>
#include <string>
#include <vector>

#include "json.hpp"
#include "utils.h"

struct Schema {
    std::vector<std::string> feature_columns;
    std::set<std::string> categorical_features;
    std::vector<int> cat_feature_indices;
    std::string objective = "binary";

    void load(const std::string& schema_path) {
        auto data = nlohmann::json::parse(read_file(schema_path));

        feature_columns.clear();
        categorical_features.clear();
        cat_feature_indices.clear();
        objective = data.value("objective", "binary");

        for (const auto& f : data["feature_columns"]) {
            feature_columns.push_back(f.get<std::string>());
        }
        if (data.contains("categorical_features")) {
            for (const auto& f : data["categorical_features"]) {
                categorical_features.insert(f.get<std::string>());
            }
        }
        for (size_t i = 0; i < feature_columns.size(); ++i) {
            if (categorical_features.count(feature_columns[i])) {
                cat_feature_indices.push_back(static_cast<int>(i));
            }
        }
    }
};
