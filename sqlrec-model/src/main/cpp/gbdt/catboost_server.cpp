// CatBoost native model inference server.
//
// Loads model.cbm + schema.json from a local directory (typically mirrored
// from remote storage by the container entrypoint), then exposes POST /predict
// and GET /health endpoints via httplib.
//
// Uses the CatBoost C API (c_api.h) which supports categorical
// features natively — no hashing or type conversion needed.
//
// Usage: catboost_server <model_dir> <port>
//
// Request body: a JSON array of objects (row-wise).
// Response body: {"probs": [p1, p2, ...]}.

#include <iostream>
#include <string>
#include <vector>
#include <cmath>
#include <cstdlib>

#include "httplib.h"
#include "json.hpp"
#include "schema.h"
#include "utils.h"

#include <catboost/libs/model_interface/c_api.h>

using json = nlohmann::json;

namespace {

ModelCalcerHandle* g_model = nullptr;
Schema g_schema;

void init_model(const std::string& model_dir) {
    std::string model_path = model_dir + "/model.cbm";
    std::string schema_path = model_dir + "/schema.json";

    std::cout << "Loading schema from " << schema_path << std::endl;
    g_schema.load(schema_path);

    std::cout << "Loading CatBoost model from " << model_path << std::endl;
    g_model = ModelCalcerCreate();
    if (!LoadFullModelFromFile(g_model, model_path.c_str())) {
        const char* error = GetErrorString();
        std::string msg = error ? error : "unknown error";
        throw std::runtime_error("Failed to load CatBoost model: " + msg);
    }

    std::cout << "CatBoost model loaded. n_features=" << g_schema.feature_columns.size()
              << ", n_categorical=" << g_schema.cat_feature_indices.size() << std::endl;
}

json predict(const json& request_data) {
    if (g_model == nullptr) {
        return json{{"error", "Model not initialized"}};
    }
    if (!request_data.is_array() || request_data.empty()) {
        return json{{"error", "Input data must be a non-empty JSON array"}};
    }

    size_t batch = request_data.size();
    size_t n_features = g_schema.feature_columns.size();
    size_t n_cat = g_schema.cat_feature_indices.size();
    size_t n_float = n_features - n_cat;

    // Build float and cat features arrays per the CatBoost C API.
    // CalcModelPrediction requires per-document arrays:
    //   - float_features: const float** (array of batch pointers, each to n_float floats)
    //   - cat_features:   const char*** (array of batch pointers, each to n_cat strings)
    std::vector<float> flat_floats(batch * n_float, 0.0f);
    std::vector<const char*> flat_cats(batch * n_cat, "");
    // Keep string objects alive so the const char* pointers remain valid
    // throughout the CalcModelPrediction call.
    std::vector<std::string> cat_storage(batch * n_cat);

    for (size_t i = 0; i < batch; ++i) {
        const auto& row = request_data[i];
        if (!row.is_object()) {
            return json{{"error", "Each array element must be a JSON object"}};
        }
        size_t float_idx = 0;
        size_t cat_idx = 0;
        for (size_t j = 0; j < n_features; ++j) {
            const std::string& col = g_schema.feature_columns[j];
            bool is_cat = g_schema.categorical_features.count(col) > 0;
            if (is_cat) {
                // Categorical: always consume the slot (even if null) so that
                // cat_idx stays aligned with the categorical feature order.
                cat_storage[i * n_cat + cat_idx] = get_string_value(row, col);
                flat_cats[i * n_cat + cat_idx] = cat_storage[i * n_cat + cat_idx].c_str();
                ++cat_idx;
            } else {
                flat_floats[i * n_float + float_idx] = get_float_value(row, col);
                ++float_idx;
            }
        }
    }

    // Build per-document pointer arrays required by CalcModelPrediction.
    std::vector<const float*> float_ptrs(batch);
    for (size_t i = 0; i < batch; ++i) {
        float_ptrs[i] = &flat_floats[i * n_float];
    }
    std::vector<const char**> cat_ptrs;
    const char*** cat_ptrs_data = nullptr;
    if (n_cat > 0) {
        cat_ptrs.resize(batch);
        for (size_t i = 0; i < batch; ++i) {
            cat_ptrs[i] = &flat_cats[i * n_cat];
        }
        cat_ptrs_data = cat_ptrs.data();
    }

    // CalcModelPrediction returns raw approximations (logits for binary Logloss).
    // Apply sigmoid to convert to probabilities for binary classification.
    std::vector<double> predictions(batch);
    if (!CalcModelPrediction(
            g_model,
            batch,
            float_ptrs.data(), n_float,
            cat_ptrs_data, n_cat,
            predictions.data(), batch)) {
        const char* error = GetErrorString();
        std::string msg = error ? error : "unknown error";
        return json{{"error", "Prediction failed: " + msg}};
    }

    json probs = json::array();
    for (double p : predictions) {
        if (g_schema.objective == "binary") {
            probs.push_back(1.0 / (1.0 + std::exp(-p)));
        } else {
            probs.push_back(p);
        }
    }
    return json{{"probs", probs}};
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <model_dir> <port>" << std::endl;
        return -1;
    }
    std::string model_dir = argv[1];

    try {
        init_model(model_dir);
    } catch (const std::exception& e) {
        std::cerr << "Failed to initialize model: " << e.what() << std::endl;
        if (g_model) {
            ModelCalcerDelete(g_model);
            g_model = nullptr;
        }
        return -1;
    }

    int port;
    try {
        port = std::stoi(argv[2]);
    } catch (const std::exception& e) {
        std::cerr << "Invalid port: " << argv[2] << std::endl;
        if (g_model) {
            ModelCalcerDelete(g_model);
            g_model = nullptr;
        }
        return -1;
    }

    httplib::Server svr;
    setup_graceful_shutdown(svr);
    register_predict_endpoint(svr, predict);
    register_health_endpoint(svr);

    std::cout << "CatBoost server listening on http://0.0.0.0:" << port << std::endl;
    if (!svr.listen("0.0.0.0", port)) {
        std::cerr << "Failed to listen on port " << port << std::endl;
        if (g_model) {
            ModelCalcerDelete(g_model);
            g_model = nullptr;
        }
        return -1;
    }

    if (g_model) {
        ModelCalcerDelete(g_model);
        g_model = nullptr;
    }
    return 0;
}
