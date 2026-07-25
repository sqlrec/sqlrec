// ONNX Runtime-based inference server for GBDT models (LightGBM, XGBoost, etc.).
//
// Loads model.onnx + schema.json from a local directory (typically mirrored
// from remote storage by the container entrypoint), then exposes POST /predict
// and GET /health endpoints via httplib.
//
// Usage: onnx_server <model_dir> <port>
//
// Both LightGBM and XGBoost export float-only ONNX models (no categorical
// features), so all input values are cast directly to float32.
//
// Request body: a JSON array of objects (row-wise).
// Response body: {"probs": [p1, p2, ...]}.

#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <fstream>

#include "httplib.h"
#include "json.hpp"

#include <onnxruntime_cxx_api.h>

using json = nlohmann::json;

namespace {

Ort::Env g_env{ORT_LOGGING_LEVEL_WARNING, "onnx_server"};
Ort::Session* g_session = nullptr;
std::vector<std::string> g_feature_columns;
std::string g_input_name;
std::string g_output_name;
httplib::Server* g_server = nullptr;  // for signal handler access

std::string read_file(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        throw std::runtime_error("Failed to open file: " + path);
    }
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

void load_schema(const std::string& schema_path) {
    json schema = json::parse(read_file(schema_path));
    g_feature_columns.clear();
    for (const auto& f : schema["feature_columns"]) {
        g_feature_columns.push_back(f.get<std::string>());
    }
}

void init_model(const std::string& model_dir) {
    std::string model_path = model_dir + "/model.onnx";
    std::string schema_path = model_dir + "/schema.json";

    std::cout << "Loading schema from " << schema_path << std::endl;
    load_schema(schema_path);

    std::cout << "Loading ONNX model from " << model_path << std::endl;
    Ort::SessionOptions session_opts;
    session_opts.SetIntraOpNumThreads(1);
    g_session = new Ort::Session(g_env, model_path.c_str(), session_opts);

    Ort::AllocatorWithDefaultOptions allocator;
    auto input_name_alloc = g_session->GetInputNameAllocated(0, allocator);
    g_input_name = input_name_alloc.get();

    // Select the probability output. ONNX classifiers exported with
    // zipmap=False produce two tensor outputs:
    //   output 0 = predicted label  (int64, shape (batch,))
    //   output 1 = probabilities     (float, shape (batch, n_classes))
    // Pick the first float-tensor output so we read probabilities, not labels.
    // (For regression models there is a single float output, which is also
    // correctly selected here.)
    size_t n_outputs = g_session->GetOutputCount();
    bool found = false;
    for (size_t i = 0; i < n_outputs; ++i) {
        auto type_info = g_session->GetOutputTypeInfo(i);
        // Skip non-tensor outputs (e.g. seq<map> from default ZipMap).
        if (type_info.GetONNXType() != ONNX_TYPE_TENSOR) {
            continue;
        }
        auto tensor_info = type_info.GetTensorTypeAndShapeInfo();
        if (tensor_info.GetElementType() == ONNX_TENSOR_ELEMENT_DATA_TYPE_FLOAT) {
            auto name_alloc = g_session->GetOutputNameAllocated(i, allocator);
            g_output_name = name_alloc.get();
            found = true;
            std::cout << "Selected probability output: index=" << i
                      << ", name=" << g_output_name << std::endl;
            break;
        }
    }
    if (!found) {
        // Fallback: use output 0 (regression or older export without zipmap).
        auto name_alloc = g_session->GetOutputNameAllocated(0, allocator);
        g_output_name = name_alloc.get();
        std::cout << "Warning: no float tensor output found, using output 0: "
                  << g_output_name
                  << " (re-export the model with zipmap=False if this is a "
                  << "classifier)" << std::endl;
    }

    std::cout << "Model loaded. Input name=" << g_input_name
              << ", n_features=" << g_feature_columns.size() << std::endl;
}

std::vector<float> build_input_tensor(const json& rows) {
    size_t batch = rows.size();
    size_t n_features = g_feature_columns.size();
    std::vector<float> tensor(batch * n_features, 0.0f);
    for (size_t i = 0; i < batch; ++i) {
        const auto& row = rows[i];
        if (!row.is_object()) {
            throw std::runtime_error("Each array element must be a JSON object");
        }
        for (size_t j = 0; j < n_features; ++j) {
            const std::string& col = g_feature_columns[j];
            if (!row.contains(col) || row[col].is_null()) {
                continue;
            }
            const auto& v = row[col];
            if (v.is_number()) {
                tensor[i * n_features + j] = v.get<float>();
            } else {
                try {
                    tensor[i * n_features + j] = std::stof(
                            v.is_string() ? v.get<std::string>() : v.dump());
                } catch (...) {
                    // leave as 0
                }
            }
        }
    }
    return tensor;
}

json predict(const json& request_data) {
    if (g_session == nullptr) {
        return json{{"error", "Model not initialized"}};
    }
    if (!request_data.is_array() || request_data.empty()) {
        return json{{"error", "Input data must be a non-empty JSON array"}};
    }

    size_t batch = request_data.size();
    size_t n_features = g_feature_columns.size();
    auto input_values = build_input_tensor(request_data);

    std::array<int64_t, 2> input_shape{static_cast<int64_t>(batch), static_cast<int64_t>(n_features)};
    Ort::MemoryInfo memory_info = Ort::MemoryInfo::CreateCpu(OrtArenaAllocator, OrtMemTypeDefault);
    Ort::Value input_tensor = Ort::Value::CreateTensor<float>(
        memory_info, input_values.data(), input_values.size(),
        input_shape.data(), input_shape.size());

    const char* input_names[] = {g_input_name.c_str()};
    const char* output_names[] = {g_output_name.c_str()};

    auto outputs = g_session->Run(Ort::RunOptions{nullptr}, input_names, &input_tensor, 1, output_names, 1);
    if (outputs.empty()) {
        return json{{"error", "Model returned no outputs"}};
    }

    Ort::Value& out = outputs.front();
    auto type_and_shape = out.GetTensorTypeAndShapeInfo();
    auto shape = type_and_shape.GetShape();
    float* data = out.GetTensorMutableData<float>();
    size_t total = type_and_shape.GetElementCount();

    // The probability output may be:
    //   (batch, n_classes) — classifier with n_classes columns [P(0), P(1), ...]
    //   (batch, 1)         — single-column output (regression / single class)
    //   (batch,)           — flat output (regression)
    // For binary classification we return P(positive class) = last column.
    json probs = json::array();
    if (shape.size() == 2) {
        // n_classes may be concrete (e.g. 2) or dynamic (-1); derive it.
        size_t n_classes = (shape[1] > 0)
                ? static_cast<size_t>(shape[1])
                : (batch > 0 ? total / batch : 1);
        if (total < batch || total % batch != 0 || n_classes == 0) {
            return json{{"error", "Output count " + std::to_string(total) +
                        " inconsistent with batch size " + std::to_string(batch)}};
        }
        if (n_classes >= 2) {
            // Classifier: return P(positive class) = last column.
            for (size_t i = 0; i < batch; ++i) {
                probs.push_back(data[i * n_classes + (n_classes - 1)]);
            }
        } else {
            for (size_t i = 0; i < batch; ++i) probs.push_back(data[i]);
        }
    } else if (shape.size() == 1) {
        if (total != batch) {
            return json{{"error", "Output count " + std::to_string(total) +
                        " does not match batch size " + std::to_string(batch)}};
        }
        for (size_t i = 0; i < total; ++i) probs.push_back(data[i]);
    } else {
        return json{{"error", "Unexpected output shape: expected (batch, n) or (batch,), got rank=" +
                    std::to_string(shape.size())}};
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
        delete g_session;
        g_session = nullptr;
        return -1;
    }

    int port;
    try {
        port = std::stoi(argv[2]);
    } catch (const std::exception& e) {
        std::cerr << "Invalid port: " << argv[2] << std::endl;
        delete g_session;
        g_session = nullptr;
        return -1;
    }

    httplib::Server svr;

    // Graceful shutdown: K8s sends SIGTERM during pod termination. Use a
    // global server pointer so the signal handler can call svr.stop(),
    // which unblocks listen() and allows in-flight requests to complete.
    g_server = &svr;
    std::signal(SIGTERM, [](int) {
        if (g_server) g_server->stop();
    });
    std::signal(SIGINT, [](int) {
        if (g_server) g_server->stop();
    });

    svr.Post("/predict", [&](const httplib::Request& req, httplib::Response& res) {
        try {
            auto body = json::parse(req.body);
            auto result = predict(body);
            if (result.contains("error")) {
                res.status = 400;
            }
            res.set_content(result.dump(), "application/json");
        } catch (const std::exception& e) {
            res.status = 500;
            res.set_content(json{{"error", e.what()}}.dump(), "application/json");
        }
    });
    svr.Get("/health", [&](const httplib::Request&, httplib::Response& res) {
        res.set_content(json{{"status", "ok"}}.dump(), "application/json");
    });

    std::cout << "ONNX server listening on http://0.0.0.0:" << port << std::endl;
    if (!svr.listen("0.0.0.0", port)) {
        std::cerr << "Failed to listen on port " << port << std::endl;
        delete g_session;
        g_session = nullptr;
        return -1;
    }

    delete g_session;
    g_session = nullptr;
    return 0;
}
