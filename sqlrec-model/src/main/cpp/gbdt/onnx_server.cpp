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
#include <string>
#include <vector>
#include <cstdint>
#include <cstdlib>

#include "httplib.h"
#include "json.hpp"
#include "schema.h"
#include "utils.h"

#include <onnxruntime_cxx_api.h>

using json = nlohmann::json;

namespace {

Ort::Env g_env{ORT_LOGGING_LEVEL_WARNING, "onnx_server"};
Ort::Session* g_session = nullptr;
Schema g_schema;
std::string g_input_name;
std::string g_output_name;

void init_model(const std::string& model_dir) {
    std::string model_path = model_dir + "/model.onnx";
    std::string schema_path = model_dir + "/schema.json";

    std::cout << "Loading schema from " << schema_path << std::endl;
    g_schema.load(schema_path);

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
        // No float tensor output found. This typically means the model was
        // exported with zipmap=True (the ONNXMLTools default), whose outputs
        // are an int64 label tensor and a seq<map> of probabilities - neither
        // is a float tensor we can read. Falling back to output 0 would feed
        // the int64 label bytes to GetTensorMutableData<float>(), which does
        // NO runtime type check and would silently return garbage "probs" with
        // HTTP 200. Fail fast at load time instead, and ask the user to
        // re-export with zipmap=False so a real float probabilities tensor is
        // exposed. (Pure regression models have a single float output and are
        // already selected by the loop above, so they never reach here.)
        throw std::runtime_error(
            "No float tensor output found in ONNX model '" + model_path +
            "'. Re-export the classifier with zipmap=False (e.g. "
            "option zipmap=False in skl2onnx) so the model exposes a float "
            "probabilities tensor.");
    }

    std::cout << "Model loaded. Input name=" << g_input_name
              << ", n_features=" << g_schema.feature_columns.size() << std::endl;
}

std::vector<float> build_input_tensor(const json& rows) {
    size_t batch = rows.size();
    size_t n_features = g_schema.feature_columns.size();
    std::vector<float> tensor(batch * n_features, 0.0f);
    for (size_t i = 0; i < batch; ++i) {
        const auto& row = rows[i];
        if (!row.is_object()) {
            throw std::runtime_error("Each array element must be a JSON object");
        }
        for (size_t j = 0; j < n_features; ++j) {
            tensor[i * n_features + j] = get_float_value(row, g_schema.feature_columns[j]);
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
    size_t n_features = g_schema.feature_columns.size();
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
    setup_graceful_shutdown(svr);
    register_predict_endpoint(svr, predict);
    register_health_endpoint(svr);

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
