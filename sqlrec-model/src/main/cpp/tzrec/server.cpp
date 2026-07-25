#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <sstream>
#include <cstdint>
#include <cstdlib>
#include <functional>

#include "httplib.h"
#include "json.hpp"

#include <torch/script.h>
#include <torch/torch.h>

using json = nlohmann::json;

torch::jit::script::Module g_module;
bool g_model_initialized = false;
torch::Device g_device = torch::kCPU;

torch::Device get_device() {
    int rank = 0;
    const char* local_rank = std::getenv("LOCAL_RANK");
    if (local_rank != nullptr) {
        rank = std::atoi(local_rank);
    }
    if (torch::cuda::is_available()) {
        return torch::Device(torch::kCUDA, rank);
    } else {
        return torch::Device(torch::kCPU);
    }
}

bool init_model(const std::string& model_path) {
    std::cout << "Loading model from " << model_path << std::endl;
    g_device = get_device();
    std::cout << "Using device: " << g_device << std::endl;
    try {
        // need fix error: Unknown builtin op: fbgemm::bounds_check_indices
        g_module = torch::jit::load(model_path);
        g_module.eval();
        g_model_initialized = true;
        std::cout << "Model loaded successfully" << std::endl;
        return true;
    } catch (const c10::Error& e) {
        std::cerr << "Error loading model: " << e.what() << std::endl;
        return false;
    }
}

int64_t hash_feature(const std::string& value) {
    int64_t hash = 1469598103934665603ULL;
    for (char c : value) {
        hash ^= static_cast<int64_t>(static_cast<unsigned char>(c));
        hash *= 1099511628211ULL;
    }
    return hash;
}

int64_t hash_feature(int64_t value) {
    return hash_feature(std::to_string(value));
}

json build_nested_json(const std::vector<float>& data, const std::vector<int64_t>& shape, int dim, int64_t& index) {
    json arr = json::array();
    int64_t current_dim_size = shape[dim];

    if (dim == static_cast<int>(shape.size()) - 1) {
        for (int64_t i = 0; i < current_dim_size; ++i) {
            arr.push_back(data[index++]);
        }
    } else {
        for (int64_t i = 0; i < current_dim_size; ++i) {
            arr.push_back(build_nested_json(data, shape, dim + 1, index));
        }
    }
    return arr;
}

json tensor_to_json(const at::Tensor& tensor) {
    at::Tensor cpu_tensor = tensor.to(torch::kCPU).contiguous();
    auto sizes = cpu_tensor.sizes();
    std::vector<int64_t> shape(sizes.begin(), sizes.end());

    std::vector<float> data(cpu_tensor.numel());
    std::copy(cpu_tensor.data_ptr<float>(), cpu_tensor.data_ptr<float>() + cpu_tensor.numel(), data.begin());

    int64_t index = 0;
    return build_nested_json(data, shape, 0, index);
}

enum class FieldType {
    Int64,
    String,
    Int64List,
    StringList,
    Unknown
};

FieldType detect_field_type(const json& value) {
    if (value.is_number_integer()) {
        return FieldType::Int64;
    } else if (value.is_string()) {
        return FieldType::String;
    } else if (value.is_array()) {
        if (value.empty()) {
            return FieldType::Unknown;
        }
        const auto& first = value[0];
        if (first.is_number_integer()) {
            return FieldType::Int64List;
        } else if (first.is_string()) {
            return FieldType::StringList;
        }
    }
    return FieldType::Unknown;
}

c10::Dict<std::string, at::Tensor> parse_input(const json& request_data) {
    size_t batch_size = request_data.size();
    if (batch_size == 0) {
        return c10::Dict<std::string, at::Tensor>();
    }

    std::set<std::string> all_fields;
    for (const auto& record : request_data) {
        for (auto it = record.begin(); it != record.end(); ++it) {
            all_fields.insert(it.key());
        }
    }

    std::map<std::string, FieldType> field_types;
    for (const auto& field_name : all_fields) {
        for (const auto& record : request_data) {
            if (record.contains(field_name)) {
                FieldType type = detect_field_type(record[field_name]);
                if (type != FieldType::Unknown) {
                    field_types[field_name] = type;
                    break;
                }
            }
        }
    }

    c10::Dict<std::string, at::Tensor> result;

    for (const auto& field_entry : field_types) {
        const std::string& field_name = field_entry.first;
        FieldType field_type = field_entry.second;

        std::vector<int64_t> values;
        std::vector<int32_t> lengths;

        for (size_t i = 0; i < batch_size; ++i) {
            const auto& record = request_data[i];

            if (!record.contains(field_name) || record[field_name].is_null()) {
                lengths.push_back(0);
                continue;
            }

            const auto& value = record[field_name];

            switch (field_type) {
                case FieldType::Int64: {
                    int64_t v = value.get<int64_t>();
                    if (v < 0 || v >= 1000000) {
                        v = 0;
                    }
                    values.push_back(hash_feature(v));
                    lengths.push_back(1);
                    break;
                }
                case FieldType::String: {
                    std::string v = value.get<std::string>();
                    values.push_back(hash_feature(v));
                    lengths.push_back(1);
                    break;
                }
                case FieldType::Int64List: {
                    int32_t len = static_cast<int32_t>(value.size());
                    lengths.push_back(len);
                    for (const auto& item : value) {
                        int64_t v = item.get<int64_t>();
                        if (v < 0 || v >= 1000000) {
                            v = 0;
                        }
                        values.push_back(hash_feature(v));
                    }
                    break;
                }
                case FieldType::StringList: {
                    int32_t len = static_cast<int32_t>(value.size());
                    lengths.push_back(len);
                    for (const auto& item : value) {
                        std::string v = item.get<std::string>();
                        values.push_back(hash_feature(v));
                    }
                    break;
                }
                default:
                    lengths.push_back(0);
                    break;
            }
        }

        std::string values_key = field_name + ".values";
        std::string lengths_key = field_name + ".lengths";

        at::Tensor values_tensor = torch::from_blob(
            values.data(),
            {static_cast<int64_t>(values.size())},
            torch::kInt64).clone().to(g_device);
        at::Tensor lengths_tensor = torch::from_blob(
            lengths.data(),
            {static_cast<int64_t>(batch_size)},
            torch::kInt32).clone().to(g_device);

        result.insert(values_key, values_tensor);
        result.insert(lengths_key, lengths_tensor);
    }

    return result;
}

json predict(const json& request_data) {
    try {
        if (!g_model_initialized) {
            return json{{"error", "Model not initialized"}};
        }

        size_t batch_size = request_data.size();
        if (batch_size == 0) {
            return json{{"error", "Input data is empty"}};
        }

        auto dict = parse_input(request_data);

        std::vector<c10::IValue> input;
        input.push_back(c10::IValue(dict));

        auto output_dict = g_module.forward(input).toGenericDict();

        json result = json::object();
        for (const auto& item : output_dict) {
            const std::string& key = item.key().toStringRef();
            at::Tensor tensor = item.value().toTensor();
            result[key] = tensor_to_json(tensor);
        }

        return result;

    } catch (const std::exception& e) {
        return json{{"error", std::string(e.what())}};
    }
}

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <model_path> <port>" << std::endl;
        return -1;
    }

    std::string model_path = argv[1];
    int port = std::stoi(argv[2]);

    if (!init_model(model_path)) {
        std::cerr << "Failed to initialize model" << std::endl;
        return -1;
    }

    std::cout << "Starting HTTP server on port " << port << std::endl;

    httplib::Server svr;

    svr.Post("/predict", [&](const httplib::Request& req, httplib::Response& res) {
        std::cout << "Predict request received" << std::endl;

        try {
            auto request_data = json::parse(req.body);

            if (!request_data.is_array()) {
                res.set_content(json{{"error", "Input data must be a JSON array"}}.dump(), "application/json");
                res.status = 400;
                return;
            }

            if (request_data.empty()) {
                res.set_content(json{{"error", "Input data is empty"}}.dump(), "application/json");
                res.status = 400;
                return;
            }

            auto result = predict(request_data);
            res.set_content(result.dump(), "application/json");

        } catch (const json::parse_error& e) {
            res.set_content(json{{"error", std::string("JSON parse error: ") + e.what()}}.dump(), "application/json");
            res.status = 400;
        } catch (const std::exception& e) {
            res.set_content(json{{"error", std::string(e.what())}}.dump(), "application/json");
            res.status = 500;
        }
    });

    svr.Get("/health", [&](const httplib::Request& req, httplib::Response& res) {
        res.set_content(json{{"status", "ok"}}.dump(), "application/json");
    });

    std::cout << "Server listening on http://0.0.0.0:" << port << std::endl;
    svr.listen("0.0.0.0", port);

    return 0;
}
