#include <cstring>
#include <dlfcn.h>
#include <filesystem>
#include <iostream>
#include <memory>
#include <vector>

#include <ATen/Parallel.h>
#include <c10/core/InferenceMode.h>
#include <torch/script.h>
#include <torch/csrc/jit/runtime/operator.h>

#include "feature_parser.h"
#include "utils.h"

using json = nlohmann::json;
namespace fs = std::filesystem;

namespace {
torch::jit::Module module;
std::unique_ptr<tzrec::FeatureParser> parser;

void load_operators() {
    const fs::path directory = FBGEMM_LIBRARY_DIR;
    // Older wheels bundle registration in fbgemm_gpu_py; newer wheels split TBE ops.
    std::vector<std::string> libraries;
    if (fs::exists(directory / "fbgemm_gpu_tbe_training_forward.so")) {
        libraries = {"fbgemm_gpu_tbe_training_forward.so", "fbgemm_gpu_tbe_inference.so"};
    }
    libraries.push_back("fbgemm_gpu_py.so");
    for (const auto& library : libraries) {
        auto path = (directory / library).string();
        // Keep operator registration alive for the lifetime of the process.
        if (!dlopen(path.c_str(), RTLD_NOW | RTLD_GLOBAL)) {
            throw std::runtime_error("Failed to load " + path + ": " + dlerror());
        }
    }
    for (const char* name : {"fbgemm::asynchronous_complete_cumsum", "fbgemm::jagged_to_padded_dense",
                             "fbgemm::bounds_check_indices", "fbgemm::int_nbit_split_embedding_codegen_lookup_function",
                             "fbgemm::keyed_jagged_index_select_dim1", "fbgemm::permute_1D_sparse_data",
                             "fbgemm::permute_2D_sparse_data", "fbgemm::segment_sum_csr"}) {
        if (torch::jit::getAllOperatorsFor(c10::Symbol::fromQualString(name)).empty()) {
            throw std::runtime_error(std::string("FBGEMM operator not registered: ") + name);
        }
    }
}

c10::Dict<std::string, at::Tensor> tensors(const json& rows) {
    c10::Dict<std::string, at::Tensor> result;
    for (const auto& feature : parser->parse(rows)) {
        if (feature.value_dim) {
            auto values = at::empty({static_cast<int64_t>(rows.size()), static_cast<int64_t>(feature.value_dim)}, at::kFloat);
            std::memcpy(values.data_ptr<float>(), feature.dense_values.data(), feature.dense_values.size() * sizeof(float));
            result.insert(feature.name + ".values", values);
            continue;
        }
        auto values = at::empty({static_cast<int64_t>(feature.values.size())}, at::kLong);
        auto lengths = at::empty({static_cast<int64_t>(feature.lengths.size())}, at::kInt);
        if (!feature.values.empty()) {
            std::memcpy(values.data_ptr<int64_t>(), feature.values.data(), feature.values.size() * sizeof(int64_t));
        }
        std::memcpy(lengths.data_ptr<int32_t>(), feature.lengths.data(), feature.lengths.size() * sizeof(int32_t));
        result.insert(feature.name + ".values", values);
        result.insert(feature.name + ".lengths", lengths);
    }
    return result;
}

template <typename T>
json nested_tensor(const at::Tensor& tensor, size_t dimension, int64_t& offset) {
    if (dimension == static_cast<size_t>(tensor.dim())) return tensor.data_ptr<T>()[offset++];
    json result = json::array();
    for (int64_t i = 0; i < tensor.size(dimension); ++i) {
        result.push_back(nested_tensor<T>(tensor, dimension + 1, offset));
    }
    return result;
}

json predict(const json& rows) {
    c10::InferenceMode guard;
    auto output = module.forward({tensors(rows), c10::IValue(c10::Device(c10::kCPU))}).toGenericDict();
    json result = json::object();
    for (const auto& item : output) {
        auto tensor = item.value().toTensor().to(at::kCPU);
        int64_t offset = 0;
        if (tensor.is_floating_point()) {
            if (!at::isfinite(tensor).all().item<bool>()) throw std::runtime_error("Model produced nonfinite output");
            result[item.key().toStringRef()] = nested_tensor<double>(tensor.to(at::kDouble).contiguous(), 0, offset);
        } else if (tensor.scalar_type() == at::kBool) {
            result[item.key().toStringRef()] = nested_tensor<bool>(tensor.contiguous(), 0, offset);
        } else if (!tensor.is_complex()) {
            result[item.key().toStringRef()] = nested_tensor<int64_t>(tensor.to(at::kLong).contiguous(), 0, offset);
        } else {
            throw std::runtime_error("Complex model outputs are not supported");
        }
    }
    return result;
}

json normalize_request(const json& data) {
    if (data.is_array()) return data;
    if (!data.is_object()) throw std::invalid_argument("Input must be an array of objects or a column map");
    size_t batch = 0;
    for (auto it = data.begin(); it != data.end(); ++it) {
        if (!it.value().is_array()) throw std::invalid_argument("Column map values must be lists");
        batch = std::max(batch, it.value().size());
    }
    if (batch > 4096) throw std::invalid_argument("Batch size exceeds 4096");
    for (auto it = data.begin(); it != data.end(); ++it) {
        size_t length = it.value().size();
        if (length != batch && length != 1) throw std::invalid_argument("Column lengths must match or be 1");
    }
    json rows = json::array();
    for (size_t i = 0; i < batch; ++i) {
        json row = json::object();
        for (auto it = data.begin(); it != data.end(); ++it) {
            row[it.key()] = it.value()[it.value().size() == 1 ? 0 : i];
        }
        rows.push_back(std::move(row));
    }
    return rows;
}
}  // namespace

int main(int argc, char** argv) {
    if (argc != 4) {
        std::cerr << "Usage: " << argv[0] << " <local_model_dir> <host> <port>" << std::endl;
        return 1;
    }
    try {
        size_t end = 0;
        int port = std::stoi(argv[3], &end);
        if (end != std::strlen(argv[3]) || port < 1 || port > 65535) {
            throw std::invalid_argument("Port must be in [1, 65535]");
        }
        at::set_num_threads(1);
        at::set_num_interop_threads(1);
        fs::path directory(argv[1]);
        parser = std::make_unique<tzrec::FeatureParser>(json::parse(read_file((directory / "fg.json").string())));
        load_operators();
        module = torch::jit::load((directory / "scripted_model.pt").string(), c10::Device(c10::kCPU));
        module.eval();
        predict(json::array({parser->warmup_row()}));

        httplib::Server server;
        // Quantized embedding graphs may mutate shared bounds-check buffers.
        server.new_task_queue = [] { return new httplib::ThreadPool(1, 1, 64); };
        // Idle keep-alive sockets must not occupy the sole inference worker and
        // starve /health probes. Close each response while preserving serial inference.
        server.set_keep_alive_max_count(1);
        server.set_payload_max_length(16 * 1024 * 1024);
        setup_graceful_shutdown(server);
        register_health_endpoint(server);
        server.Post("/predict", [](const httplib::Request& request, httplib::Response& response) {
            json result;
            try {
                const auto content_type = request.get_header_value("Content-Type");
                auto mime = httplib::detail::trim_copy(content_type.substr(0, content_type.find(';')));
                std::transform(mime.begin(), mime.end(), mime.begin(), [](unsigned char c) { return std::tolower(c); });
                if (mime != "application/json") {
                    throw std::invalid_argument("Content-Type must be application/json");
                }
                auto rows = normalize_request(json::parse(request.body));
                if (rows.size() > 4096) throw std::invalid_argument("Batch size exceeds 4096");
                result = predict(rows);
            } catch (const json::exception& error) {
                response.status = 400;
                result = {{"error", error.what()}};
            } catch (const std::invalid_argument& error) {
                response.status = 400;
                result = {{"error", error.what()}};
            } catch (const std::exception& error) {
                response.status = 500;
                std::cerr << "Prediction failed: " << error.what() << std::endl;
                result = {{"error", "Model prediction failed"}};
            }
            response.set_content(result.dump(), "application/json");
        });
        server.set_error_handler([](const httplib::Request&, httplib::Response& response) {
            if (response.get_header_value("Content-Type").find("application/json") != 0) {
                response.set_content(json({{"error", httplib::status_message(response.status)}}).dump(), "application/json");
            }
        });
        std::cout << "CPU model ready; listening on " << argv[2] << ":" << port << std::endl;
        if (!server.listen(argv[2], port)) throw std::runtime_error("Failed to listen on requested address");
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "Failed to start TZRec server: " << error.what() << std::endl;
        return 1;
    }
}
