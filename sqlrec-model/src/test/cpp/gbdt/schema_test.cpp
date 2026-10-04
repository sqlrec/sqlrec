#include "schema.h"
#include <filesystem>
#include <fstream>
#include <random>
#include <stdexcept>

int main() {
    auto path = std::filesystem::temp_directory_path()
        / ("sqlrec-gbdt-schema-" + std::to_string(std::random_device{}()) + ".json");
    try {
        Schema schema;
        for (const auto& objective : {"binary", "regression", "multiclass", "unknown"}) {
            std::ofstream(path) << nlohmann::json{{"objective", objective},
                {"feature_columns", {"feature"}}}.dump();
            bool rejected = false;
            try { schema.load(path.string()); }
            catch (const std::runtime_error&) { rejected = true; }
            bool supported = std::string(objective) == "binary" || std::string(objective) == "regression";
            if (rejected == supported) throw std::runtime_error("Unexpected objective acceptance");
            if (supported && schema.feature_columns != std::vector<std::string>{"feature"})
                throw std::runtime_error("Supported model lost its feature schema");
        }
    } catch (...) {
        std::filesystem::remove(path);
        throw;
    }
    std::filesystem::remove(path);
}
