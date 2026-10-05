#include "utils.h"
#include <chrono>
#include <iostream>
#include <thread>

using json = nlohmann::json;

void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

int main() {
    require(std::isnan(get_float_value(json::object(), "x")), "Missing numeric input must remain missing");
    require(std::isnan(get_float_value(json{{"x", nullptr}}, "x")), "Null numeric input must remain missing");
    require(get_float_value(json{{"x", 0}}, "x") == 0.0f, "Zero must not be treated as missing");
    require(get_float_value(json{{"x", " -1.25e2 "}}, "x") == -125.0f, "Decimal string conversion failed");
    require(get_string_value(json{{"x", nullptr}}, "x").empty(), "Null category must be empty");
    require(get_string_value(json{{"x", std::numeric_limits<int64_t>::min()}}, "x") == "-9223372036854775808",
            "Signed category lost precision");
    require(get_string_value(json{{"x", std::numeric_limits<uint64_t>::max()}}, "x") == "18446744073709551615",
            "Unsigned category lost precision");
    for (const auto& value : {json(1.0), json(true), json::array({1})}) {
        bool rejected = false;
        try { get_string_value(json{{"x", value}}, "x"); }
        catch (const std::invalid_argument&) { rejected = true; }
        require(rejected, "Invalid categorical input was accepted");
    }
    for (const auto& value : {json("bad"), json("12oops"), json("NaN"), json("Infinity"),
                              json("0x1p2"), json("1e1000"), json(1e39), json(true), json::array({1})}) {
        bool rejected = false;
        try { get_float_value(json{{"x", value}}, "x"); }
        catch (const std::invalid_argument&) { rejected = true; }
        require(rejected, "Invalid numeric input was accepted");
    }

    httplib::Server server;
    register_predict_endpoint(server, [](const json& rows) {
        if (rows.empty()) return json{{"error", "Input data must be non-empty"}};
        if (rows.front().contains("simulate_failure")) throw std::runtime_error("Simulated model failure");
        json missing = json::array();
        for (const auto& row : rows) missing.push_back(std::isnan(get_float_value(row, "x")));
        return json{{"missing", missing}};
    });
    int port = server.bind_to_any_port("127.0.0.1");
    require(port > 0, "Could not bind test server");
    std::thread worker([&] { server.listen_after_bind(); });
    try {
        for (int i = 0; i < 1000 && !server.is_running(); ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        require(server.is_running(), "Test server did not start");
        httplib::Client client("127.0.0.1", port);
        client.set_read_timeout(5, 0);
        auto response = client.Post("/predict", R"([{}, {"x":null}, {"x":0}])", "application/json");
        require(response && response->status == 200, "Valid request failed");
        require(json::parse(response->body)["missing"] == json::array({true, true, false}),
                "HTTP numeric input lost missing-value semantics");
        response = client.Post("/predict", R"({"x":[null, 0]})", "application/json");
        require(response && response->status == 200, "Columnar request failed");
        require(json::parse(response->body)["missing"] == json::array({true, false}), "Columnar null changed");
        response = client.Post("/predict", R"({"x":[0],"unused":[1,2]})", "application/json");
        require(response && response->status == 200, "Length-one column broadcasting failed");
        require(json::parse(response->body)["missing"] == json::array({false, false}), "Broadcast values changed");
        response = client.Post("/predict", R"([{"x":"12oops"}])", "application/json");
        require(response && response->status == 400, "Invalid numeric request must return HTTP 400");
        for (const auto* body : {"null", "true", "1", R"("invalid")", "{}", "[]", "[1]",
                                 R"([{},null])", R"({"x":1})", R"({"x":null})",
                                 R"({"x":[1,2],"y":[3,4,5]})",
                                 R"({"x":[1],"y":[2,3],"z":[4,5,6]})",
                                 R"({"x":[],"y":[1]})", "[", R"([{"x":1e1000}])"}) {
            response = client.Post("/predict", body, "application/json");
            require(response && response->status == 400, "Malformed request must return HTTP 400");
            require(response->get_header_value("Content-Type").find("application/json") == 0,
                    "Error response must be JSON");
            require(json::parse(response->body).contains("error"), "Error response must contain an error");
        }
        response = client.Post("/predict", R"([{"simulate_failure":true}])", "application/json");
        require(response && response->status == 500, "Model runtime failure must return HTTP 500");
    } catch (...) {
        server.stop();
        worker.join();
        throw;
    }
    server.stop();
    worker.join();
    std::cout << "GBDT request tests passed\n";
}
