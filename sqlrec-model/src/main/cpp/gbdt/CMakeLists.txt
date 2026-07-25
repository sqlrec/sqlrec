cmake_minimum_required(VERSION 3.12)

project(gbdt_inference C CXX)

set(CMAKE_CXX_STANDARD 17)

# httplib.h uses std::thread internally; pthread is required on Linux.
find_package(Threads REQUIRED)

# Shared third-party header-only libraries (httplib.h, json.hpp) live in the
# sibling common/ directory so they can be reused by other model servers.
set(COMMON_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../common")

# ---------------------------------------------------------------------------
# ONNX server — ONNX Runtime based (float features only)
# Used by both LightGBM and XGBoost (and any future ONNX-based GBDT backend).
# ---------------------------------------------------------------------------

# Read include/lib paths from environment variables (set by the Dockerfile).
# NOTE: In CMake's find_path/find_library PATHS option, ${VAR} references a
# CMake variable, NOT an environment variable. To read an environment variable,
# the ENV prefix must be used: PATHS ENV VAR_NAME.
find_path(ONNXRUNTIME_INCLUDE_DIR
    NAMES onnxruntime_cxx_api.h
    PATHS ENV ONNXRUNTIME_INCLUDE_DIR /usr/include/onnxruntime /usr/local/include/onnxruntime
    REQUIRED)
find_library(ONNXRUNTIME_LIB
    NAMES onnxruntime
    PATHS ENV ONNXRUNTIME_LIB_DIR /usr/lib /usr/local/lib
    REQUIRED)

add_executable(onnx_server onnx_server.cpp)
target_include_directories(onnx_server PRIVATE
        ${ONNXRUNTIME_INCLUDE_DIR}
        ${COMMON_INCLUDE_DIR})
target_link_libraries(onnx_server PRIVATE ${ONNXRUNTIME_LIB} Threads::Threads)

# ---------------------------------------------------------------------------
# CatBoost server — CatBoost C API based (supports categorical features)
# ---------------------------------------------------------------------------

# CatBoost model interface header and library are expected at standard paths
# or via CATBOOST_INCLUDE_DIR / CATBOOST_LIB_DIR environment variables.
find_path(CATBOOST_INCLUDE_DIR
    NAMES catboost/libs/model_interface/c_api.h
    PATHS ENV CATBOOST_INCLUDE_DIR /usr/include /usr/local/include
    REQUIRED)
find_library(CATBOOST_LIB
    NAMES catboostmodel
    PATHS ENV CATBOOST_LIB_DIR /usr/lib /usr/local/lib
    REQUIRED)

add_executable(catboost_server catboost_server.cpp)
target_include_directories(catboost_server PRIVATE
        ${CATBOOST_INCLUDE_DIR}
        ${COMMON_INCLUDE_DIR})
target_link_libraries(catboost_server PRIVATE ${CATBOOST_LIB} Threads::Threads)
