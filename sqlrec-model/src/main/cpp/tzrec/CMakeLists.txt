cmake_minimum_required(VERSION 3.19)
project(tzrec_serving LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Python3 REQUIRED COMPONENTS Interpreter)
execute_process(
  COMMAND "${Python3_EXECUTABLE}" -c
          "import importlib.util, pathlib, torch; print(torch.utils.cmake_prefix_path); print(pathlib.Path(importlib.util.find_spec('fbgemm_gpu').origin).parent)"
  OUTPUT_VARIABLE WHEEL_PATHS OUTPUT_STRIP_TRAILING_WHITESPACE
  COMMAND_ERROR_IS_FATAL ANY
)
string(REPLACE "\n" ";" WHEEL_PATHS "${WHEEL_PATHS}")
list(GET WHEEL_PATHS 0 TORCH_CMAKE_PATH)
list(GET WHEEL_PATHS 1 FBGEMM_LIBRARY_DIR)
list(PREPEND CMAKE_PREFIX_PATH "${TORCH_CMAKE_PATH}")
find_package(Torch REQUIRED)
find_package(Threads REQUIRED)
if(NOT EXISTS "${FBGEMM_LIBRARY_DIR}/fbgemm_gpu_py.so")
  message(FATAL_ERROR "FBGEMM native operator library missing: ${FBGEMM_LIBRARY_DIR}")
endif()

set(COMMON_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../common")
add_executable(tzrec_server server.cpp "${COMMON_INCLUDE_DIR}/farmhash.cc")
target_include_directories(tzrec_server PRIVATE "${COMMON_INCLUDE_DIR}")
target_compile_definitions(tzrec_server PRIVATE FBGEMM_LIBRARY_DIR="${FBGEMM_LIBRARY_DIR}")
separate_arguments(TORCH_COMPILE_FLAGS NATIVE_COMMAND "${TORCH_CXX_FLAGS}")
target_compile_options(tzrec_server PRIVATE ${TORCH_COMPILE_FLAGS})
target_link_libraries(tzrec_server PRIVATE "${TORCH_LIBRARIES}" Threads::Threads ${CMAKE_DL_LIBS})
set_target_properties(tzrec_server PROPERTIES BUILD_RPATH "${TORCH_INSTALL_PREFIX}/lib;${FBGEMM_LIBRARY_DIR}")

include(CTest)
if(BUILD_TESTING)
  add_executable(features_test "${CMAKE_CURRENT_SOURCE_DIR}/../../../test/cpp/tzrec/features_test.cpp"
                 "${COMMON_INCLUDE_DIR}/farmhash.cc")
  target_include_directories(features_test PRIVATE "${COMMON_INCLUDE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}")
  add_test(NAME features COMMAND features_test)
endif()
