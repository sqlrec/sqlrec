cmake_minimum_required(VERSION 3.8)

project(inference C CXX)

include_directories("${CMAKE_CURRENT_BINARY_DIR}")

set(CMAKE_CXX_STANDARD 17)

find_package(Torch REQUIRED)
add_library( fbgemm SHARED IMPORTED GLOBAL )
set_target_properties(fbgemm PROPERTIES IMPORTED_LOCATION ${FBGEMM_LIB})

add_executable(server server.cpp)
target_link_libraries(server
  "${TORCH_LIBRARIES}"
  fbgemm
)
