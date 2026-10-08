cmake_minimum_required(VERSION 3.16)
project(orderer-cpp CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# ORDERER_SANITIZE=address|thread: instrumented builds (CHECKED harness, CI).
set(ORDERER_SANITIZE "" CACHE STRING "Sanitizer for every target: address (+undefined) or thread")

find_package(Threads REQUIRED)
include(GNUInstallDirs)

# Header-only library: include/orderer (+ vendored matcher-cpp in include/matcher).
add_library(orderer INTERFACE)
target_include_directories(orderer INTERFACE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_compile_features(orderer INTERFACE cxx_std_20)
target_link_libraries(orderer INTERFACE Threads::Threads)
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  target_compile_options(orderer INTERFACE -Wall -Wextra)
  if(ORDERER_SANITIZE STREQUAL "address")
    target_compile_options(orderer INTERFACE -fsanitize=address,undefined -fno-omit-frame-pointer -fno-sanitize-recover=undefined)
    target_link_options(orderer INTERFACE -fsanitize=address,undefined)
  elseif(ORDERER_SANITIZE STREQUAL "thread")
    target_compile_options(orderer INTERFACE -fsanitize=thread -fno-omit-frame-pointer)
    target_link_options(orderer INTERFACE -fsanitize=thread)
  endif()
endif()

# ---- harness tools (spec/HARNESS.md) ----
foreach(tool orderrun ordererfuzz orderrecover ordersnap orderbench)
  add_executable(${tool} tools/${tool}.cpp)
  target_link_libraries(${tool} PRIVATE orderer)
endforeach()

add_executable(quickstart examples/quickstart.cpp)
target_link_libraries(quickstart PRIVATE orderer)

# ---- tests ----
enable_testing()
set(VEC ${CMAKE_CURRENT_SOURCE_DIR}/vectors)
foreach(t ring_test pipeline_test golden_test)
  add_executable(${t} tests/${t}.cpp)
  target_link_libraries(${t} PRIVATE orderer)
endforeach()
add_test(NAME ring COMMAND ring_test)
add_test(NAME golden COMMAND golden_test ${VEC})
add_test(NAME pipeline COMMAND pipeline_test ${VEC} ${CMAKE_CURRENT_BINARY_DIR}/scratch)
if(ORDERER_SANITIZE STREQUAL "")
  add_executable(no_alloc_test tests/no_alloc_test.cpp)
  target_link_libraries(no_alloc_test PRIVATE orderer)
  add_test(NAME no_alloc COMMAND no_alloc_test ${CMAKE_CURRENT_BINARY_DIR}/scratch)
endif()
add_test(NAME vectors COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/tests/vectors.sh $<TARGET_FILE_DIR:orderrun> ${VEC})

install(DIRECTORY include/orderer include/matcher DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
