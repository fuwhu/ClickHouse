PROTOBUF_GENERATE_GRPC_CPP(dictionary_api_proto_sources dictionary_api_proto_headers dictionary_api.proto)

# Ignore warnings while compiling protobuf-generated *.pb.h and *.pb.cpp files.
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")

# Disable clang-tidy for protobuf-generated *.pb.h and *.pb.cpp files.
set (CMAKE_CXX_CLANG_TIDY "")

add_library(dictionary_api_protos ${dictionary_api_proto_sources} ${dictionary_api_proto_headers})
target_include_directories(dictionary_api_protos SYSTEM PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries (dictionary_api_protos PUBLIC ch_contrib::grpc)
