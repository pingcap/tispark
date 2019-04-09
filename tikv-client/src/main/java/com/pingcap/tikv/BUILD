package(default_visibility = ["//visibility:public"])

java_library(
    name = "tikv_java_client_lib",
    srcs = glob(
        ["**/*.java"],
    ),
    deps = [
        "//tikv-client:tikv_proto_java_lib",
        ":tikv_parser_lib",
        "@antlr4_runtime//jar",
        "@com_sangupta_murmur//jar",
        "@com_fasterxml_jackson_core_jackson_annotations//jar",
        "@com_fasterxml_jackson_core_jackson_core//jar",
        "@com_fasterxml_jackson_core_jackson_databind//jar",
        "@com_google_code_findbugs_jsr305//jar",
        "@com_google_code_gson_gson//jar",
        "@com_google_errorprone_error_prone_annotations//jar",
        "@com_google_guava_guava//jar",
        "@com_google_protobuf_protobuf_java//jar",
        "@joda_time//jar",
        # the following are defined in rules_protobuf
        "@org_pubref_rules_protobuf//java:grpc_compiletime_deps",
        "@org_pubref_rules_protobuf//java:netty_runtime_deps",
        "@org_slf4j_slf4j_api//jar",
        "@org_slf4j_jcl_over_slf4j//jar",
        "@org_slf4j_jul_to_slf4j//jar",
        "@log4j_log4j//jar",
        "@net_sf_trove4j_trove4j//jar",
    ],
)


load("@rules_antlr//antlr:antlr4.bzl", "antlr4")
antlr4(
    name = "tikv_parser",
    srcs = [
        "parser/MySqlLexer.g4",
        "parser/MySqlParser.g4",
        ],
    package = "com.pingcap.tikv.parser",
    visitor = True,
)

java_library(
    name = "tikv_parser_lib",
    srcs = [":tikv_parser"],
    deps = [
        "@antlr4_tool//jar",
        "@antlr4_runtime//jar"
    ]
)