package(default_visibility = ["//visibility:public"])

java_library(
    name = "tikv-java-client-lib",
    srcs = glob(
        ["**/*.java"],
    ),
    deps = [
        "//:java",
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

filegroup(
    name = "srcs",
    srcs = ["BUILD"] + glob(["**/*.java"]),
)
