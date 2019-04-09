package(default_visibility = ["//visibility:public"])

load(":rule.bzl", "junit_suite_test")

junit_suite_test(
    name = "tikv_client_java_test",
    srcs = glob(
        ["**/*.java"],
    ),
    deps = [
        "//tikv-client/src/main/java/com/pingcap/tikv:tikv_java_client_lib",
        "//tikv-client:tikv_proto_java_lib",
        "@com_fasterxml_jackson_core_jackson_annotations//jar",
        "@com_fasterxml_jackson_core_jackson_core//jar",
        "@com_fasterxml_jackson_core_jackson_databind//jar",

        "@org_pubref_rules_protobuf//java:grpc_compiletime_deps",
        "@org_pubref_rules_protobuf//java:netty_runtime_deps",
        "@net_sf_trove4j_trove4j//jar",
        "@junit_junit//jar",
        "@joda_time//jar",
        "@log4j_log4j//jar",
    ],
)

