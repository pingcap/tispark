package(default_visibility = ["//visibility:public"])

load(":rule.bzl", "junit_suite_test")

junit_suite_test(
    name = "tikv-client-java-test",
    srcs = glob(
        ["**/*.java"],
    ),
    deps = [
        "//src/main/java/com/pingcap/tikv:tikv-java-client-lib",
        "//:java",
        "//:java_compile_imports",
        "@com_fasterxml_jackson_core_jackson_annotations//jar",
        "@com_fasterxml_jackson_core_jackson_core//jar",
        "@com_fasterxml_jackson_core_jackson_databind//jar",

        "@org_pubref_rules_protobuf//java:grpc_compiletime_deps",
        "@org_pubref_rules_protobuf//java:netty_runtime_deps",
        "@junit_junit//jar",
        "@joda_time//jar",
    ],
)
