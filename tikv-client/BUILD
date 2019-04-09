load(
    "@com_github_zhexuany_bazel_shade//:java_shade.bzl",
    "java_shade"
)
package(default_visibility = ["//visibility:public"])

java_binary(
    name = "tikv_java_client",
    main_class = "com.pingcap.tikv.Main",
    runtime_deps = [
        "//tikv-client/src/main/java/com/pingcap/tikv:tikv_java_client_lib",
        ":shaded_scalding",
    ],
)

java_shade(
    name = "shaded_args",
    input_jar = "@io_netty_netty_codec_socks//jar",
    rules = "shading_rule"
)

java_import(
    name = "shaded_scalding",
    jars = ["shaded_args.jar"]
)

filegroup(
    name = "tikv_protos",
    srcs = glob([
         "kvproto/proto/*.proto",
         "kvproto/include/*.proto",
         "kvproto/include/gogoproto/*.proto",
         "tipb/proto/*.proto",
    ]),
)

load("@org_pubref_rules_protobuf//java:rules.bzl", "java_proto_library")

java_proto_library(
    name = "tikv_proto_java_lib",
    imports = [
            "external/com_google_protobuf/src/",
            "tikv-client/kvproto/proto",
            "tikv-client/kvproto/include",
            "tikv-client/tipb/proto",
    ],
    inputs = ["@com_google_protobuf//:well_known_protos"],
    protos = [":tikv_protos"],
    verbose = 0,  # 0=no output, 1=show protoc command, 2+ more...
    with_grpc = True,
)

