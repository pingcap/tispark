run:
	bazel run :tikv-java-client
uber_jar:
	bazel build :tikv-java-client_deploy.jar
test:
	bazel test //src/test/java/com/pingcap/tikv:tikv-client-java-test --test_output=errors  --test_timeout=3600
