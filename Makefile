run:
	bazelisk run //tikv-client:tikv_java_client
uber_jar:
	bazelisk build //tikv-client:tikv_java_client_deploy.jar
test:
	bazelisk test //tikv-client/src/test/java/com/pingcap/tikv:tikv_client_java_test --test_output=errors  --test_timeout=3600
