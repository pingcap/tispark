def call(ghprbActualCommit, ghprbCommentBody, ghprbPullId, ghprbPullTitle, ghprbPullLink, ghprbPullDescription, credentialsId, channel, teamDomain, tokenCredentialId) {
    env.GOROOT = "/usr/local/go"
    env.GOPATH = "/go"
    env.PATH = "/bin:/sbin:/usr/bin:/usr/sbin:/usr/local/bin:/usr/local/sbin"
    env.PATH = "${env.GOROOT}/bin:/home/jenkins/bin:/bin:${env.PATH}"
    def TIDB_BRANCH = "master"
    def TIKV_BRANCH = "master"
    def PD_BRANCH = "master"
    def TIFLASH_BRANCH = "master"
    def TEST_MODE = "full"
    def PARALLEL_NUMBER = 18
    def TEST_REGION_SIZE = "normal"
    def TEST_TIFLASH = "false"
    def MVN_PROFILE = ""
    def MVN_PROFILE_SCALA_2_12 = "-Pspark-2.4-scala-2.12"
    def MVN_PROFILE_SCALA_2_12_TEST = ["-Pjenkins-test-spark-3.0", "-Pjenkins-test-spark-2.4"]
    def MVN_PROFILE_SCALA_2_11 = "-Pspark-2.3-scala-2.11"
    def MVN_PROFILE_SCALA_2_11_TEST = ["-Pjenkins-test-spark-2.4", "-Pjenkins-test-spark-2.3"]


    // parse tidb branch
    def m1 = ghprbCommentBody =~ /tidb\s*=\s*([^\s\\]+)(\s|\\|$)/
    if (m1) {
        TIDB_BRANCH = "${m1[0][1]}"
    }
    println "TIDB_BRANCH=${TIDB_BRANCH}"

    // parse pd branch
    def m2 = ghprbCommentBody =~ /pd\s*=\s*([^\s\\]+)(\s|\\|$)/
    if (m2) {
        PD_BRANCH = "${m2[0][1]}"
    }
    println "PD_BRANCH=${PD_BRANCH}"

    // parse tikv branch
    def m3 = ghprbCommentBody =~ /tikv\s*=\s*([^\s\\]+)(\s|\\|$)/
    if (m3) {
        TIKV_BRANCH = "${m3[0][1]}"
    }
    println "TIKV_BRANCH=${TIKV_BRANCH}"

    // parse tiflash branch
    def m4 = ghprbCommentBody =~ /tiflash\s*=\s*([^\s\\]+)(\s|\\|$)/
    if (m4) {
        TIFLASH_BRANCH = "${m4[0][1]}"
    }
    println "TIFLASH_BRANCH=${TIFLASH_BRANCH}"

    // parse mvn profile
    def m5 = ghprbCommentBody =~ /profile\s*=\s*([^\s\\]+)(\s|\\|$)/
    if (m5) {
        MVN_PROFILE = " -P${m5[0][1]}"
    }

    // parse test mode
    def m6 = ghprbCommentBody =~ /mode\s*=\s*([^\s\\]+)(\s|\\|$)/
    if (m6) {
        TEST_MODE = "${m6[0][1]}"
    }

    // parse test region size
    def m7 = ghprbCommentBody =~ /region\s*=\s*([^\s\\]+)(\s|\\|$)/
    if (m7) {
        TEST_REGION_SIZE = "${m7[0][1]}"
    }

    // parse test mode
    def m8 = ghprbCommentBody =~ /test-flash\s*=\s*([^\s\\]+)(\s|\\|$)/
    if (m8) {
        TEST_TIFLASH = "${m8[0][1]}"
    }

    groovy.lang.Closure readfile = { filename ->
        def file = readFile filename
        return file.split("\n") as List
    }

    groovy.lang.Closure get_mvn_str = { total_chunks ->
        def mvnStr = " -DwildcardSuites="
        for (int i = 0 ; i < total_chunks.size() - 1; i++) {
            // print total_chunks
            def trimStr = total_chunks[i]
            mvnStr = mvnStr + "${trimStr},"
        }
        def trimStr = total_chunks[total_chunks.size() - 1]
        mvnStr = mvnStr + "${trimStr}"
        mvnStr = mvnStr + " -DfailIfNoTests=false"
        mvnStr = mvnStr + " -DskipAfterFailureCount=1"
        return mvnStr
    }

    def label = "regression-test-tispark"

    podTemplate(name: label, label: label, instanceCap: 10, idleMinutes: 720, containers: [
            containerTemplate(name: 'golang', image: 'hub.pingcap.net/jenkins/centos7_golang-1.12:cached',
                    envVars: [
                            envVar(key: 'DOCKER_HOST', value: 'tcp://localhost:2375'),
                    ], alwaysPullImage: true, ttyEnabled: true, command: 'cat'),
            containerTemplate(name: 'java', image: 'hub.pingcap.net/jenkins/centos7_golang-1.13_java:cached',
                    resourceRequestCpu: '8000m',
                    resourceRequestMemory: '16Gi',
                    envVars: [
                            envVar(key: 'DOCKER_HOST', value: 'tcp://localhost:2375'),
                    ], alwaysPullImage: true, ttyEnabled: true, command: 'cat'),
    ]) {

        catchError {
            stage('Prepare') {
                node (label) {
                    println "${NODE_NAME}"
                    container("golang") {
                        deleteDir()

                        // tidb
                        def tidb_sha1 = sh(returnStdout: true, script: "curl ${FILE_SERVER_URL}/download/refs/pingcap/tidb/${TIDB_BRANCH}/sha1").trim()
                        sh "curl ${FILE_SERVER_URL}/download/builds/pingcap/tidb/${tidb_sha1}/centos7/tidb-server.tar.gz | tar xz"
                        // tikv
                        def tikv_sha1 = sh(returnStdout: true, script: "curl ${FILE_SERVER_URL}/download/refs/pingcap/tikv/${TIKV_BRANCH}/sha1").trim()
                        sh "curl ${FILE_SERVER_URL}/download/builds/pingcap/tikv/${tikv_sha1}/centos7/tikv-server.tar.gz | tar xz"
                        // pd
                        def pd_sha1 = sh(returnStdout: true, script: "curl ${FILE_SERVER_URL}/download/refs/pingcap/pd/${PD_BRANCH}/sha1").trim()
                        sh "curl ${FILE_SERVER_URL}/download/builds/pingcap/pd/${pd_sha1}/centos7/pd-server.tar.gz | tar xz"
                        // tiflash
                        if (TEST_TIFLASH != "false") {
                            def tiflash_sha1 = sh(returnStdout: true, script: "curl ${FILE_SERVER_URL}/download/refs/pingcap/tiflash/${TIFLASH_BRANCH}/sha1").trim()
                            sh "curl ${FILE_SERVER_URL}/download/builds/pingcap/tiflash/${TIFLASH_BRANCH}/${tiflash_sha1}/centos7/tiflash.tar.gz | tar xz"
                            sh """
                        cd tiflash
                        tar -zcvf flash_cluster_manager.tgz flash_cluster_manager/
                        rm ./flash_cluster_manager.tgz
                        cd ..
                        """
                            stash includes: "tiflash/**", name: "tiflash_binary"
                        }

                        stash includes: "bin/**", name: "binaries"

                        dir("/home/jenkins/agent/git/tispark") {
                            if (sh(returnStatus: true, script: '[ -d .git ] && [ -f Makefile ] && git rev-parse --git-dir > /dev/null 2>&1') != 0) {
                                deleteDir()
                            }
                            checkout changelog: false, poll: false, scm: [$class: 'GitSCM', branches: [[name: 'master']], doGenerateSubmoduleConfigurations: false, extensions: [[$class: 'PruneStaleBranch'], [$class: 'CleanBeforeCheckout']], submoduleCfg: [], userRemoteConfigs: [[credentialsId: credentialsId, refspec: '+refs/pull/*:refs/remotes/origin/pr/*', url: 'git@github.com:pingcap/tispark.git']]]
                        }

                        dir("go/src/github.com/pingcap/tispark") {
                            deleteDir()
                            sh """
                        pwd
                        cp -R /home/jenkins/agent/git/tispark/. ./
                        git checkout -f ${ghprbActualCommit}
                        find core/src -name '*Suite*' | grep -v 'MultiColumnPKDataTypeSuite' > test
                        shuf test -o  test2
                        mv test2 test
                        """

                            if (TEST_REGION_SIZE  != "normal") {
                                sh "sed -i 's/\\# region-max-size = \\\"2MB\\\"/region-max-size = \\\"2MB\\\"/' config/tikv.toml"
                                sh "sed -i 's/\\# region-split-size = \\\"1MB\\\"/region-split-size = \\\"1MB\\\"/' config/tikv.toml"
                                sh "cat config/tikv.toml"
                            }

                            if (TEST_MODE != "simple") {
                                sh """
                            find core/src -name '*MultiColumnPKDataTypeSuite*' >> test
                            """
                            }

                            if (TEST_TIFLASH != "false") {
                                sh "cp .ci/tidb_config-for-tiflash-test.properties core/src/test/resources/tidb_config.properties"
                            }

                            sh """
                        sed -i 's/core\\/src\\/test\\/scala\\///g' test
                        sed -i 's/\\//\\./g' test
                        sed -i 's/\\.scala//g' test
                        split test -n r/$PARALLEL_NUMBER test_unit_ -a 2 --numeric-suffixes=1
                        """

                            for (int i = 1; i <= PARALLEL_NUMBER; i++) {
                                if (i < 10) {
                                    sh """cat test_unit_0$i"""
                                } else {
                                    sh """cat test_unit_$i"""
                                }
                            }

                            sh """
                        cp .ci/log4j-ci.properties core/src/test/resources/log4j.properties
                        bash core/scripts/version.sh
                        bash core/scripts/fetch-test-data.sh
                        mv core/src/test core-test/src/
                        bash tikv-client/scripts/proto.sh
                        """
                        }

                        stash includes: "go/src/github.com/pingcap/tispark/**", name: "tispark", useDefaultExcludes: false
                    }
                }
            }

            stage('Integration Tests') {
                def tests = [:]

                groovy.lang.Closure run_tispark_test = { chunk_suffix ->
                    dir("go/src/github.com/pingcap/tispark") {
                        if (chunk_suffix < 10) {
                            run_chunks = readfile("test_unit_0${chunk_suffix}")
                        } else {
                            run_chunks = readfile("test_unit_${chunk_suffix}")
                        }

                        print run_chunks
                        def mvnStr = get_mvn_str(run_chunks)
                        sh """
                        rm -rf /maven/.m2/repository/*
                        rm -rf /maven/.m2/settings.xml
                        rm -rf ~/.m2/settings.xml
                        archive_url=http://fileserver.pingcap.net/download/builds/pingcap/tispark/cache/tispark-m2-cache-latest.tar.gz
                        if [ ! "\$(ls -A /maven/.m2/repository)" ]; then curl -sL \$archive_url | tar -zx -C /maven || true; fi
                    """

                        sh "./dev/change-scala-version.sh 2.12"
                        MVN_PROFILE_SCALA_2_12_TEST.each { MVN_TEST_PROFILE ->
                            sh """
                            export MAVEN_OPTS="-Xmx6G -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=51M"
                            mvn clean test ${MVN_PROFILE} ${MVN_PROFILE_SCALA_2_12} ${MVN_TEST_PROFILE} -Dtest=moo ${mvnStr}
                        """
                        }

                        sh "./dev/change-scala-version.sh 2.11"
                        MVN_PROFILE_SCALA_2_11_TEST.each { MVN_TEST_PROFILE ->
                            sh """
                            export MAVEN_OPTS="-Xmx6G -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=51M"
                            mvn clean test ${MVN_PROFILE} ${MVN_PROFILE_SCALA_2_11} ${
                                MVN_TEST_PROFILE
                            } -Dtest=moo ${mvnStr}
                        """
                        }
                    }
                }

                groovy.lang.Closure run_tikvclient_test = { chunk_suffix ->
                    dir("go/src/github.com/pingcap/tispark") {
                        sh """
                        rm -rf /maven/.m2/repository/*
                        rm -rf /maven/.m2/settings.xml
                        rm -rf ~/.m2/settings.xml
                        archive_url=http://fileserver.pingcap.net/download/builds/pingcap/tispark/cache/tispark-m2-cache-latest.tar.gz
                        if [ ! "\$(ls -A /maven/.m2/repository)" ]; then curl -sL \$archive_url | tar -zx -C /maven || true; fi
                    """
                        sh """
                        export MAVEN_OPTS="-Xmx6G -XX:MaxPermSize=512M"
                        mvn test ${MVN_PROFILE} -am -pl tikv-client
                    """
                        unstash "CODECOV_TOKEN"
                        sh 'curl -s https://codecov.io/bash | bash -s - -t @CODECOV_TOKEN'
                    }
                }

                groovy.lang.Closure run_intergration_test = { chunk_suffix, run_test ->
                    node(label) {
                        println "${NODE_NAME}"
                        container("java") {
                            deleteDir()
                            unstash 'binaries'
                            unstash 'tispark'
                            if (TEST_TIFLASH != "false") {
                                unstash 'tiflash_binary'
                            }

                            try {

                                groovy.lang.Closure isv4 = { branch_name ->
                                    if (branch_name.startsWith("v4") || branch_name.startsWith("release-4") || branch_name == "master") {
                                        return true
                                    }
                                    return false
                                }

                                if (isv4(TIDB_BRANCH) || isv4(TIKV_BRANCH) || isv4(PD_BRANCH)) {
                                    sh """
                                rm go/src/github.com/pingcap/tispark/config/pd.toml
                                rm go/src/github.com/pingcap/tispark/config/tikv.toml
                                rm go/src/github.com/pingcap/tispark/config/tidb.toml

                                mv go/src/github.com/pingcap/tispark/config/pd-4.0.toml go/src/github.com/pingcap/tispark/config/pd.toml
                                mv go/src/github.com/pingcap/tispark/config/tikv-4.0.toml go/src/github.com/pingcap/tispark/config/tikv.toml
                                mv go/src/github.com/pingcap/tispark/config/tidb-4.0.toml go/src/github.com/pingcap/tispark/config/tidb.toml
                                """
                                }

                                sh """
                            #sudo sysctl -w net.ipv4.ip_local_port_range=\'1000 30000\'
                            killall -9 tidb-server || true
                            killall -9 tikv-server || true
                            killall -9 pd-server || true
                            killall -9 tiflash || true
                            killall -9 java || true
                            touch tiflash_cmd_line.log
                            sleep 10
                            bin/pd-server --name=pd --data-dir=pd --config=go/src/github.com/pingcap/tispark/config/pd.toml &>pd.log &
                            sleep 10
                            bin/tikv-server --pd=127.0.0.1:2379 -s tikv --addr=0.0.0.0:20160 --advertise-addr=127.0.0.1:20160 --config=go/src/github.com/pingcap/tispark/config/tikv.toml &>tikv.log &
                            sleep 10
                            ps aux | grep '-server' || true
                            curl -s 127.0.0.1:2379/pd/api/v1/status || true
                            bin/tidb-server --store=tikv --path="127.0.0.1:2379" --config=go/src/github.com/pingcap/tispark/config/tidb.toml &>tidb.log &
                            sleep 60
                            """

                                if (TEST_TIFLASH != "false") {
                                    sh """
                                export LD_LIBRARY_PATH=/home/jenkins/agent/workspace/tispark_ghpr_integration_test/tiflash
                                ls -l \$LD_LIBRARY_PATH
                                tiflash/tiflash server config --config-file go/src/github.com/pingcap/tispark/config/tiflash.toml &>tiflash_cmd_line.log &
                                ps aux | grep 'tiflash'
                                sleep 60
                                """
                                    sh "ps aux | grep 'tiflash'"
                                }

                                timeout(360) {
                                    run_test(chunk_suffix)
                                }
                            } catch (err) {
                                sh """
                            ps aux | grep '-server' || true
                            curl -s 127.0.0.1:2379/pd/api/v1/status || true
                            """
                                sh "cat pd.log"
                                sh "cat tikv.log"
                                sh "cat tidb.log"
                                if (TEST_TIFLASH != "false") {
                                    sh """
                                touch tiflash_cmd_line.log
                                touch tiflash/tiflash_error.log
                                touch tiflash/tiflash.log
                                touch tiflash/tiflash_cluster_manager.log
                                touch tiflash/tiflash_tikv.log
                                """
                                    sh "cat tiflash_cmd_line.log"
                                    sh "cat tiflash/tiflash_error.log"
                                    sh "cat tiflash/tiflash.log"
                                    sh "cat tiflash/tiflash_cluster_manager.log"
                                    sh "cat tiflash/tiflash_tikv.log"
                                }
                                throw err
                            }
                        }
                    }
                }

                for (int i = 1; i <= PARALLEL_NUMBER; i++) {
                    int x = i
                    tests["Integration test = $i"] = {run_intergration_test(x, run_tispark_test)}
                }
                tests["Integration tikv-client test"] = {run_intergration_test(0, run_tikvclient_test)}

                parallel tests
            }

            currentBuild.result = "SUCCESS"
        }

    }
    stage('Summary') {
        def duration = ((System.currentTimeMillis() - currentBuild.startTimeInMillis) / 1000 / 60).setScale(2, BigDecimal.ROUND_HALF_UP)
        def slackmsg = "[#${ghprbPullId}: ${ghprbPullTitle}]" + "\n" +
                "${ghprbPullLink}" + "\n" +
                "${ghprbPullDescription}" + "\n" +
                "Integration Common Test Result: `${currentBuild.result}`" + "\n" +
                "Elapsed Time: `${duration} mins` " + "\n" +
                "${env.RUN_DISPLAY_URL}"

        if (currentBuild.result != "SUCCESS") {
            slackSend channel: channel, color: 'danger', teamDomain: teamDomain, tokenCredentialId: tokenCredentialId, message: "${slackmsg}"
        }
    }
}

return this