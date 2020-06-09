def call(ghprbActualCommit, ghprbPullId, ghprbPullTitle, ghprbPullLink, ghprbPullDescription, credentialsId, tokenCredentialId, channel, teamDomain) {
    env.GOROOT = "/usr/local/go"
    env.GOPATH = "/go"
    env.PATH = "/bin:/sbin:/usr/bin:/usr/sbin:/usr/local/bin:/usr/local/sbin"
    env.PATH = "${env.GOROOT}/bin:/home/jenkins/bin:/bin:${env.PATH}"
    env.SPARK_HOME = "/usr/local/spark-2.1.1-bin-hadoop2.7"
    
    catchError {
        node ('build') {
            deleteDir()
            container("java") {
                stage('Checkout') {
                    dir("/home/jenkins/agent/git/tispark") {
                        sh """
                        archive_url=http://fileserver.pingcap.net/download/builds/pingcap/tispark/cache/tispark-m2-cache-latest.tar.gz
                        if [ ! "\$(ls -A /maven/.m2/repository)" ]; then curl -sL \$archive_url | tar -zx -C /maven || true; fi
                        """
                        if (sh(returnStatus: true, script: '[ -d .git ] && [ -f Makefile ] && git rev-parse --git-dir > /dev/null 2>&1') != 0) {
                            deleteDir()
                        }
                        checkout changelog: false, poll: false, scm: [$class: 'GitSCM', branches: [[name: 'master']], doGenerateSubmoduleConfigurations: false, extensions: [[$class: 'PruneStaleBranch'], [$class: 'CleanBeforeCheckout']], submoduleCfg: [], userRemoteConfigs: [[credentialsId: credentialsId, refspec: '+refs/pull/*:refs/remotes/origin/pr/*', url: 'git@github.com:pingcap/tispark.git']]]
                    }
                }
    
                stage('Format') {
                    dir("go/src/github.com/pingcap/tispark") {
                        sh """
                        cp -R /home/jenkins/agent/git/tispark/. ./
                        git checkout -f ${ghprbActualCommit}
                        mvn mvn-scalafmt_2.11:format -Dscalafmt.skip=false -Dfile.encoding=UTF-8
                        mvn com.coveo:fmt-maven-plugin:format
                        git diff --quiet
                        formatted="\$?"
                        if [[ "\${formatted}" -eq 1 ]]
                        then
                           echo "code format error, please run the following commands:"
                           echo "   mvn mvn-scalafmt_2.11:format -Dscalafmt.skip=false"
                           echo "   mvn com.coveo:fmt-maven-plugin:format"
                           exit 1
                        fi
                        """
                    }
                }

                stage('Build') {
                    dir("go/src/github.com/pingcap/tispark") {
                        sh """
                        git checkout -f ${ghprbActualCommit}
                        mvn clean package -Dmaven.test.skip=true
                        """
                    }
                }
            }
        }
    
        currentBuild.result = "SUCCESS"
    }
    
    stage('Summary') {
        def duration = ((System.currentTimeMillis() - currentBuild.startTimeInMillis) / 1000 / 60).setScale(2, BigDecimal.ROUND_HALF_UP)
        def slackmsg = "[#${ghprbPullId}: ${ghprbPullTitle}]" + "\n" +
        "${ghprbPullLink}" + "\n" +
        "${ghprbPullDescription}" + "\n" +
        "Build Result: `${currentBuild.result}`" + "\n" +
        "Elapsed Time: `${duration} mins` " + "\n" +
        "${env.RUN_DISPLAY_URL}"
    
        if (currentBuild.result != "SUCCESS") {
            slackSend channel: channel, color: 'danger', teamDomain: teamDomain, tokenCredentialId: tokenCredentialId, message: "${slackmsg}"
        }
    }
}

return this