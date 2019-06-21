pipeline {
    agent {
        kubernetes {
            label "liquidata-inc-ld-build"
        }
    }
    stages {
        stage('Test') {
            environment {
                GIT_SSH_KEYFILE = credentials("liquidata-inc-ssh")
                PATH = "${pwd()}/.ci_bin:${env.PATH}"
                GIT_SSH = "${pwd()}/.ci_bin/cred_ssh"
                LD_SKIP_POSTGRES = "y"
                NOMS_VERSION_NEXT = "1"
            }
            steps {
                dir (".ci_bin") {
                    writeFile file: "cred_ssh", text: '''\
                    #!/bin/sh
                    exec /usr/bin/ssh -i $GIT_SSH_KEYFILE -o StrictHostKeyChecking=no "$@"
                    '''.stripIndent()
                    sh "chmod +x cred_ssh"

                    writeFile file: "git", text: '''\
                    #!/bin/sh
                    exec /usr/bin/git -c url."ssh://git@github.com:".insteadOf=https://github.com "$@"
                    '''.stripIndent()
                    sh "chmod +x git"
                }
                dir (".") {
                    sh "go get ./cmd/... ./go/..."
                    sh "go test -mod=readonly -test.v ./cmd/... ./go/..."
                }
            }
        }
    }
}
