pipeline {
    agent none
    stages {
        stage('Test') {
            parallel {
                stage ("go/") {
                    agent {
                        kubernetes {
                            label "liquidata-inc-ld-build"
                        }
                    }
                    environment {
                        GIT_SSH_KEYFILE = credentials("liquidata-inc-ssh")
                        PATH = "${pwd()}/.ci_bin:${env.HOME}/go/bin:${env.PATH}"
                        GIT_SSH = "${pwd()}/.ci_bin/cred_ssh"
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

                            sh "go get golang.org/x/tools/cmd/goimports"
                        }
                        dir ("go") {
                            sh "go get ./..."
                            sh "./utils/repofmt/check_fmt.sh"
                            sh "go vet -mod=readonly ./..."
                            sh "go run -mod=readonly ./utils/copyrightshdrs/"
                            sh "go test -mod=readonly -test.v ./..."
                        }
                    }
                }
                stage ("bats/") {
                    agent {
                        kubernetes {
                            label "liquidata-inc-ld-build"
                        }
                    }
                    environment {
                        GIT_SSH_KEYFILE = credentials("liquidata-inc-ssh")
                        PATH = "${pwd()}/.ci_bin:${pwd()}/.ci_bin/node_modules/.bin:${env.PATH}"
                        GIT_SSH = "${pwd()}/.ci_bin/cred_ssh"
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
                        dir ("go") {
                            sh "go get ./..."
                            sh "go build -mod=readonly -o ../../.ci_bin/dolt ./cmd/dolt/."
                            sh "go build -mod=readonly -o ../../.ci_bin/git-dolt ./cmd/git-dolt/."
                            sh "go build -mod=readonly -o ../../.ci_bin/git-dolt-smudge ./cmd/git-dolt-smudge/."
                            sh "go build -mod=readonly -o ../../.ci_bin/remotesrv ./utils/remotesrv/."
                        }
                        dir (".ci_bin") {
                            sh "npm i bats"
                        }
                        sh "dolt config --global --add user.name 'Liquidata Jenkins'"
                        sh "dolt config --global --add user.email 'jenkins@liquidata.co'"
                        dir ("bats") {
                            sh "bats ."
                        }
                    }
                }
            }
        }
    }
}
