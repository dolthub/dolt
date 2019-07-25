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
                        PATH = "${pwd()}/.ci_bin:${env.HOME}/go/bin:${env.PATH}"
                    }
                    steps {
                        dir (".ci_bin") {
                            sh "go get golang.org/x/tools/cmd/goimports"
                        }
                        dir ("go") {
                            sh "go get -mod=readonly ./..."
                            sh "./utils/repofmt/check_fmt.sh"
                            sh "./Godeps/verify.sh"
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
                        PATH = "${pwd()}/.ci_bin:${pwd()}/.ci_bin/node_modules/.bin:${env.PATH}"
                    }
                    steps {
                        dir (".ci_bin") {
                            sh "npm i bats"
                        }
                        dir ("go") {
                            sh "go get -mod=readonly ./..."
                            sh "go build -mod=readonly -o ../.ci_bin/dolt ./cmd/dolt/."
                            sh "go build -mod=readonly -o ../.ci_bin/git-dolt ./cmd/git-dolt/."
                            sh "go build -mod=readonly -o ../.ci_bin/git-dolt-smudge ./cmd/git-dolt-smudge/."
                            sh "go build -mod=readonly -o ../.ci_bin/remotesrv ./utils/remotesrv/."
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
