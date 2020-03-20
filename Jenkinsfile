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
                        dir ("go") {
                            // Keep this in sync with //go/utils/prepr/prepr.sh.
                            sh "go get -mod=readonly ./..."
                            sh "./utils/repofmt/check_fmt.sh"
                            sh "./Godeps/verify.sh"
                            sh "./utils/checkcommitters/check_pr.sh"
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
                        PATH = "${pwd()}/.ci_bin/pyenv/bin:${pwd()}/.ci_bin:${pwd()}/.ci_bin/node_modules/.bin:${env.PATH}"
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
                        sh "python3 -m venv .ci_bin/pyenv"
                        sh "./.ci_bin/pyenv/bin/pip install doltpy"
                        sh "dolt config --global --add user.name 'Liquidata Jenkins'"
                        sh "dolt config --global --add user.email 'jenkins@liquidata.co'"
                        dir ("bats") {
                            sh "bats ."
                        }
                    }
                }
                stage ("Windows") {
                    agent {
                        label "windows"
                    }
                    environment {
                        GIT_SSH_KEYFILE = credentials("liquidata-inc-ssh")
                        PATH = "C:\\tools\\msys64\\mingw64\\bin;${pwd()}\\.ci_bin;${env.PATH}"
                    }
                    steps {
                        dir ("go/") {
                            bat "go test ./..."
                            bat "go build -mod=readonly -o ..\\.ci_bin\\dolt.exe ./cmd/dolt/."
                            bat "copy /Y ..\\.ci_bin\\dolt.exe ..\\.ci_bin\\dolt"
                            bat "go build -mod=readonly -o ..\\.ci_bin\\git-dolt.exe ./cmd/git-dolt/."
                            bat "copy /Y ..\\.ci_bin\\git-dolt.exe ..\\.ci_bin\\git-dolt"
                            bat "go build -mod=readonly -o ..\\.ci_bin\\git-dolt-smudge.exe ./cmd/git-dolt-smudge/."
                            bat "copy /Y ..\\.ci_bin\\git-dolt-smudge.exe ..\\.ci_bin\\git-dolt-smudge"
                            bat "go build -mod=readonly -o ..\\.ci_bin\\remotesrv.exe ./utils/remotesrv/."
                            bat "copy /Y ..\\.ci_bin\\remotesrv.exe ..\\.ci_bin\\remotesrv"
                        }
                        dir ("bats/") {
                            bat "dolt config --global --add user.name \"Liquidata Jenkins\""
                            bat "dolt config --global --add user.email \"jenkins@liquidata.co\""
                            bat "C:\\wsl.exe bats `pwd`"
                        }
                        dir ("./") {
                            bat(returnStatus: true, script: "setLocal EnableDelayedExpansion && pushd %LOCALAPPDATA%\\Temp && del /q/f/s .\\* >nul 2>&1 && rmdir /s/q . >nul 2>&1 && popd")
                            bat(returnStatus: true, script: "setLocal EnableDelayedExpansion && pushd C:\\batstmp && del /q/f/s .\\* >nul 2>&1 && rmdir /s/q . >nul 2>&1 && popd")
                        }
                    }
                }
                stage ("compatibility/") {
                    agent {
                        kubernetes {
                            label "liquidata-inc-ld-build"
                        }
                    }
                    environment {
                        PATH = "${pwd()}/.ci_bin/pyenv/bin:${pwd()}/.ci_bin:${pwd()}/.ci_bin/node_modules/.bin:${env.PATH}"
                    }
                    steps {
                        dir (".ci_bin") {
                            sh "npm i bats"
                            sh "export CI_BIN=`pwd`"
                        }
                        dir ("go") {
                            sh "go get -mod=readonly ./..."
                            sh "go build -mod=readonly -o $CI_BIN/dolt ./cmd/dolt/."
                        }
                        sh "dolt config --global --add user.name 'Liquidata Jenkins'"
                        sh "dolt config --global --add user.email 'jenkins@liquidata.co'"
                        sh "git checkout -b build$BUILD_ID"
                        dir ("bats/compatibility") {
                            sh "./runner.sh"
                        }
                    }
                }
            }
        }
    }
}
