pipeline {
    agent none
    stages {
        stage('Test') {
            parallel {
                stage ("Windows") {
                    agent {
                        label "windows"
                    }
                    environment {
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
            }
        }
    }
}
