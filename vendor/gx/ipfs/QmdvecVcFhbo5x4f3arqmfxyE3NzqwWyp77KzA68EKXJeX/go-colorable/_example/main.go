package main

import (
	"github.com/Sirupsen/logrus"
	"gx/ipfs/QmdvecVcFhbo5x4f3arqmfxyE3NzqwWyp77KzA68EKXJeX/go-colorable"
)

func main() {
	logrus.SetFormatter(&logrus.TextFormatter{ForceColors: true})
	logrus.SetOutput(colorable.NewColorableStdout())

	logrus.Info("succeeded")
	logrus.Warn("not correct")
	logrus.Error("something error")
	logrus.Fatal("panic")
}
