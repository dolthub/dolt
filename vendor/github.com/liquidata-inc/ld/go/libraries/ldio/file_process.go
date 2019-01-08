package ldio

type FileProcessor func(path string)
type FileMatches func(path string)

func FileParallelProcess(numThreads int, inDir string, recursive bool, procFunc FileProcessor, matchFunc func(string) bool) {
	fc := make(chan string)
	go func() {
		defer close(fc)
		FindFiles(inDir, recursive, matchFunc, fc)
	}()

	done := make(chan error)
	for i := 0; i < numThreads; i++ {
		go processThread(procFunc, fc, done)
	}

	for i := 0; i < numThreads; i++ {
		<-done
	}
}

func processThread(procFunc FileProcessor, fc chan string, done chan error) {
	for inFilePath := range fc {
		procFunc(inFilePath)
	}

	done <- nil
}
