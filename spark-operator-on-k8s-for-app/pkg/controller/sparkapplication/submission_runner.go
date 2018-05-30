package sparkapplication


type sparkSubmitRunner struct {
	workers int

	// 这个channel管道 读取操作都是可以的
	queue chan *submission
}

func newSparkSubmitRunner(workers int) *sparkSubmitRunner {

	return &sparkSubmitRunner{
		workers: workers,
		// 声明一个chan管道，
		// 阻塞式
		// 阻塞大小是workers
		queue : make(chan *submission, workers),
	}
}


