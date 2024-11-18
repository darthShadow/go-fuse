package fuse

func (ms *Server) setSplice() {
	ms.canSplice = false
}

func (ms *Server) trySplice(header []byte, req *request, fdData *readResultFd) error {
	return errRecoverSplice
}
