package task

import "net/http"

func ready(w http.ResponseWriter, _ *http.Request) {
	_, _ = w.Write([]byte("OK"))
}
