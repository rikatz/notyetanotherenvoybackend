package probe

import "net/http"

func StartProbeServer() {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	if err := http.ListenAndServe(":15021", handler); err != nil {
		panic(err)
	}
}
