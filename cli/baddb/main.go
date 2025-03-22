package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/ocowchun/baddb/server"
	"log"
	"net/http"
)

func main() {
	var port = flag.Int("port", 9527, "ddb server port")

	flag.Parse()

	svr := server.NewDdbServer()
	mux := http.NewServeMux()
	mux.HandleFunc("/", svr.Handler)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", *port),
		Handler: mux,
	}

	log.Printf("baddb server is running on port %d...", *port)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("Server error: %v", err)
	}
}
