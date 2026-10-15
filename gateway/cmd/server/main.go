package main

import (
	"fmt"
	"net/http"
)

func hello(w http.ResponseWriter, req *http.Request) {
	
	fmt.Fprintf(w, "Hello\n")
}

func header(w http.ResponseWriter, req *http.Request) {
	
	fmt.Fprintf(w, "Header\n")
	
}



func main() {

	http.HandleFunc("/hello", hello)
	http.HandleFunc("/headers", header)

	http.ListenAndServe(":8090", nil)
}