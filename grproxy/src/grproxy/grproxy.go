package main

import (
        "net/http"
        "net/http/httputil"
        "fmt"
        "net/url"
        "log"
        "math/rand"
)

func reverseProxyRedirect(urls []*url.URL) *httputil.ReverseProxy {
//handle requests with or without /library path separately
	director := func(r *http.Request) {
	if r.URL.Path == "/library" {
		fmt.Println("gserver request")
		//handle gserve instances on a random basis
		targetUrl := urls[rand.Int()%len(urls)]
		r.URL.Scheme = targetUrl.Scheme
		r.URL.Host = targetUrl.Host
	} else {
		fmt.Println("nginx request")
		r.URL.Scheme = "http"
		r.URL.Host = "nginx"
	}
	}
	return &httputil.ReverseProxy{Director: director}
}

func main() {
//call the reverseProxyRedirect function. Pass urls of active gserve instances (for now)
        proxies := reverseProxyRedirect([]*url.URL{
                {
                        Scheme: "http",
                        Host:   "localhost:9094",
                },
                {
                        Scheme: "http",
                        Host:   "localhost:9092",
                },
        })
        log.Fatal(http.ListenAndServe(":9090", proxies))
}
