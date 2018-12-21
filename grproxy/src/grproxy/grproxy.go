package main

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	//        "net/url"
	"log"
	//        "math/rand"
//	"strings"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)
var checkServers []string
var servers = []string{}
var server string = ""
var i int = 0

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func connect() *zk.Conn {
    fmt.Printf("Creating zookeeper connection, inside connect function")
	conn, _, err := zk.Connect([]string{"zookeeper"}, time.Second)
//	must(err)
	if err != nil {
	fmt.Printf("Error while connction, retrying")
	time.Sleep(2 * time.Second)
	connect()
	}
	fmt.Printf("Created zookeeper connection, inside connect function")
	return conn
}

func roundrobin() int {
	if i >= (len(servers)) {
		i = 0
	}
	return i
}

/*func Balance() string {
	if len(urls) <= 0 {
	return "error"
	}
	if len(urls) > 0 {
	server = servers[i]
	i++
	}
	if i >= len(urls) {
	i = 0
	}
	return "From BalanceRR" + server
}*/
func checkChildren() {


    fmt.Printf("checking for children after zookeeper connection")
	conn := connect()
	defer conn.Close()

	children, _, err := conn.Children("/grproxy")
	must(err)
	for _, name := range children {
		data, _, err := conn.Get("/grproxy/" + name)
		must(err)
		fmt.Printf("/grproxy/%s: %s\n", name, string(data))
	}

}

func mirror(conn *zk.Conn, path string) (chan []string, chan error) {
	snapshots := make(chan []string)
	errors := make(chan error)
	go func() {
		for {
			children, _, events, err := conn.ChildrenW(path)
			if err != nil {
				errors <- err
				return
			}
			checkServers = []string{}
			for _, name := range children {
				data, _, err := conn.Get("/grproxy/" + name)
				must(err)

				checkServers = append(checkServers, string(data))
				fmt.Printf("childurl: %s\n", data)

			}
			servers = checkServers
			for j, namecheck := range servers {
				fmt.Printf("index", j)
				fmt.Printf(namecheck)
			}
			fmt.Printf("total: %s\n", servers)
			snapshots <- children
			evt := <-events
			if evt.Err != nil {
				errors <- evt.Err
				return
			}
		}
	}()
	return snapshots, errors
}

func reverseProxyRedirect() *httputil.ReverseProxy {
	//handle requests with or without /library path separately
	director := func(r *http.Request) {
		if r.URL.Path == "/library" {
			fmt.Println("gserver request")
			fmt.Printf("calling roundrobin function for getting active gserve instances")
			targetUrl := servers[roundrobin()]
			fmt.Printf(servers[roundrobin()])
			i++
			fmt.Println("setting url scheme and host according to the obtained instance of gserve")
			r.URL.Scheme = "http"
			r.URL.Host = targetUrl
		} else {
			fmt.Println("nginx request")
			fmt.Println("setting url scheme and host according to the nginx")
			r.URL.Scheme = "http"
			r.URL.Host = "nginx"
		}
	}
	return &httputil.ReverseProxy{Director: director}
}

func main() {

    fmt.Println("initializing zookeeper connection in the main function")
	conn := connect()
	defer conn.Close()
	//add if condition
	flags := int32(0)
	acl := zk.WorldACL(zk.PermAll)
	for conn.State() != zk.StateHasSession {
		fmt.Printf("waiting to establish connection with zookeeper in the main function")
		time.Sleep(8 * time.Second)
	}
	checkifexists, st, errrrrr := conn.Exists("/grproxy")
	must(errrrrr)
	fmt.Printf("exists: %+v %+v\n", checkifexists, st)

	if !checkifexists {
		grproxy, err := conn.Create("/grproxy", []byte("grproxy:80"), flags, acl)
		must(err)
		fmt.Printf("created: %+v\n", grproxy)
	}

	snapshots, errors := mirror(conn, "/grproxy")
	go func() {
		for {
			select {
			case snapshot := <-snapshots:
				fmt.Printf("%+v\n", snapshot)
			case err := <-errors:
				panic(err)
			}
		}
	}()

    fmt.Println("calling the reverse proxy to allot the active gserve instance obtained or nginx to the accessed port")
	//call the reverseProxyRedirect function. Pass urls of active gserve instances (for now)
	proxies := reverseProxyRedirect()
	log.Fatal(http.ListenAndServe(":9090", proxies))
}
