package main

import (
        "net/http"
        "net/http/httputil"
        "fmt"
        "net/url"
        "log"
        "math/rand"
	"time"
	"strings"
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
        zksStr := "localhost:2181"
	zks := strings.Split(zksStr, ",")
	conn, _, err := zk.Connect(zks, time.Second)
	must(err)
	return conn
}

/*func Balance() string {
	server := urls[i]
	i++

	// it means that we reached the end of servers
	// and we need to reset the counter and start
	// from the beginning
	if i >= len(urls) {
		i = 0
	}
	return "hihello"+server
}*/


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

	conn := connect()
	defer conn.Close()

	children, _, err := conn.Children("/grproxy")
	must(err)
	for _, name := range children {
		data, _, err := conn.Get("/grproxy/"+name)
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
//			snapshots <- snapshot
//			var s []string
			checkServers = []string{}
		for _, name := range children {
// append works on nil slices.
			data, _, err := conn.Get("/grproxy/"+name)
			must(err)
			
//			fmt.Printf(string(data))
			checkServers = append(checkServers, string(data))
			fmt.Printf("childurl: %s\n", data)
			}
			servers = checkServers
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

	conn := connect()
	defer conn.Close()
//add if condition
//	flags := int32(0)
//	acl := zk.WorldACL(zk.PermAll)

//	_, err := conn.Create("/grproxy", []byte("http://localhost:9090"), flags, acl)
//	must(err)
//	time.Sleep(5 * time.Second)
//	checkChildren()	

	snapshots, errors := mirror(conn, "/grproxy")
/*	childchn, errors := mirror(conn, "/grproxy")
	go func() {
		for {
			select {

			case children := <-childchn:
				fmt.Printf("%+v .....\n", children)
				var temp []string
				for _, child := range children {
					gserve_urls, _, err := conn.Get("/grproxy/" + child)
					fmt.Printf("childurl: %s\n", gserve_urls)
					temp = append(temp, string(gserve_urls))
					if err != nil {
						fmt.Printf("from child: %+v\n", err)
					}
				}
				urls = temp
				fmt.Printf("total: %s\n", urls)
				time.Sleep(5 * time.Second)
				//fmt.Println(Balance())
				fmt.Printf("%+v \n", urls)
			case err := <-errors:
				fmt.Printf("%+v routine error \n", err)
			}
		}
	}()*/

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


		
//call the reverseProxyRedirect function. Pass urls of active gserve instances (for now)
        proxies := reverseProxyRedirect([]*url.URL{
                {
                        Scheme: "http",
                        Host:   "localhost:9092",
                },
                {
                        Scheme: "http",
                        Host:   "localhost:9094",
                },
        })
        log.Fatal(http.ListenAndServe(":9090", proxies))
}
