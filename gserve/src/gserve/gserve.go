package main

import (
        "net/http"
	"fmt"
	"io/ioutil"
	"encoding/json"
	"log"
	"bytes"
//"os"
	"strings"
	"time"
	"github.com/samuel/go-zookeeper/zk"
)

var serverName string = "check"
//var zookeeper string = "zookeeper"
func main() {
//	serverName = os.Getenv("servername")
	serverName = "gserve1"
	fmt.Printf("Server Name: %+v\n", serverName)
	conn1 := connect()
	fmt.Printf("Created zookeeper connection")
	flags := int32(zk.FlagEphemeral)
	acl := zk.WorldACL(zk.PermAll)
	servers, err := conn1.Create("/grproxy/"+serverName, []byte("http://localhost:9092"), flags, acl)
	must(err)
	fmt.Printf("Created ephemeral node under grproxy: %+v\n", servers)
	//handle requests with "/library" path
	http.HandleFunc("/library", handlerForPath)
	//serverport for this instance
	log.Fatal(http.ListenAndServe(":9092", nil))
}

func connect() *zk.Conn {
        zksStr := "localhost:2181"
	zks := strings.Split(zksStr, ",")
	conn, _, err := zk.Connect(zks, time.Second)
	must(err)
	return conn
}

func must(err error) {
	if err != nil {
		//panic(err)
		fmt.Printf("%+v From must \n", err)
	}
}

func stringencoder(unencodedJSON []byte) string {
	// convert JSON to Go objects
	var unencodedRows RowsType
	json.Unmarshal(unencodedJSON, &unencodedRows)
	// encode fields in Go objects
	encodedRows := unencodedRows.encode()
	// convert encoded Go objects to JSON
	encodedJSON, _ := json.Marshal(encodedRows)
	return string(encodedJSON)
}

func stringdecoder(encodedJSON []byte) string {
	// convert JSON to Go objects
	var encodedRows EncRowsType
	json.Unmarshal(encodedJSON, &encodedRows)
	// encode fields in Go objects
	decodedRows, error1 := encodedRows.decode()
	if error1 != nil {
		fmt.Println("%+v", error1)
	}
	// convert encoded Go objects to JSON
	decodedJSON, _ := json.Marshal(decodedRows)
	return string(decodedJSON)
}

func handlerForPath(w http.ResponseWriter, r *http.Request) {
	//handle Get and Post requests
	if r.Method == "GET" {
	r.Header.Set("Accept", "application/json")
	//set url for interacting with rest api to read data
	urlrest1 := "http://" + "hbase" + ":8080/se2:library/*"
	readReq, _ := http.NewRequest("GET", urlrest1, nil)
	readReq.Header.Set("Accept", "application/json")
	client := &http.Client{}
	responseGet, err := client.Do(readReq)
	if err != nil {
		fmt.Printf("Error: %+v", err)
	}
	//read data from the body
	encodedJsonByte, errr := ioutil.ReadAll(responseGet.Body)
	if errr != nil {
		fmt.Printf("Error: %+v", errr)
	}
	//decode the data
	obtainedDecodedData := stringdecoder(encodedJsonByte)
	defer responseGet.Body.Close()
	//print data received from Hbase as it is (decoded) on the landing page
	fmt.Fprintf(w, "ReceivedData", string(obtainedDecodedData))		
	} else if r.Method == "POST" {
	//read data from body
	encodedData, errrr := ioutil.ReadAll(r.Body)
	if errrr != nil {
		fmt.Printf("Error: %+v", errrr)
	}
	//encode the received data
	encodedJsonData := stringencoder(encodedData)
	r.Header.Set("Content-type", "application/json")
	//set url for interacting with rest api to write data
	urlrest2 := "http://" + "hbase" + ":8080/se2:library/fakerow"
	responsePost, errrrr := http.Post(urlrest2, "application/json", bytes.NewBuffer([]byte(encodedJsonData)))
	if errrrr != nil {
		fmt.Println("Error: %+v", errrrr)
		return
	}
	defer responsePost.Body.Close()
	}
        fmt.Fprintf(w, "proudly served by %s", serverName)
}
