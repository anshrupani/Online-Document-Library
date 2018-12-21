package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

var serverName string = "check"

func registerToZookeeper(conn1 *zk.Conn)  {
	//	serverName = "gserve1"
	fmt.Printf("Server Name: %+v\n", serverName)

	fmt.Printf("Created zookeeper connection")
	flags := int32(zk.FlagEphemeral)
	acl := zk.WorldACL(zk.PermAll)

	for conn1.State() != zk.StateHasSession {
		fmt.Printf("establishing connection with zookeeper\n")
		time.Sleep(2 * time.Second)
	}
	exists, _, _ := conn1.Exists("/grproxy")
	if !exists {
		time.Sleep(2*time.Second)
		registerToZookeeper(conn1)
	} else {
		fmt.Println("root node existsm already")

		servers, err := conn1.Create("/grproxy/"+serverName, []byte(serverName+":9094"), flags, acl)
		must(err)
		fmt.Printf("established an ephemeral child node under parent /grproxy: %+v\n", servers)
	}


}
//var zookeeper string = "zookeeper"
func main() {
	serverName = os.Getenv("servername")
	fmt.Printf("establishing connection with zookeeper in main method")
	conn1 := connect()
	registerToZookeeper(conn1)
	//handle requests with "/library" path
	fmt.Printf("handling path /library")
	http.HandleFunc("/library", handlerForPath)
	//serverport for this instance
	log.Fatal(http.ListenAndServe(":9094", nil))
}

func connect() *zk.Conn {
    fmt.Printf("establishing connection with zookeeper in connect method")
	conn, _, err := zk.Connect([]string{"zookeeper"}, time.Second)
//	must(err)
	if err != nil {
	fmt.Printf("Error while connction, retrying")
	time.Sleep(2 * time.Second)
	connect()
	}
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

func stringdecoder(encodedJSON []byte) []byte {
	// convert JSON to Go objects
	var encodedRows EncRowsType
	json.Unmarshal(encodedJSON, &encodedRows)
	// encode fields in Go objects
	decodedRows, error1 := encodedRows.decode()
	if error1 != nil {
		fmt.Printf("%+v \n", error1)
	}
	// convert encoded Go objects to JSON
	decodedJSON, _ := json.Marshal(decodedRows)
	return decodedJSON
}

func getScanner() string {
    fmt.Printf("implementation of the scanner as specified")
	payload1 := strings.NewReader("<Scanner batch=\"10\"/>")
	req1, _ := http.NewRequest(http.MethodPut, "http://"+"hbase"+":8080/se2:library/scanner", payload1)
	req1.Header.Set("Content-Type", "text/xml")
	req1.Header.Set("Accept", "text/plain")
	req1.Header.Set("Accept-Encoding", "identity")
	resp1, _ := http.DefaultClient.Do(req1)

	locationURL, _ := resp1.Location()
	defer resp1.Body.Close()
	return locationURL.String()

}

//Split - splits the string on colon and gives the value
func Split(s string) string {
	arr := strings.Split(s, ":")
	return arr[1]
}

//SplitKey - splits the string on colon and gives the column family
func SplitKey(s string) string {
	arr := strings.Split(s, ":")
	return strings.Title(arr[0])
}

func handlerForPath(w http.ResponseWriter, r *http.Request) {
	fmt.Println("handlerForPath for server " + serverName)
	//handle Get and Post requests
	if r.Method == "GET" {

		scanner := getScanner()
		req2, _ := http.NewRequest(http.MethodGet, scanner, nil)
		req2.Header.Set("Accept", "application/json")
		resp2, _ := http.DefaultClient.Do(req2)

		encodedJSONByte, errr := ioutil.ReadAll(resp2.Body)
		if errr != nil {
			fmt.Printf("Error: %+v", errr)
		}
		defer resp2.Body.Close()
		//decode the data
		obtainedDecodedData := stringdecoder(encodedJSONByte)

		//print data received from Hbase as it is (decoded) on the landing page
		var data EncRowsType
		json.Unmarshal(obtainedDecodedData, &data)
		fmt.Printf("unmarshalled data %v \n", data)
		// fmt.Fprintf(w, "ReceivedData", data)
		tplFuncMap := make(template.FuncMap)
		tplFuncMap["Split"] = Split
		tplFuncMap["SplitKey"] = SplitKey
		t := template.Must(template.New("library.tmpl").Funcs(tplFuncMap).Parse(`<!DOCTYPE html>
	<html lang="en">
	<head>
	  <title>
	    SE2 Library
	  </title>
	  <body>

	    <div name="library">
	    <h1>SE2 Library</h1>
	      {{range .Row}}
	              <h2>{{.Key}}</h2>
	                  {{range $index,$element := .Cell}}
	                    <h4>{{SplitKey $element.Column}}</h4>
	                    <div class="wrapper">
	                                <div class="wrapper">
	                                  <div class="box">{{Split $element.Column}}</div>
	                                  <div class="box">{{Split $element.Value}}</div>
	                                </div>
	                      </div>
	                  {{end}}
	      {{end}}

	    </div>
	  </body>
	  <style>
	  body {
	    margin: 40px;
	  }

	  .wrapper {
	    display: grid;
	    grid-template-columns: 200px 500px 500px;
	    grid-gap: 50px;
	    background-color: #fff;
	  }

	  .box {
	    border-radius: 5px;
	    padding: 20px;
	    font-size: 100%;
	  }
	  </style>
	</html>
`))

		t.Execute(w, data)

	} else if r.Method == "POST" || r.Method == "PUT" {
		fmt.Println("handlerForPath post request for server " + serverName)
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
			fmt.Printf("Error: %+v \n", errrrr)
			return
		}
		defer responsePost.Body.Close()
	}
	fmt.Fprintf(w, "proudly served by %s \n", serverName)
}
