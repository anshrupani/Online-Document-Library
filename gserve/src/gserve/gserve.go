package main

import (
        "net/http"
	"fmt"
	"io/ioutil"
	"encoding/json"
	"log"
	"bytes"
)


func main() {
	//handle requests with "/library" path
	http.HandleFunc("/library", handlerForPath)
	//serverport for this instance
	log.Fatal(http.ListenAndServe(":9094", nil))
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
}
