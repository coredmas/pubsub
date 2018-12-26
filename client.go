package pubsub

import (
	"net"
	"fmt"
	"time"
	"errors"
	"strconv"
)

type Client struct {
	Proto string
	Address string
	Port uint16
}

func NewClient(proto,address string,port uint16) *Client {
	return &Client{Proto:proto,Address:address,Port:port}
}

func (cl *Client)Publish(topic,bodyRequest string) (uint16,string,string,error){

	//TODO add support for topic wildcard like root.* or root.chield_level1.*
	timeout := 5 * time.Second
	//TODO add log library debug msg for input params
	strAddr := cl.Address+":"+ strconv.Itoa(int(cl.Port))

	msgToServer := fmt.Sprintf("PUBLISH/1.0 %s\r\n\r\n%s\r\n",topic,bodyRequest)
	//TODO add TLS
	conn, err := net.Dial(cl.Proto, strAddr)
	defer  conn.Close()

	if err != nil {
		//TODO add log library error msg
		println("Dial failed", err.Error())
		return 0,"","",err
	}

	totalBytesWrite, err := conn.Write([]byte(msgToServer))
	if err != nil {
		//TODO add log library error msg
		println("Write to server failed", err.Error())
		return 0,"","",err
	}
	if totalBytesWrite == 0{
		//TODO add log library error msg
		println("Write operation failed!")
		return 0,"","",errors.New("Write operation failed!")
	}

	for {
		conn.SetReadDeadline(time.Now().Add(timeout))
		msgFromServer := make([]byte, 1024)
		totalBytesRead, err := conn.Read(msgFromServer)
		if err != nil {
			//TODO add log library error msg
			println("Write to server failed:", err.Error())
			return 0,"","",err
		}else{
			cmd,statusCode,statusMsg,subHeadersResponse,bodyResponse,err := ParseResponse(string(msgFromServer[0:totalBytesRead]))

			if cmd != "PUBLISH/1.0"{
				return 0,"","",errors.New("wrong cmd response")
			}

			if err != nil {
				return 0,"","",err
			}

			//TODO process subHeadersResponse or return it for client needs
			subHeadersResponse = subHeadersResponse
			return statusCode,statusMsg,bodyResponse,nil
		}
		break
	}
	return 0,"","",errors.New("wrong cmd")
}

func (cl *Client)Subscribe(topic,subscriberName string) (uint16,string,string,error){

	subHeadersRequest := make(map[string]string)
	subHeadersRequest["Subscribe-Name"] = subscriberName
	subHeadersRaw := getSubHeaders(subHeadersRequest)

	timeout := 5 * time.Second
	//TODO add log library debug msg for input params
	strAddr := cl.Address+":"+ strconv.Itoa(int(cl.Port))

	msgToServer := fmt.Sprintf("SUBSCRIBE/1.0 %s\r\n%s\r\n",topic,subHeadersRaw)
	//TODO add TLS
	conn, err := net.Dial(cl.Proto,  strAddr)
	defer  conn.Close()

	if err != nil {
		//TODO add log library error msg
		println("Dial failed", err.Error())
		return 0,"","",err
	}

	totalBytesWrite, err := conn.Write([]byte(msgToServer))
	if err != nil {
		//TODO add log library error msg
		println("Write to server failed", err.Error())
		return 0,"","",err
	}
	if totalBytesWrite == 0{
		//TODO add log library error msg
		println("Write operation failed!")
		return 0,"","",errors.New("Write operation failed!")
	}

	for {
		conn.SetReadDeadline(time.Now().Add(timeout))
		msgFromServer := make([]byte, 1024)
		totalBytesRead, err := conn.Read(msgFromServer)
		if err != nil {
			//TODO add log library error msg
			println("Write to server failed:", err.Error())
			return 0,"","",err
		}else{
			cmd,statusCode,statusMsg,subHeadersResponse,bodyResponse,err := ParseResponse(string(msgFromServer[0:totalBytesRead]))

			if cmd != "SUBSCRIBE/1.0"{
				return 0,"","",errors.New("wrong cmd response")
			}

			if err != nil {
				return 0,"","",err
			}

			//TODO process subHeadersResponse or return it for client needs
			subHeadersResponse = subHeadersResponse

			return statusCode,statusMsg,bodyResponse,nil
		}
		break
	}
	return 0,"","",errors.New("wrong cmd")
}

func (cl *Client)UnSubscribe(topic,subscriberName string) (uint16,string,string,error){

	subHeadersRequest := make(map[string]string)
	subHeadersRequest["Subscribe-Name"] = subscriberName
	subHeadersRaw := getSubHeaders(subHeadersRequest)

	timeout := 5 * time.Second
	//TODO add log library debug msg for input params
	strAddr := cl.Address+":"+ strconv.Itoa(int(cl.Port))

	msgToServer := fmt.Sprintf("UNSUBSCRIBE/1.0 %s\r\n%s\r\n",topic,subHeadersRaw)
	//TODO add TLS
	conn, err := net.Dial(cl.Proto,  strAddr)
	defer  conn.Close()

	if err != nil {
		//TODO add log library error msg
		println("Dial failed", err.Error())
		return 0,"","",err
	}

	totalBytesWrite, err := conn.Write([]byte(msgToServer))
	if err != nil {
		//TODO add log library error msg
		println("Write to server failed", err.Error())
		return 0,"","",err
	}
	if totalBytesWrite == 0{
		//TODO add log library error msg
		println("Write operation failed!")
		return 0,"","",errors.New("Write operation failed!")
	}

	for {
		conn.SetReadDeadline(time.Now().Add(timeout))
		msgFromServer := make([]byte, 1024)
		totalBytesRead, err := conn.Read(msgFromServer)
		if err != nil {
			//TODO add log library error msg
			println("Write to server failed:", err.Error())
			return 0,"","",err
		}else{
			cmd,statusCode,statusMsg,subHeadersResponse,bodyResponse,err := ParseResponse(string(msgFromServer[0:totalBytesRead]))

			if cmd != "UNSUBSCRIBE/1.0"{
				return 0,"","",errors.New("wrong cmd response")
			}

			if err != nil {
				return 0,"","",err
			}

			//TODO process subHeadersResponse or return it for client needs
			subHeadersResponse = subHeadersResponse

			return statusCode,statusMsg,bodyResponse,nil
		}
		break
	}
	return 0,"","",errors.New("wrong cmd")
}

func (cl *Client)Poll(topic,subscriberName string,subscriberNumber uint64) (uint16,string,string,error){

	subHeadersRequest := make(map[string]string)
	subHeadersRequest["Subscribe-Name"] = subscriberName
	subHeadersRequest["Subscribe-Number"] =  strconv.Itoa(int(subscriberNumber))

	subHeadersRaw := getSubHeaders(subHeadersRequest)

	timeout := 5 * time.Second
	//TODO add log library debug msg for input params
	strAddr := cl.Address+":"+ strconv.Itoa(int(cl.Port))

	msgToServer := fmt.Sprintf("POLL/1.0 %s\r\n%s\r\n",topic,subHeadersRaw)
	//TODO add TLS
	conn, err := net.Dial(cl.Proto, strAddr)
	defer  conn.Close()

	if err != nil {
		//TODO add log library error msg
		println("Dial failed", err.Error())
		return 0,"","",err
	}

	totalBytesWrite, err := conn.Write([]byte(msgToServer))
	if err != nil {
		//TODO add log library error msg
		println("Write to server failed", err.Error())
		return 0,"","",err
	}
	if totalBytesWrite == 0{
		//TODO add log library error msg
		println("Write operation failed!")
		return 0,"","",errors.New("Write operation failed!")
	}

	for {
		conn.SetReadDeadline(time.Now().Add(timeout))
		//TODO array size 1024 will not enough
		msgFromServer := make([]byte, 1024)
		totalBytesRead, err := conn.Read(msgFromServer)
		if err != nil {
			//TODO add log library error msg
			println("Write to server failed:", err.Error())
			return 0,"","",err
		}else{
			cmd,statusCode,statusMsg,subHeadersResponse,bodyResponse,err := ParseResponse(string(msgFromServer[0:totalBytesRead]))

			if cmd != "POLL/1.0"{
				return 0,"","",errors.New("wrong cmd response")
			}

			if err != nil {
				return 0,"","",err
			}

			//TODO process subHeadersResponse or return it for client needs
			subHeadersResponse = subHeadersResponse

			return statusCode,statusMsg,bodyResponse,nil
		}
		break
	}
	return 0,"","",errors.New("wrong cmd")
}