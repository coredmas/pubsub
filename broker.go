package pubsub

import (
	"sync"
	"fmt"
	"net"
	"time"
	"errors"
	"strconv"
)

type Broker struct {
	Proto string
	Port uint16

	TopicMsges map[string][]*string
	TopicSubNames map[string][]*string
	SyncObj   sync.Mutex
}

func NewBroker(proto string,port uint16) *Broker {
	return &Broker{Proto:proto,Port:port}
}

func (br *Broker)StartBroker() {

	//TODO add log library
	fmt.Println("Starting broker...")

	br.TopicMsges =  make(map[string][]*string)
	br.TopicSubNames =  make(map[string][]*string)

	newConns :=  make(chan net.Conn, 128)
	deadConns := make(chan net.Conn, 128)

	listener, err := net.Listen(br.Proto, ":" + string(br.Port))
	if err != nil { panic(err) }
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil { panic(err) }
			newConns <- conn
		}
	}()

	for {
		select {

		case conn := <-newConns:
			go func() {
				//TODO close connection if during 30 sec we did not get any data
				br.NewConnection(conn)

				buf := make([]byte, 1024)
				for {
					timeout := 5 * time.Second
					conn.SetReadDeadline(time.Now().Add(timeout))
					nbyte, err := conn.Read(buf)
					if err != nil {
						deadConns <- conn
						break
					} else {

						requestBytes := make([]byte, nbyte)
						copy(requestBytes, buf[:nbyte])
						responseStr := br.NewMessage(string(requestBytes))
						responseBytes := []byte(responseStr)

						go func(conn net.Conn) {
							totalWritten := 0
							for totalWritten < len(responseBytes) {
								writtenThisCall, err := conn.Write(responseBytes[totalWritten:])
								if err != nil {
									deadConns <- conn
									break
								}
								totalWritten += writtenThisCall
							}
							defer conn.Close()
						}(conn)
					}
				}
			}()

		case deadConn := <-deadConns:
			err := deadConn.Close()
			br.ConnectionClosed(deadConn,err)
			//TODO add case to stop broker correctly
		}
	}
	listener.Close()
	fmt.Println("Stoping broker...")
}
//TODO create this instance only for test
func (br *Broker)StartBrokerTest() {

	//TODO add log library
	fmt.Println("Starting broker...")

	br.TopicMsges =  make(map[string][]*string)
	br.TopicSubNames =  make(map[string][]*string)

	newConns :=  make(chan net.Conn, 128)
	deadConns := make(chan net.Conn, 128)

	address :=  ":" + strconv.Itoa(int(br.Port))
	listener, err := net.Listen(br.Proto, address)
	if err != nil { panic(err) }

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil { panic(err) }
			newConns <- conn
		}
	}()

	go func() {
		for {
			select {

			case conn := <-newConns:
				go func() {
					//TODO close connection if during 30 sec we did not get any data
					br.NewConnection(conn)

					buf := make([]byte, 1024)
					for {
						timeout := 5 * time.Second
						conn.SetReadDeadline(time.Now().Add(timeout))
						nbyte, err := conn.Read(buf)
						if err != nil {
							deadConns <- conn
							break
						} else {

							requestBytes := make([]byte, nbyte)
							copy(requestBytes, buf[:nbyte])
							responseStr := br.NewMessage(string(requestBytes))
							responseBytes := []byte(responseStr)

							go func(conn net.Conn) {
								totalWritten := 0
								for totalWritten < len(responseBytes) {
									writtenThisCall, err := conn.Write(responseBytes[totalWritten:])
									if err != nil {
										deadConns <- conn
										break
									}
									totalWritten += writtenThisCall
								}
								defer conn.Close()
							}(conn)
						}
					}
				}()

			case deadConn := <-deadConns:
				err := deadConn.Close()
				br.ConnectionClosed(deadConn,err)
				//TODO add case to stop broker correctly
			}
		}
			listener.Close()
			fmt.Println("Stoping broker...")
	}()

}


func (br *Broker)NewConnection(conn net.Conn){

	//TODO add log library
	println("new connection %s", conn.RemoteAddr().String())
}

func getStatusInfoClientError()(uint16,string ){
	return 400,"CLIENT_ERROR_400"
}

func getStatusInfoServerError()(uint16,string ){
	return 500,"CLIENT_ERROR_500"
}

func getStatusInfoOk()(uint16,string ){
	return 200,"OK"
}
func (br *Broker)NewMessage(message string)string{

	cmd ,topic,subHeaders,bodyRequest,err := ParseRequest(message)
	var msgToClient string
	if err != nil {
		statusCode,statusMsg := getStatusInfoClientError()
		msgToClient = fmt.Sprintf("%s %d %s\r\n\r\n",cmd,statusCode,statusMsg)
		return msgToClient
	}
	var statusCode uint16
	var statusMsg string

	switch cmd {
	case "PUBLISH/1.0":
		errProcPublish := br.processPublish(topic,subHeaders,bodyRequest)
		if errProcPublish != nil {
			statusCode,statusMsg = getStatusInfoClientError()
		}else{
			statusCode,statusMsg = getStatusInfoOk()
		}
		msgToClient = fmt.Sprintf("%s %d %s\r\n\r\n",cmd,statusCode,statusMsg)

	case "SUBSCRIBE/1.0":
		errProcSubscribe := br.processSubscribe(topic,subHeaders,bodyRequest)
		if errProcSubscribe != nil {
			statusCode,statusMsg = getStatusInfoClientError()
		}else{
			statusCode,statusMsg = getStatusInfoOk()
		}
		msgToClient = fmt.Sprintf("%s %d %s\r\n\r\n",cmd,statusCode,statusMsg)

	case "UNSUBSCRIBE/1.0":
		errProcUnSubscribe := br.processUnSubscribe(topic,subHeaders,bodyRequest)
		if errProcUnSubscribe != nil {
			statusCode,statusMsg = getStatusInfoClientError()
		}else{
			statusCode,statusMsg = getStatusInfoOk()
		}
		msgToClient = fmt.Sprintf("%s %d %s\r\n\r\n",cmd,statusCode,statusMsg)

	case "POLL/1.0":
		bodyResponse,errProcPoll := br.processPoll(topic,subHeaders,bodyRequest)
		if errProcPoll != nil {
			statusCode,statusMsg = getStatusInfoClientError()
			msgToClient = fmt.Sprintf("%s %d %s\r\n\r\n",cmd,statusCode,statusMsg)
		}else{
			statusCode,statusMsg = getStatusInfoOk()
			msgToClient = fmt.Sprintf("%s %d %s\r\n\r\n%s\r\n",cmd,statusCode,statusMsg,bodyResponse)
		}

	default:
		statusCode,statusMsg = getStatusInfoClientError()
		msgToClient = fmt.Sprintf("%s %d %s\r\n\r\n",cmd,statusCode,statusMsg)
	}

	return msgToClient
}
func (br *Broker)ConnectionClosed(conn net.Conn,err error){
	println("connection from %s was closed", conn.RemoteAddr().String())
}

func (br *Broker)processPublish(topic string,subHeaders map[string]string,body string)(error){

	if topic == "" || body == "" {
		return errors.New("wrong format")
	}
	br.SyncObj.Lock()
	defer br.SyncObj.Unlock()

	br.TopicMsges[topic] = append( br.TopicMsges[topic], &body)
	return nil
}
func (br *Broker)processSubscribe(topic string,subHeaders map[string]string,body string)(error){

	subName,ok := subHeaders["Subscribe-Name"]
	if ok == false{
		return errors.New("Subscribe-Name is empty")
	}
	br.SyncObj.Lock()
	defer br.SyncObj.Unlock()
	br.TopicSubNames[topic] = append( br.TopicMsges[topic], &subName)
	return nil
}
func (br *Broker)processUnSubscribe(topic string,subHeaders map[string]string,body string)(error){

	subName,ok := subHeaders["Subscribe-Name"]
	if ok == false{
		return errors.New("Subscribe-Name is empty")
	}
	br.SyncObj.Lock()
	defer br.SyncObj.Unlock()

	for index := 0; index < len(br.TopicSubNames[topic]); index++{
		value := br.TopicSubNames[topic][index]
		if *value == subName{
			br.TopicSubNames[topic] = append(br.TopicSubNames[topic][:index], br.TopicSubNames[topic][index+1:]...)
			index = 0
		}
	}
	//there is no subscribers for this topic
	if len(br.TopicSubNames[topic]) == 0{
		delete (br.TopicMsges,topic)
	}
	return nil
}
func (br *Broker)processPoll(topic string,subHeaders map[string]string,body string)(string,error){

	if topic == ""  {
		return "",errors.New("wrong format")
	}
	seqNumberStr,ok := subHeaders["Subscribe-Number"]
	if ok == false{
		return "",errors.New("Subscribe-Number is empty")
	}
	seqNumberInt, errParseSeqNumber := strconv.ParseUint(seqNumberStr, 10, 64)
	if errParseSeqNumber != nil{
		return "",errors.New("can not parse Subscribe-Number")
	}
	br.SyncObj.Lock()
	defer br.SyncObj.Unlock()
	bodyResponse := "["

	for index := int(seqNumberInt); index < len(br.TopicMsges[topic]); index++ {
		bodyResponse += *br.TopicMsges[topic][index]
	}
	bodyResponse+="]"

	return bodyResponse,nil
}
