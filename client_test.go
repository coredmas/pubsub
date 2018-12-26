package pubsub

import "testing"

func TestPubSubClientCreation(t *testing.T) {

	cl := NewClient("tcp","localhost",9090)
	if cl == nil{
		t.Errorf("NewClient is nil")
	}
	if cl.Port != 9090 || cl.Proto != "tcp" || cl.Address != "localhost"{
		t.Errorf("NewClient parameters are incorrect")
	}
}

func TestPubSubBrokerCreation(t *testing.T) {

	br := NewBroker("tcp",9090)
	if br == nil{
		t.Errorf("NewBroker is nil")
	}
	if br.Port != 9090 || br.Proto != "tcp" {
		t.Errorf("NewBroker parameters are incorrect")
	}
}

func TestPubSub(t *testing.T) {

	br := NewBroker("tcp",9090)
	if br == nil{
		t.Errorf("NewBroker is nil")
	}

	br.StartBrokerTest()

	cl := NewClient("tcp","localhost",9090)
	if cl == nil{
		t.Errorf("NewClient is nil")
	}


	statusCode,statusMsg,bodyResponse,err :=cl.Subscribe("test","test_client")

	if statusCode != 200{
		t.Errorf("Status code is not 200 for Subscribe operation")
	}

	if statusMsg != "OK"{
		t.Errorf("Status msg is not 200 for Subscribe operation")
	}

	if bodyResponse != "" {
		t.Errorf("bodyResponse is not empty for Subscribe operation")
	}

	if err != nil{
		t.Errorf(err.Error())
	}

	statusCode,statusMsg,bodyResponse,err = cl.Publish("test","{'id':0,'text':'msg'}")

	if statusCode != 200{
		t.Errorf("Status code is not 200 for Publish operation")
	}

	if statusMsg != "OK"{
		t.Errorf("Status msg is not 200 for Publish operation")
	}

	if bodyResponse != "" {
		t.Errorf("bodyResponse is not empty for Publish operation")
	}

	if err != nil{
		t.Errorf(err.Error())
	}

	statusCode,statusMsg,bodyResponse,err = cl.Poll("test","{'id':0,'text':'msg'}",0)
	if statusCode != 200{
		t.Errorf("Status code is not 200 for Poll operation")
	}

	if statusMsg != "OK"{
		t.Errorf("Status msg is not 200 for Poll operation")
	}

	if bodyResponse != "[{'id':0,'text':'msg'}]" {
		t.Errorf("bodyResponse is not empty for Poll operation")
	}

	if err != nil{
		t.Errorf(err.Error())
	}

	statusCode,statusMsg,bodyResponse,err = cl.UnSubscribe("test","test_client")
	if statusCode != 200{
		t.Errorf("Status code is not 200 for Poll operation")
	}

	if statusMsg != "OK"{
		t.Errorf("Status msg is not 200 for Poll operation")
	}

	if bodyResponse != "" {
		t.Errorf("bodyResponse is not empty for Poll operation")
	}

	if err != nil{
		t.Errorf(err.Error())
	}
}
