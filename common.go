package pubsub

import (
	"strings"
	"errors"
	"strconv"
)

const REQUEST_HEADER_TOKEN_LEN = 2
const RESPONSE_HEADER_TOKEN_LEN = 3
const SUBHEADER_TOKEN_LEN = 2

func processHeaderRequest(token string)(cmd string, topic string, err error){

	subTokens :=strings.Split(token, " ")
	if len(subTokens) != REQUEST_HEADER_TOKEN_LEN{
		return "","",errors.New("wrong format")
	}
	for indx,currentSubToken := range subTokens {
		switch indx {
		case 0:
			//CMD
			cmd = strings.TrimSpace(currentSubToken)
		case 1:
			//TOPIC
			topic = strings.TrimSpace(currentSubToken)
		}
	}
	return cmd,topic,nil
}

func processHeaderResponse(token string)(cmd string, statusCode uint16,statusMsg string, err error){

	subTokens :=strings.Split(token, " ")
	if len(subTokens) != RESPONSE_HEADER_TOKEN_LEN{
		return "",0,"",errors.New("wrong format")
	}
	for index,currentSubToken := range subTokens {
		switch index {
		case 0:
			//CMD
			cmd = strings.TrimSpace(currentSubToken)
		case 1:
			//STATUS CODE
			tmp := strings.TrimSpace(currentSubToken)
			tmpRes, _ := strconv.ParseUint(tmp, 10, 16)
			statusCode = uint16(tmpRes)
		case 2:
			//STATUS MSG
			statusMsg = strings.TrimSpace(currentSubToken)
		}
	}
	return cmd,statusCode,statusMsg,nil
}

func processSubHeader(token string)(k string,v string,err error){

	subTokens := strings.Split(token, ":")
	if len(subTokens) != SUBHEADER_TOKEN_LEN{
		return "","",errors.New("wrong format")
	}
	for index,currentSubToken := range subTokens {
		switch index {
		case 0:
			k = strings.TrimSpace(currentSubToken)
		case 1:
			v= strings.TrimSpace(currentSubToken)
		}
	}
	return k,v,nil
}

func processBody(token string)(string){

	return strings.TrimSpace(token)
}

func ParseRequest(request string)(cmd string,topic string,subHeaders map[string]string,body string,err error) {

	tokens := strings.Split(request, "\r\n")

	emptyLine := false
	for index, currentToken := range tokens {

		if currentToken == ""{
			emptyLine = true
			continue
		}
		switch index {
		case 0:
			//HEADER
			var procErr error
			cmd,topic,procErr = processHeaderRequest(currentToken)
			if procErr != nil{
				return "","",nil,"",procErr
			}
		default:

			//we still do not meat empty line so - process sub headers
			if emptyLine == false {
				k,v,procSubHeaderErr := processSubHeader(currentToken)
				if procSubHeaderErr != nil {
					return "","",nil,"",procSubHeaderErr
				}
				if subHeaders == nil {
					subHeaders = make(map[string]string)
				}
				subHeaders[k]=v
			}else {
				body = processBody(currentToken)
			}
		}
	}

	return cmd,topic,subHeaders,body,nil
}

func ParseResponse(request string)(cmd string,statusCode uint16,statusMsg string,subHeaders map[string]string,body string,err error) {

	tokens := strings.Split(request, "\r\n")

	emptyLine := false
	for index, currentToken := range tokens {

		if currentToken == ""{
			emptyLine = true
			continue
		}
		switch index {
		case 0:
			//HEADER
			var procErr error
			cmd,statusCode,statusMsg,procErr = processHeaderResponse(currentToken)
			if procErr != nil{
				return "",0,"",nil,"",procErr
			}
		default:

			//we still do not meat empty line so - process sub headers
			if emptyLine == false {
				k,v,procSubHeaderErr := processSubHeader(currentToken)
				if procSubHeaderErr != nil {
					return "",0,"",nil,"",procSubHeaderErr
				}
				if subHeaders == nil {
					subHeaders = make(map[string]string)
				}
				subHeaders[k]=v
			}else {
				body = processBody(currentToken)
			}
		}
	}

	return cmd,statusCode,statusMsg,subHeaders,body,nil
}

func getSubHeaders(subHeaders map[string]string)string{
	var result string
	for k,v := range subHeaders{
		result = k + ":" + v + "\r\n"
	}
	return result
}