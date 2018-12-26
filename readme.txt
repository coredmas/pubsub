-->
PUBLISH/1.0 topicName\r\n
\r\n
BODY_OBJECT\r\n
<--
PUBLISH/1.0 200 OK\r\n
\r\n
or
<--
PUBLISH/1.0 40X CLIENT_ERROR_X\r\n
\r\n
or
<--
PUBLISH/1.0 50X CLIENT_ERROR_X\r\n
\r\n
*****
-->
SUBSCRIBE/1.0 topicName\r\n
Subscribe-Name:subscriberName\r\n
\r\n
<--
SUBSCRIBE/1.0 200 OK\r\n
\r\n
or
<--
SUBSCRIBE/1.0 40X CLIENT_ERROR_X\r\n
\r\n
or
<--
SUBSCRIBE/1.0 50X CLIENT_ERROR_X\r\n
\r\n
*****
-->
UNSUBSCRIBE/1.0 topicName\r\n
Subscribe-Name:subscriberName\r\n
\r\n
<--
UNSUBSCRIBE/1.0 200 OK\r\n
\r\n
or
<--
UNSUBSCRIBE/1.0 40X CLIENT_ERROR_X\r\n
\r\n
or
<--
UNSUBSCRIBE/1.0 50X CLIENT_ERROR_X\r\n
\r\n
*****
-->
POLL/1.0 topicName\r\n
Subscribe-Name:subscriberName\r\n
Subscribe-Number:0\r\n
\r\n
<--
POLL/1.0 200 OK\r\n
\r\n
BODY_ARRAY_OF_OBJECTS\r\n
or
<--
POLL/1.0 40X CLIENT_ERROR_X\r\n
\r\n
or
<--
POLL/1.0 50X CLIENT_ERROR_X\r\n
\r\n

*Seq-Number starts from 0
*BODY_OBJECT - json object {}
*BODY_ARRAY_OF_OBJECTS - array of json objects [{}]
*****

SUBSCRIBE/1.0 topicName\r\n
Sub-Name:subscriberName\r\n
Sub-Name:subscriberName\r\n
Sub-Name:subscriberName\r\n
Sub-Name:subscriberName\r\n
\r\n
BODY\r\n