Scenario: Execute single RPC request

Given a RPC master<demo1> with properties file: 'opflow.properties'
Then the RPC master<demo1> connection is 'opened'
Given a RPC worker<demo1>
Then the RPC worker<demo1> connection is 'opened'
Given a counter consumer in worker<demo1>
And a FibonacciGenerator consumer with names 'fibonacci,fib' in worker<demo1>
When I make a request<fib20> to routine<fibonacci> in master<demo1> with input number: 20
When I make a request<fib30> to routine<fib> in master<demo1> with input number: 30
Then the request<fib30> should finished successfully
Then the request<fib20> should finished successfully
When I make requests from number 21 to number 30 to routine<fibonacci> in master<demo1>
Then the requests from 21 to 30 should finished successfully
Then the FibonacciGenerator consumers have performed '12' messages, in which '12' messages are successful and '0' messages are failed
Then the routine<fib> have been published '1' requests, received '1' successful messages and '0' failed messages
And the routine<fibonacci> have been published '11' requests, received '11' successful messages and '0' failed messages
When I close RPC master<demo1>
Then the RPC master<demo1> connection is 'closed'
When I close RPC worker<demo1>
Then the RPC worker<demo1> connection is 'closed'
Given a waiting time in 1 seconds


Scenario: Request to an invalid consumer

Given a RPC master<demo2>
Then the RPC master<demo2> connection is 'opened'
Given a RPC worker<demo2>
Then the RPC worker<demo2> connection is 'opened'
Given a FibonacciGenerator consumer with names 'fibonacci,fib' in worker<demo2>
When I make a request<fib25> to routine<fibonacci> in master<demo2> with input number: 25
When I make a request<fib35> to routine<fibonacci> in master<demo2> with input number: 35
Then the request<fib35> should finished successfully
Then the request<fib25> should finished successfully
When I make a request<echo>(40) to routine<echojsonobject> in master<demo2> with timeout: 500
Then the request<echo> should be timeout
And the routine<fibonacci> have been published '2' requests, received '2' successful messages and '0' failed messages
When I close RPC master<demo2>
Then the RPC master<demo2> connection is 'closed'
When I close RPC worker<demo2>
Then the RPC worker<demo2> connection is 'closed'
Given a waiting time in 1 seconds


Scenario: Request to a delay consumer

Given a RPC master<demo3>
Then the RPC master<demo3> connection is 'opened'
Given a RPC worker<demo3>
Then the RPC worker<demo3> connection is 'opened'
Given a FibonacciGenerator consumer with names 'fibonacci,fib' in worker<demo3>
When I make a request<fib25> to routine<fibonacci> in master<demo3> with input number: 25
When I make a request<fib35> to routine<fibonacci> in master<demo3> with input number: 35
Then the request<fib35> should finished successfully
Then the request<fib25> should finished successfully
When I make a request<echo>(40) to routine<echojsonobject> in master<demo3> with timeout: 5000
Given a EchoJsonObject consumer in worker<demo3> with names 'echojsonobject'
Then the request<echo> should finished successfully
And the routine<echojsonobject> have been published '1' requests, received '1' successful messages and '0' failed messages
And the routine<fibonacci> have been published '2' requests, received '2' successful messages and '0' failed messages
When I close RPC master<demo3>
Then the RPC master<demo3> connection is 'closed'
When I close RPC worker<demo3>
Then the RPC worker<demo3> connection is 'closed'