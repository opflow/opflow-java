Scenario: Execute single RPC request

Given a RPC master<demo> with properties file: 'opflow.properties'
Then the RPC master<demo> connection is 'opened'
Given a RPC worker<demo>
Then the RPC worker<demo> connection is 'opened'
Given a counter consumer in worker<demo>
And a FibonacciGenerator consumer with names 'fibonacci,fib' in worker<demo>
When I make a request<fib20> to routine<fibonacci> in master<demo> with input number: 20
When I make a request<fib30> to routine<fib> in master<demo> with input number: 30
Then the request<fib30> should finished successfully
Then the request<fib20> should finished successfully
Given a waiting time in 5 seconds
When I make requests from number 21 to number 30 to routine<fibonacci> in master<demo>
Then the requests from 21 to 30 should finished successfully
Then the FibonacciGenerator consumers have performed '12' messages, in which '12' messages are successful and '0' messages are failed
Then the routine<fib> have been published '1' requests, received '1' successful messages and '0' failed messages
And the routine<fibonacci> have been published '11' requests, received '11' successful messages and '0' failed messages
When I close RPC master<demo>
Then the RPC master<demo> connection is 'closed'
When I close RPC worker<demo>
Then the RPC worker<demo> connection is 'closed'


Scenario: Request to an invalid consumer

Given a RPC master<demo>
Then the RPC master<demo> connection is 'opened'
Given a RPC worker<demo>
Then the RPC worker<demo> connection is 'opened'
Given a FibonacciGenerator consumer with names 'fibonacci,fib' in worker<demo>
When I make a request<fib25> to routine<fibonacci> in master<demo> with input number: 25
When I make a request<fib35> to routine<fibonacci> in master<demo> with input number: 35
Then the request<fib35> should finished successfully
Then the request<fib25> should finished successfully
When I make a request<echo>(40) to routine<echojsonobject> in master<demo> with timeout: 500
Then the request<echo> should be timeout
And the routine<fibonacci> have been published '2' requests, received '2' successful messages and '0' failed messages
When I close RPC master<demo>
Then the RPC master<demo> connection is 'closed'
When I close RPC worker<demo>
Then the RPC worker<demo> connection is 'closed'


Scenario: Request to a delay consumer

Given a RPC master<demo>
Then the RPC master<demo> connection is 'opened'
Given a RPC worker<demo>
Then the RPC worker<demo> connection is 'opened'
Given a FibonacciGenerator consumer with names 'fibonacci,fib' in worker<demo>
When I make a request<fib25> to routine<fibonacci> in master<demo> with input number: 25
When I make a request<fib35> to routine<fibonacci> in master<demo> with input number: 35
Then the request<fib35> should finished successfully
Then the request<fib25> should finished successfully
When I make a request<echo>(40) to routine<echojsonobject> in master<demo> with timeout: 5000
Given a EchoJsonObject consumer in worker<demo> with names 'echojsonobject'
Then the request<echo> should finished successfully
And the routine<fibonacci> have been published '2' requests, received '2' successful messages and '0' failed messages
And the routine<echojsonobject> have been published '1' requests, received '1' successful messages and '0' failed messages
When I close RPC master<demo>
Then the RPC master<demo> connection is 'closed'
When I close RPC worker<demo>
Then the RPC worker<demo> connection is 'closed'