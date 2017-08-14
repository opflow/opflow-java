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
When I close RPC master<demo>
Then the RPC master<demo> connection is 'closed'
When I close RPC worker<demo>
Then the RPC worker<demo> connection is 'closed'
