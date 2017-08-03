Given a RPC master[demo] with properties file: 'opflow.properties'
And a RPC worker[demo]
And a counter consumer in worker[demo]
And a FibonacciGenerator consumer with names 'fibonacci' in worker[demo]
When I make a request[fib20] to routine[fibonacci] in master[demo] with input number: 20
Then the request[fib20] should finished successfully
