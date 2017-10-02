Batched requests sending story

Lifecycle:
Before:
Given a Commander named 'commander1' with properties file: 'commander.properties'
Given a Serverlet named 'serverlet1' with properties file: 'serverlet.properties'
After:
Outcome: ANY
When I close Commander named 'commander1'
When I close Serverlet named 'serverlet1'

Scenario: Simple Serverlet
Given a registered FibonacciCalculator interface in Commander named 'commander1'
Given an instantiated FibonacciCalculator class in Serverlet named 'serverlet1'
When I make '100' requests to Commander 'commander1' to calculate fibonacci of random number from '20' to '50'
Then the commander 'commander1' will receive '100' responses