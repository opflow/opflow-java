Simple Serverlet story

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
When I send a request to Commander 'commander1' to calculate fibonacci of '34'
