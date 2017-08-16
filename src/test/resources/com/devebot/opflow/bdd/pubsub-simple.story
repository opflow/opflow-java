Scenario: Simple Publisher/Subscriber

Given a PubsubHandler named 'pubsub1'
Then the PubsubHandler named 'pubsub1' connection is 'opened'
Given a subscriber named 'EchoJsonObject' in PubsubHandler named 'pubsub1'
When I publish '10000' random messages to PubsubHandler named 'pubsub1'
Then PubsubHandler named 'pubsub1' receives '10000' messages
When I close PubsubHandler named 'pubsub1'
Then the PubsubHandler named 'pubsub1' connection is 'closed'
