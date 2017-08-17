Scenario: Simple Publisher/Subscriber

Given a PubsubHandler named 'pubsub2' with properties file: 'pubsub-subscriber.properties'
Then the PubsubHandler named 'pubsub2' connection is 'opened'
Given a subscriber named 'EchoJsonObject' in PubsubHandler named 'pubsub2'
When I publish '10000' random messages to PubsubHandler named 'pubsub2'
Then PubsubHandler named 'pubsub2' receives '10000' messages
When I close PubsubHandler named 'pubsub2'
Then the PubsubHandler named 'pubsub2' connection is 'closed'
