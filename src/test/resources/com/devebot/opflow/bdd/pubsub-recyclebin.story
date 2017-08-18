Scenario: Simple Publisher/Recyclebin

Given a PubsubHandler named 'pubsub1' with properties file: 'pubsub-recyclebin.properties'
Then the PubsubHandler named 'pubsub1' connection is 'opened'
When I purge subscriber in PubsubHandler named 'pubsub1'
Then subscriber in PubsubHandler named 'pubsub1' has '0' messages
When I purge recyclebin in PubsubHandler named 'pubsub1'
Then recyclebin in PubsubHandler named 'pubsub1' has '0' messages
Given a subscriber named 'EchoRandomError' in PubsubHandler named 'pubsub1'
When I publish '1000' random messages to subscriber 'EchoRandomError' on PubsubHandler named 'pubsub1'
And waiting for subscriber of PubsubHandler(pubsub1) finish
Then PubsubHandler named 'pubsub1' receives '1015' messages
And subscriber in PubsubHandler named 'pubsub1' has '0' messages
And recyclebin in PubsubHandler named 'pubsub1' has '5' messages
When I close PubsubHandler named 'pubsub1'
Then the PubsubHandler named 'pubsub1' connection is 'closed'
