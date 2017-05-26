# JSON Data flow based on rabbitmq-client-java

## Usage

### Install dependencies

Change working directory to project homedir:

```
$ cd /<PROJECTDIR>/rabbitmq-client-java/
```

To install dependent libraries, use `mvn`:

```
$ mvn compile
```

### Run

To consume message from RabbitMQ, use the following command:

```
$ mvn compile exec:exec -Drun
```
