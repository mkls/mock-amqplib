# mock-amqplib

This module is intended to replace rabbitMq in integration tests to gain speed in test
execution.

## Usage
with mock-require in tests:

```javaScript
// setup
var mockRequire = require('mock-require');
mockRequire('amqplib', 'mock-amqplib');

// teardow
mockRequire.stopAll();
```

or simply overwrite amqplibs connect method:
```javaScript
amqplib.connect = mockAmqplib.connect;
```

## Similar modules:
amqplib-mocks, exp-fake-amqplib (callbacks only), amqplib-mock,

As far as I can tell they try to solve the same problem, but they implemented different
parts of the API.

For this module I implemented whatever was neccesary to use it in the app I'm currently
developing, so parts are missing here too, pull requests are welcome.