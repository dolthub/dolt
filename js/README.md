# NOMS JS

This provides a library to read data from a NOMS server.

## Requirements

As usual, do `npm install`.

## Testing

We use [Mocha](https://mochajs.org/)/[Chai](http://chaijs.com/) for our JS unit tests. These currently only run in a browser so we use [Karma](https://karma-runner.github.io/).

To start testing run:

```
npm run start-tests
```

This starts the Karma test server and launches a bunch of browser windows that connects to this server. Keep this process running as you work on your tests. The test runner detects changes in the test files and will rerun the tests when something changes and report the results in the same terminal.

You can also manually run the tests and see the results in a terminal by doing `karma run`. To do this you first need to globally install karma by doing `npm install -g karma`.

### Debugging in browser

When you ran `npm run start-test` one or more browser windows should have opened. Find the the browser you care to test in. In the upper right of the status page there should be *Debug* button. Clicking that takes you to a page running your test. You can open dev tool there etc.
