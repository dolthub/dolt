# Contributing Guidelines

We love getting feedback from our users. Bugs and code contributions are great forms of feedback and we thank you for any bugs you report or code you contribute.

## Reporting Issues

Before reporting a new bug, please check first to see if a similar bug [exists](https://bugs.mysql.com/search.php).

Bug reports should be as complete as possible.  Please try and include the following:

* complete steps to reproduce the issue
* any information about platform and environment that could be specific to the bug  
* Specific version of the product you are using
* Specific version of the server being used
* C++ code to help reproduce the issue if possible

## Contributing Code

Contributing to this project is easy. You just need to follow these steps.

* Sign the Oracle Contributor Agreement. You can find instructions for doing that at [OCA Page](https://www.oracle.com/technetwork/community/oca-486395.html)
* Develop your pull request
  * Make sure you are aware of the requirements for the project (i.e. don't require C++17 if we are supporting C++11 and higher)
* Validate your pull request by including tests that sufficiently cover the functionality
* Verify that the entire test suite passes with your code applied
* Submit your pull request

## Running Tests

Any contributed code should pass our unit tests.
To run the unit tests you need to perform the following steps:

* Build the Connector/C++ with the cmake option enabling unit tests (-DWITH_TESTS=1)
* Run MySQL Server
* Set the following environment variables:
  * XPLUGIN_PORT = <the port number of XPlugin in MySQL Server>
  * XLPIGIN_USER = <MySQL user name>
  * XPLUGIN_PASSWORD = <MySQL password>
* In the OS command line enter the Connector/C++ build directory and run `ctest` utility

At the end of `ctest` run the result should indicate 100% tests passed.
