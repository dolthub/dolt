First Milestone MS1
===================

MS1 Functionality
-----------------

- NG protocol class
- Session/Result/Cursor over NG protocol
- All required infrastructure, including: errors, I/O, memory management etc.

Limitations:

- only TCP/IP connections
- minimal or no authentication
- no session configuration: fixed parameters, no extensions (compression, multiplexing)
- no out-of-band notifications
- no CRUD operations
- one command transactions
- only synchronous operation
- simple cursors (to be specified further)

External Deliverables
---------------------

  :E1: Protocol class prototype that supports messages needed to establish a
       session, send SQL commands and process results.
  :E2: Complete protocol class and infrastructure.
  :E3: Session class 1st prototype that allows establishing a session and do
       basic session sanity checks + handle related errors.
  :E4: Session class 2nd prototype that allows sending SQL queries and process
       their results.
  :E5: Complete session class and infrastructure.


Internal Deliverables
---------------------

Designs and specifications

    :S1: Coding standards: source tree organizations, headers, namespaces etc.
    :S2: Coding standards final version.

    :S3: I/O API draft: interfaces to be implemented by TCP/IP connection implementation.
    :S4: I/O API complete designs.

    :S5: Error classes draft: classes and infrastructure for error handling.
    :S6: Error classes complete design.

    :S7: Protocol API draft: interface to be implemented by protocol class and friends;
         enough to establish and tear-down a session + error handling.
    :S8: Protocol API 1st revision: support for sending SQL queries and processing
         results;  update taking into account final I/O API designs.
    :S9: Protocol API complete design.

    :S10: Session API 1st revision: session creation and tear-down + related errors;
         update taking into account final I/O API designs.
    :S11: Session API 2nd revision: sending SQL queries and handling results.
    :S12: Session API complete design.

Infrastructure

    :I1: Code repositories.
    :I2: Cmake automation: handling external dependencies, ...
    :I3: Automated repository builds at least on Windows and Linux.
    :I4: Automated builds on major platforms (Linux, Windows, OSX, Solaris).
    :I5: Automated unit test runs.
    :I6: Unit test reports.

Code

- CDK error handling infrastructure:
    :C1: working prototype;
    :C2: I/O and system error classes to handle errors from OS level;
    :C3: code complete (unit tests).

- Base types and classes: strings, buffers, streams, async, time etc.
    :C4: first working prototype with enough functionality for TCP/IP connection
          class;
    :C5: second prototype with functionality required for protocol class;
    :C6: code complete (unit tests).

- TCP/IP connection class:
    :C7: working prototype;
    :C8: code complete (error handling, unit tests).

- Protocol class and friends:
    :C9: first working prototype that supports messages needed to setup and
          close a session + handle errors;
    :C10: second prototype that supports messages involved in executing SQL
          queries and reading their results;
    :C11: code complete (error handling, unit tests).

- Session class and friends:
    :C12: first prototype: creating session which connects to server and
          can do basic session sanity checks; graceful session tear-down;
          basic error handling;
    :C13: second prototype: sending SQL queries and reading results - simplest
          possible working version.
    :C14: code complete (error handling, unit tests).


Time plan
---------

 ===========  ==============  ===============  =================  ============
 Week         Specs           Infrastructure   Code               Deliverables
 ===========  ==============  ===============  =================  ============
 This week    **S3**          **I1**
 -----------  --------------  ---------------  -----------------  ------------
 16-20 Feb     **S1,S5**      **I3**           **C4**
 -----------  --------------  ---------------  -----------------  ------------
 23-27 Feb     **S6,S7**      **I2,I5**        **C1,C7,C9**
 -----------  --------------  ---------------  -----------------  ------------
 2-6 Mar       **S4,S8,S10**  **I4**           **C2,C6,C12**,C10   E1,E3
 -----------  --------------  ---------------  -----------------  ------------
 9-13 Mar      S2,S11         **I6**            C3, **C8**,C13     E4
 -----------  --------------  ---------------  -----------------  ------------
 16-20 Mar     S9                               C11                E2
 -----------  --------------  ---------------  -----------------  ------------
 23-27 Mar     S12                              C14                E5
 ===========  ==============  ===============  =================  ============


Detailed dependencies:

.. uml::

  scale 2/3

  participant "Designs" as spec
  participant "Infrastructure" as dev

  box "Foundation"
  participant "Errors" as err
  participant "Base\nClasses" as buff
  participant "TCP/IP\nConnection" as conn
  endbox

  box "Protocol"
  participant "Protocol" as proto
  endbox

  box "Session"
  participant "Session creation\nand tear-down" as sess
  participant "Send query\nand read results" as query
  participant "Complete\nfunctionality" as all
  endbox

  participant "CDK user" as cc

  activate spec

  == Today ==

  dev  -> conn : code repositories
  activate dev
  note left #green: I1

  spec -> conn : I/O API draft
  activate buff
  note left #green: S3

  == 16-20 Feb ==

  spec -> err : Error classes draft
  activate err
  note left #green: S5

  spec -> conn : Coding standards
  note left #green: S1

  dev -> buff : Hudson builds
  note left #green: I3

  buff -> conn : working prototype
  activate conn
  note left #green: C4

  == 23-27 Feb ==

  err -> conn : working prototype
  note left #green: C1

  dev -> conn : cmake automation
  note left #green: I2

  spec -> proto : Protocol API draft
  note left #green : S7

  conn -> proto : working prototype
  activate proto
  note left #green: C7

  proto -> sess  : first prototype (handshake)
  activate sess
  note left #green: C9

  dev -> sess : Unit tests run
  note left #green: I5

  spec -> err : Error classes design
  note left #green: S6

  == 2-6 Mar ==

  dev -> buff : Hudson builds (all platforms)
  note left #green: I4

  spec -> conn : I/O API completed
  note left #green: S4

  buff -> conn : completed
  deactivate buff
  note left #green: C6

  err  -> conn : I/O Errors
  note left #green: C2

  spec -> sess : Session API 1st revision
  note left #green: S10

  sess -> cc : first prototype
  note left #green: C12

  hnote over cc: E3

  spec -> proto : Prtocol API 1st revision
  note left #green: S8

  proto -> cc : second prototype (queries&results)
  activate query
  note left: C10

  hnote over cc: E1


  == 9-13 Mar ==

  spec -> conn : coding standards (final)
  note left: S2

  conn   -> proto : completed
  note left #green: C8
  deactivate conn

  spec  -> query : Session API 2nd revision
  note left: S11

  sess  -> query : completed
  deactivate sess

  query -> cc : second prototype
  activate all
  note left: C13

  hnote over cc : E4

  err -> proto : completed
  note left: C3
  deactivate err

  dev -> all : test reports
  note left #green: I6

  == 16-20 Mar ==

  spec -> proto : Protocol API completed
  note left : S9

  proto -> cc : completed
  note left : C11
  deactivate proto

  hnote over cc : E2

  dev -> all : fully functional
  deactivate dev

  == 23-27 Mar ==

  spec -> all : Session API completed
  note left : S12

  query -> all : completed
  deactivate query

  all -> cc : completed
  note left: C14
  deactivate all

  hnote over cc: E5

