Using CDK from another project
==============================

CDK library is prepared to be used from projects that use cmake for their
build configuration. CDK sources can be included as a sub-project of the
main project, adding cdk targets to be used by other targets of the main
project.

To use CDK in another project, first step is to put CDK sources in a location
within project's main folder. One way to do it is by creating a git
sub-module within the git repository of the main project. CDK's main
git repository is at *myrepo:cdkbase* ("master" branch).

Let asume that CDK sources are located in ``cdk/`` sub-folder of
project's root folder. CDK should be included from project's main
``CMakeLists.txt`` as follows::

  ADD_SUBDIRECTORY(cdk)

CDK requires boost and protobuf to be built. If these libraries can not
be found automatically, locations should be specified using ``WITH_BOOST``
and ``WITH_PROTOBUF`` configuration options.

To build a library target ``foo`` which uses CDK
one should declare it as follows in ``CMakeLists.txt``::

  ADD_LIBRARY(foo ...)
  ADD_CDK(foo)

The :func:`ADD_CDK` macro takes care of adding required libraries to the
``foo`` target, setting up include paths etc. After this the sources of
``foo`` can include CDK headers and use CDK objects as described in
documentation.

An alternative is to setup CDK on folder level and add ``cdk`` library
to targets in this folder manually::

  CDK_SETUP()  # Setup include path for CDK

  ADD_LIBRARY(foo ...)
  TARGET_LINK_LIBRARIES(foo cdk)

However, if any additional setups are required to correctly build a target
that uses CDK, macro :func:`ADD_CDK` will take care of this while just adding
``cdk`` library might not be enough (at the moment it is enough).

The include path for CDK is stored in ``CDK_INCLUDE_DIR`` variable. So an
alternative to :func:`CDK_SETUP` is this command::

  INCLUDE_DIRECTORIES(${CDK_INCLUDE_DIR})

but again, if in the future any additional setup is required, macro
:func:`CDK_SETUP` will take care of this.


Dependency on ngcommon
----------------------

CDK sources use common code of NG project, which is located in git repository
*myrepo:ngcommon*. If a project has a copy of this common code stored
somewhere, CDK can be configured to use this copy. This is done by setting
``WITH_NGCOMMON`` variable prior to including CDK sources in the project::

  SET(WITH_NGCOMMON "${PROJECT_SOURCE_DIR}/common")
  ADD_SUBDIRECTORY(cdk)

Above example assumes that common code is present in ``common/`` subfolder
of project's main folder.

An alternative is to use a copy of ngcommon sources included in CDK repository
as a submodule. If CDK sources are fetched from the git repository, then
commands ``git submodule init``, ``git submodule update`` will fetch the
common code into ``common/`` sub-folder of CDK's main folder and CDK will
use this copy by default (if ``WITH_NGCOMMON`` is not set).

.. note:: This means that if one feteches CDK sources without fetching
  submodules and does not set ``WITH_NGCOMMON``, then the copy of ngcommon
  sources will not be present in CDK folder and CDK build configuration
  will fail.


Other build configuration settings
----------------------------------

By default, when CDK is included as a sub-project, only main CDK targets
are configured. CDK documentation, unit tests and similar goodies are not
built to not clutter main project's target list. This default can be changed
with build configuration options:

:WITH_CDK_TESTS:
  Build CDK unit tests. This introduces additional dependency on gtest
  (see below).

:WITH_CDK_DOC:
  Build CDK documetation using Sphinx. 

:WITH_NGS_MOCKUP:
  Build NGS mockup server which can be used for testing client-side code.

:WITH_TESTS:
  If this option is set and ``WITH_CDK_TESTS`` option is undefined then
  CDK unit tests will be built. To disable CDK tests, set ``WITH_CDK_TESTS``
  to ``OFF``.

:WITH_DOC:
  If this option is set and ``WITH_CDK_DOC`` option is undefined then
  CDK documentation will be built. To disable building CDK docs, set
  ``WITH_CDK_DOC`` to ``OFF``.

:WITH_BOOST:
  Location of boost libraries to be used by CDK.

:WITH_PROTOBUF:
  Location of protobuf libraries to be used by CDK.

:WITH_GTEST:
  Location where gtest libraries were built. Used only if building of
  CDK unit tests was enabled. See ... for details how to setup gtest
  for use by CDK.

.. todo:: Options for building CDK docs.

Example code
============

.. todo:: Session API examples.

Client/server using CDK protocol API
------------------------------------

This example shows how to write simple client and server application that
communicate using the MySQL X protocol via CDK Protocol API.

Both server and client code use the following CDK headers, importing symbols
defined there into the root namespace for conveninece:

.. code-block:: cpp

  #include <mysql/cdk.h>
  #include <mysql/cdk/protocol/mysqlx.h>
  #include <iostream>

  using namespace cdk;                   // Generic CDK classes
  using namespace cdk::protocol::mysqlx; // MySQL X protocol classes
  using namespace std;


Client code
...........

In order to receive messages from the server, client application must define
message processors which implement one of the interfaces described in section
:ref:`msg_processors`. Our example client program will need a processor for
server replies after statement prepare request

.. code-block:: cpp

  class Reply_handler: public Stmt_processor
  {
    void prepare_ok()
    {
      cout <<"Statement was prepared" <<endl;
    }

    void execute_ok()
    {
      cout <<"Statement was executed" <<endl;
    }

    void error_msg(string msg)
    {
      cout <<"ERROR from server: " <<msg <<endl;
    }
  };

An instance of this processor will be passed to
:func:`Protocol::rcv_stmtReply` call to process server reply after
:func:`Protocol::snd_prepareStmt` (see :ref:`proto_sql_queries`).

But, before client can send any commands, it needs to authenticate with
the server. We will use :class:`Auth_handler` class for that purpose.

.. code-block:: cpp

  class Auth_handler: public Auth_processor
  {
    Protocol     &m_proto;
    const string  m_usr;
    const string  m_pwd;
    bool          m_accepted;

   public:

    Auth_handler(Protocol &proto, const string &user, const string &pwd)
      : m_proto(proto), m_usr(user), m_pwd(pwd),
        m_accepted(false)
    {}

    // Perform authentication handshake
    bool authenticate();

   private:

    // Auth_processor methods

    void authenticateOk(bytes data);
    void authenticateContinue(bytes data);
    void authenticateFail(bytes msg);
    void error_msg(string msg);
  };

This class inherits from :class:`Auth_processor` so that its instance can
be used to process server replies during authentication handshake (see
:ref:`proto_auth`).

.. code-block:: cpp

  void Auth_handler::authenticateOk(bytes data)
  {
    cout <<"Server accepted new session: "
         <<string(data.begin(), data.end())
         <<endl;
    m_accepted= true;
  }

  void Auth_handler::authenticateContinue(bytes data)
  {
    throw "Not implemented";
  }

  void Auth_handler::authenticateFail(bytes msg)
  {
    cout <<"Server rejected new session: "
         <<string(msg.begin(), msg.end()) <<endl;
  }

  void Auth_handler::error_msg(string msg)
  {
    cout <<"Server error during authentication: "
         <<msg <<endl;
  }

The authentication handshake is performed by
:func:`Auth_handler::authenticate` method which returns ``true`` if new
session was accepted on server.

.. code-block:: cpp

  bool Auth_handler::authenticate()
  {
    // Send initial message.

    Protocol::Op &snd=
      m_proto.snd_authenticateStart("test",
        bytes((byte*)m_usr.c_str(), m_usr.length()),
        bytes((byte*)m_pwd.c_str(), m_pwd.length())
      );

    // Wait for send operation to complete.

    snd.wait();

    // Receive server reply using itself as reply processor.

    Protocol::Op &rcv=
      m_proto.rcv_authenticateReply(*this);

    rcv.wait();

    return m_accepted;
  }

Note that protocol methods :func:`snd_XXX` and :func:`rcv_XXX` create
asynchronous operations which must be completed before we can continue
with next operation (see :ref:`proto_if` and :ref:`foundation_async`).
Here we simply wait for each operation to complete but in true asynchronous
setting, some sort of asynchronous loop can be used to monitor and drive all
existing asynchronous operations of an asynchronous application.

.. note:: Current version of CDK does not support fully asynchronous
  semantics. All asynchronous operations created by CDK block until full
  completion when their :func:`cont` method is called.


Having defined the required processors, we can write the main logic of
the client. First step is to create a connection to the server. This is
done using :class:`connection::TCPIP` provided by CDK (see :ref:`foundation_io`).
Method :func:`connect` establishes connection, throwing errors in case
of problems. Once connected, we can create :class:`Protocol` instance
over the connection, which will be used to send and receive protocol
messages.

.. code-block:: cpp

  connection::TCPIP conn("localhost", PORT);

  cout <<"Connectiog to port " <<PORT <<"..." <<endl;
  conn.connect();

  cout <<"Connected, authenticating with server" <<endl;

  Protocol proto(conn);

Next, we perform authentication handshake using our :class:`Auth_handler`
class:

.. code-block:: cpp

  Auth_handler ah(proto, "test_user", "test_pwd");

  if (!ah.authenticate())
  {
    cout <<"Session rejected, bailing out!" <<endl;
    return 1;
  }

After successful authentication, let us try to prepare some query. We use
:class:`Reply_handler` instance to handle server reply to ``prepareStmt``
request:

.. code-block:: cpp

  cout <<"Authenticated, preparing query" <<endl;

  Protocol::Op &snd_prepare= proto.snd_prepareStmt(1, "test query");
  snd_prepare.wait();

  Reply_handler rh;

  proto.rcv_stmtReply(rh).wait();

Finally, we can close session by sending ``close`` message:

.. code-block:: cpp

  cout <<"Closing session" <<endl;
  Protocol::Op &snd_close= proto.snd_close();
  snd_close.wait();


Putting it all together, the :func:`main` function of the client application
looks as follows. Note that we catch CDK errors there. Since CDK code is still
not complete, it sometimes throws simple strings as exceptions. It is good
idea to catch them as they give some hint on what went wrong.

.. code-block:: cpp

  int main()
  try {

    connection::TCPIP conn("localhost", PORT);

    cout <<"Connectiog to port " <<PORT <<"..." <<endl;
    conn.connect();

    cout <<"Connected, authenticating with server" <<endl;

    Protocol proto(conn);

    Auth_handler ah(proto, "test_user", "test_pwd");

    if (!ah.authenticate())
    {
      cout <<"Session rejected, bailing out!" <<endl;
      return 1;
    }

    cout <<"Authenticated, preparing query" <<endl;

    Protocol::Op &snd_prepare= proto.snd_prepareStmt(1, "test query");
    snd_prepare.wait();

    Reply_handler rh;

    proto.rcv_stmtReply(rh).wait();

    cout <<"Closing session" <<endl;
    Protocol::Op &snd_close= proto.snd_close();
    snd_close.wait();

    cout <<"Done!" <<endl;
  }
  catch (cdk::Error &e)
  {
    cout <<"CDK ERROR: " <<e <<endl;
  }
  catch (std::exception &e)
  {
    cout <<"std exception: " <<e.what() <<endl;
  }
  catch (const char *e)
  {
    cout <<"ERROR: " <<e <<endl;
  }


Server code
...........

To implement simple server we need two processors. One implements
:class:`Init_processor` interface to handle messages during initial handshake
and another which implements :class:`Cmd_processor` interface to process
client commands.

Actually, it is possible to implement both interfaces in a single class and
this is what we are going to do, implementing them in :class:`Session` class
which will be used to handle single client session.

.. code-block:: cpp

  class Session
    : public Init_processor
    , public Cmd_processor
  {
  public:

    Session(Socket::Connection &conn);
    void process_requests();

  private:

    Protocol_server m_proto;
    string m_auth;
    string m_user;
    string m_pass;
    bool   m_closed;

    // Init_processor

    void authenticateStart(const char *mech, bytes data, bytes response);
    void authenticateContinue(bytes data);

    // Cmd_processor

    void close();
  };

  void Session::authenticateStart(const char *mech, bytes data, bytes response)
  {
    m_auth= mech;
    m_user= string(data.begin(),data.end());
    m_pass= string(response.begin(),response.end());
  }

  void Session::authenticateContinue(bytes data)
  {
    throw "Not implemented";
  }

  void close()
  {
    cout <<"Client closed connection" <<endl;
    m_closed= true;
  }


Constructor of :class:`Session` performs authentication handshake, so
that when :class:`Session` instance is created it is ready to serve
client requests. Note that :class:`Session` has `m_proto` member which
is an instance of :class:`Protocol_server` class that will be used to
receive client messages and send server replies. This protocol instance is
constructed from an object representing active connection with client.

.. code-block:: cpp

  Session::Session(Socket::Connection &conn)
    : m_proto(conn), m_closed(false)
  {
    cout <<"Waiting for initial message ..." <<endl;
    m_proto.rcv_initMessage(*this).wait();

    cout <<"Authentication using method: " <<m_auth <<endl;
    cout <<"User: " <<m_user <<endl;
    cout <<"Password: " <<m_pass <<endl;

    // Send OK reply

    byte welcome[] = "Welcome!";
    m_proto.snd_authenticateOK(bytes(welcome, sizeof(welcome))).wait();
  }

Once new session has been established we can start processing client
commands. Currently server-side protocol support is very limited and the
only recognized command is a ``close`` command from client. We reply with
error if anything else was received.

.. code-block:: cpp

  void Session::process_requests()
  {
    while (!m_closed)
    {
      cout <<"Waiting for next command..." <<endl;
      m_proto.rcv_command(*this).wait();

      if (m_closed)
        break;

      cout <<"Unimplemented command" <<endl;
      m_proto.snd_Error(1, "Not implemented").wait();
    }
  }

The main logic of server application accepts single connection on
TCP/IP port, creates session and serves client requests until it closes
the connection. After serving a single client this simple server quits,
so no more connections can be made to it.

To handle incoming TCP/IP connections, CDK provides :class:`Socket` class.
Given :class:`Socket` instance, one can create :class:`Socket::Connection`
instance out of it - it represents an incoming connection on the socket.
Instance of :class:`Socket::Connection` class is an asynchronous operation
and one has to wait for it to complete, before connection can be used.

.. code-block:: cpp

  int main()
  try {

    Socket sock(PORT);

    cout <<"Waiting for connection on port " <<PORT <<" ..." <<endl;
    Socket::Connection conn(sock);
    conn.wait();

    cout <<"New connection, starting session ..." <<endl;
    Session sess(conn);

    cout <<"Session accepted, serving requests ..." <<endl;
    sess.process_requests();

    cout <<"Done!" <<endl;
  }
  catch (cdk::Error &e)
  {
    cout <<"CDK ERROR: " <<e <<endl;
  }
  catch (std::exception &e)
  {
    cout <<"std exception: " <<e.what() <<endl;
  }
  catch (const char *e)
  {
    cout <<"ERROR: " <<e <<endl;
  }



Reporting bugs
==============
Please report bugs in Jira project 
`MySQLng Connector/C <https://jira.oraclecorp.com/jira/browse/MYC>`_
as issues of type ``Defect`` with "Component" set to ``CDK``.

Since CDK is not released as an external product, there is no need
to create bugs entries in our bug databases. When we release connectors
that are implemented using CDK, bugs will be reported against these
connectors.

Building CDK stand-alone
========================

.. todo:: Add contents, including instructions on resolving gtest
  dependency.



