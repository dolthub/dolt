==============
 Protocol API
==============

Access to protocols that are supported by CDK. Currently this is mainly
the new MySQL protocol ``mysqlx``.

The new MySQL X protocol
========================
.. todo:: CRUD requests

Designs based on this protocol specification:
http://clustra.no.oracle.com/~jkneschk/mysql-ng/ng-protocol.html


Synopsis
--------
Protocol API for the new protocol is defined in ``cdk::protocol::mysqlx``
namespace

.. code-block:: cpp

  #include <mysql/cdk/protocol/mysqlx.h>

  using namespace cdk;
  using namespace cdk::protocol::mysqlx;

The API is implemented by :class:`Protocol` class. Instance of this class
needs a connection object to send the messages to and receive messages from
(and perhaps other arguments):

.. code-block:: cpp

  foundation::api::Connection &conn= ...;
  Protocol proto(conn, ...);

To perform authentication handshake, one needs a message processor object
that implements :class:`Auth_processor` interface. This object will receive
server replies during authentication handshake. Class :class:`Auth_handler`
implements :class:`Auth_processor` interface and handles the authentication
process interpreting messages sent by the server and generating appropriate
replies.

.. code-block:: cpp

  class Auth_handler : public Auth_processor
  {
    Protocol &m_proto;
    bool      m_accepted;

   public:

    Auth_handler(Protocol &proto, ...)
      : m_proto(proto), m_accepted(false)
    { ... }

    // Perform authentication handshake
    bool authenticate();

   private:

    // Auth_processor methods

    void auth_ok(bytes data);
    void auth_continue(bytes data);
    void auth_fail(string msg);
    ...

    // Continue handshake after initial message
    void continue_handshake();

    // Computed reply to last server challenge
    byte   m_reply[...];
    size_t m_reply_len;
  };

  Auth_handler ah(proto, ...);

  if (!ah.authenticate())
  {
    // failed to authenticate
  }

Authentication handshake starts with us sending ``authenticateStart``
message. This is done by :func:`Protocol::snd_authenticateStart` method
which returns reference to asynchronous operation of type
:class:`Protocol::Op` that sends the message. Message sending operation is
asynchronous but here we simply wait for it to complete before proceeding.
Method :func:`continue_hadshake` checks if connection was accepted in the
first round and if not it will continue the handshake generating replies
required by server.

.. code-block:: cpp

  void Auth_handler::authenticate()
  try
  {
    ...

    // Send authenticateStart() packet which initiates authentication
    // handshake

    bytes initial_auth_data(...);
    bytes first_response(...);
    m_proto.snd_AuthenticateStart("method",
                                  initial_auth_data,
                                  first_response).wait();

    continue_handshake();
    return true;
  }
  catch (Error &e)
  {
    // Error during handshake
    return false;
  }

If server accepts connection, it sends ``authenticateOK`` message and then
handler's :func:`authenticateOK` method is called.

.. code-block:: cpp

  void Auth_handler::auth_ok(bytes data)
  {
    m_accepted = true;
    ...
  }

Otherwise server sends ``authenticateContinue`` message expecting reply
from client. Handler`s :func:`authenticateContinue` method is called upon
receiving such message and it computes appropriate reply.

.. code-block:: cpp

  void Auth_handler::auth_continue(bytes data)
  {
     // compute reply and store it in m_reply
     ...
  }

The handshake is driven until completion by :func:`continue_handshake()`
method:

.. code-block:: cpp

  void Auth_handler::continue_handshake()
  {
    while (!m_accepted)
    {
      // get server's reply
      m_proto.rcv_AuthenticateReply(*this).wait();

      // if server accepted connection, stop here
      if (m_accepted)
        break;

      // send response computed in authenticateContinue() mehtod
      m_proto.snd_AuthenticateContinue(bytes(m_reply,m_reply_len)).wait();
    }
  }

When done using the session and before closing the connection, client sends
``close`` message. After this it should wait for ``Ok`` reply from server.

.. code-block:: cpp

  class Reply_handler: public Reply_processor
  {
    ...
  };

  Reply_handler rh(...);

  proto.snd_Close().wait();
  proto.rcv_Reply(rh).wait();


.. _proto_if:

Protocol interface
------------------

.. uml::

  !include class.cnf
  !include designs/events.if
  !include designs/async.if

  package "Protocol API" {

  interface Protocol {
    Send or receive protocol messages
    --
    snd_XXX(...) : Op
    rcv_XXX(...) : Op
  }

  interface "Protocol::Op" as op {
    Asynchronous message
    send/receive operation
  }

  op --|> Async_op

  }

Interface :class:`Protocol` defines methods for sending and receiving
protocol messages. These methods return :class:`Protocol::Op` object which
represents asynchronous send/receive operation which is completed when the
message has been sent or received, respectively. Data required to construct
outgoing message is passed to :func:`snd_XXX` calls as method's arguments.

When calling :func:`rcv_XXX` method to receive messages, a message
processor object is passed to that method. This processor object implements
callbacks which are used to pass data extracted from the message. When
calling :func:`rcv_XXX` usually more than one type of incoming messages is
expected. The processor has callbacks for handling any of the expected
messages as well as a callback for dealing with unexpected ones.

.. note:: Data passed to message processor from :func:`rcv_XXX` method is
  valid only when processor method to which this data was passed is
  called. After returning from processor method protocol object can
  discard the data. It means that processor needs to make copy of the
  data if it wants to use it later.

The send/receive operation created with :func:`Protocol::snd_XXX` or
:func:`Protocol::rcv_XXX` method is owned by the :class:`Protocol`
instance. Such operation must be completed before new operation of the same
type is created. For example, in such sequence::

  Protocol::Op &snd1 = proto.snd_XXX(...);
  ...
  Protocol::Op &snd2 = proto.snd_YYY(...);

the ``proto.snd_YYY(...)`` call will throw error if ``snd1`` is not
completed at the time of the call. However, in the following sequence::

  Protocol::Op &snd = proto.snd_XXX(...);
  ...
  Protocol::Op &rcv = proto.rcv_YYY(...);

it is OK to create new receive operation even if send operation was not
yet completed.

Creating new send/receive operation invalidates any previously created
operations of the same type. In a sequence::

  Protocol::Op &snd1 = proto.snd_XXX(...);
  snd1.wait();
  Protocol::Op &snd2 = proto.snd_YYY(...);

after second ``proto.snd_YYY(...)`` call it is no longer valid to use
object referred by ``snd1`` - an attempt to use it will result in undefined
behavior.

Reporting errors from asynchronous operations
.............................................

Asynchronous operation created by :func:`snd_XXX` or :func:`rcv_XXX` call
works until it does its job of sending or receiving messages. If errors are
detected during that process, they are signaled by throwing exceptions
from :class:`Async_op` interface methods such as :func:`cont` or
:func:`wait`.

Two scenarios are possible. In one scenario, an error can interrupt operation
before it has completed communication with the server. For example,
:func:`rcv_MetaData` operation (see below) throws error before reading all
the packets that describe result set metadata. In that case, after throwing
an error, operation's :func:`is_completed` method should return `false`.

Another case is when operation throws error after finishing communication
with server, so that protocol is in a state where exchange of messages can
continue as expected. For example, :func:`rcv_MetaData` throws an error after
reading all the metadata packets from server. In this case the asynchronous
operation also throws an error, but its method :func:`is_completed` should
return `true` after that.

.. note:: When possible, implementation should finish exchange with the
  server before throwing an error from asynchronous operation (so that it is
  completed after throwing an error).

A separate case is when a receive operation sees an error message from
server. In this case operation does not throw any errors. Instead, error is
reported to the processor and operation completes successfully. After sending
error message, server is ready to continue conversation with the client,
unless it is a fatal error in which case whole :class:`Protocol` instance is
invalidated and can no longer be used (calling its methods throws errors).


.. _msg_processors:

Message processors
------------------

.. uml::

  !include class.cnf

  package "Protocol API - Processors" {

  interface Processor_base {
    message_begin(short int type, bool &flag) : size_t
    message_data(bytes data) : size_t
    message_received(size_t bytes_read)
    message_end() : bool
  }

  interface Error_processor {
    error(int code, short int severity, sql_state_t sql_state, string msg)
    notice(unsigned int type, short int scope, bytes payload)
  }

  Error_processor --|> Processor_base

  interface Reply_processor {
    ok(string msg)
  }

  Reply_processor --|> Error_processor


Protocol :func:`rcv_XXX` methods accept a processor object which should
implement one of processor interfaces defined below. Processor interfaces
define callback methods which are called during processing of incoming
messages to pass data extracted from the messages to the processor.

Processor interfaces extend the generic :class:`Processor_base` one which
allows processor to see message boundaries, type of each message and access
raw message data. A receive operation also informs processor if received
message was expected or not.

Most processor interfaces extend :class:`Error_processor` one since in many
contexts server can send an ``Error`` message and processor should be
informed about that fact. Interface :class:`Reply_processor` is used in
situations where simple ``Ok``/``Error`` reply is expected from the server.


Sequence of processor callbacks
...............................

Processing a single received message starts with a call
to :func:`message_begin` and ends by a call to :func:`message_end`. Between
these two calls other processor callbacks are called which pass information
extracted from the message, for example :class:`Row_processor` methods to
inform about contents of a row in a result set.

It is not specified how implementation handles raw message data that is
received from server. It can store complete message in an internal buffer
before processing message contents, or it can process message as it reads it.
In either case, processor is informed about the fact that the whole message
payload has been received via :func:`message_received` callback. A call
to :func:`message_received` can happen before, during or after other calls to
processor methods, depending on how reading of message payload is handled by
receive operation. However, :func:`message_received` is always called
after :func:`message_begin` and before :func:`message_end`.

Processor can request that raw message payload is passed to it. The data is
passed via one or more :func:`message_data` calls which happen
after :func:`message_begin` and befroe :func:`message_received` calls.

If inital :func:`message_begin` call succeeds then receive operation
ensures that a matching call to :func:`message_end` will be made (so that
processor can, e.g., free allocated resources inside :func:`message_end`).
This is the case even if processing of message is interrupted for whatever
reason, such as exception thrown in processor callback or error reported by
server. However, if initial call to :func:`message_begin` throws exception
then it means that processor could not prepare for processing this message
and there is no corresponding :func:`message_end` call.

In either case, if processor callback throws an exception this interrupts
the receive operation which should cancel itself and report error informing
about the fact that processing was interrupted because of error in the
processor object. As noted above, if possible the operation should finish
reading all expected messages from the server before reporting the error.

If receive operation reads several messages from the server, message
processor can stop this operation after any message. This is done by
returning `false` from :func:`message_end` callback. If receive operation was
stopped in this way, another receive operation must be created to resume
processing of the pending messages from the server. Details depend on
particular :func:`rcv_XXX` operation that is involved.

.. note:: Processor callbacks can corrupt processing of incoming messages
  if they misbehave. They should not perform time consuming operations to
  ensure that internal logic used to process messages runs smoothly.


Base processor callbacks
------------------------
These callbacks are implemented by any message processor.

.. function:: size_t Processor_base::message_begin(short int type, \
                                                   bool &flag)

  Called when message header is received. The type of the message stored
  in the header is passed to the method. Argument ``flag`` informs if
  the message was expected (``true``) or not (``false``).

  On return from this method, processor can change value of the flag to
  change the way given message is processed (see below).

  If :func:`message_begin` returns non-zero value then processor
  requests that message's raw data is sent to it via
  :func:`message_data` calls (see below). The value indicates maximum
  amount of data that should be sent in the first :func:`message_data`
  call.

  Processor can request raw message bytes regardless of whether
  message was expected or not and what is the exit value of the flag.


.. function:: size_t Processor_base::message_data(bytes data)

  This method is called if processor requested raw message data. It
  passes next fragment of the data which is never larger than the
  maximum allowed size returned by :func:`message_begin` or previous
  :func:`message_data` call. If this method returns zero then no more
  message data will be sent. End of message data is indicated by
  :func:`message_received` call.

.. function:: void Processor_base::message_received(size_t bytes_read)

  Called when whole message payload has been read. The number of bytes of
  the payload that have been read is passed as ``bytes_read``.

.. function:: bool Processor_base::message_end()

  This method is called at the end of processing given message, after all
  other processor callbacks. It can be used by processor to free any
  resources allocated to process the message. The value returned by this
  method indicates whether further messages (if any) should be read and
  processed by the receive operation. This is the case if method returns
  `true`. Otherwise, if method returns `false`, the receive operation is
  stopped at this point. To resume reading and processing remaining messages
  (if any) another receive operation must be created.

  A call to :func:`message_end` always matches the
  initial :func:`message_begin` call (if successful), even if processor
  requested skipping given message or processing of the message was
  interrupted for whatever reason. Only if initial :func:`message_begin` call
  has thrown error there is no corresponding :func:`message_end` call at the
  end.


Changing the way given message is handled
.........................................

Normally, unexpected message interrupts the receive operation which cancels
itself and reports "unexpected message" error. For expected messages, its
contents is parsed and passed to the processor via callbacks.

This default behavior can be changed by processor by changing value of
``flag`` parameter on exit from :func:`message_begin` callback. Receive
operation checks the value of the flag after the call
to :func:`message_begin` and decides how to process the message based on it.

For unexpected messages, flag value tells if this message should be
ignored (flag is ``true``) or not (flag is ``false``). If unknown message
is ignored then receive operation continues as if this message was not
seen. If it is not ignored then receive operation gets interrupted and
throws error.

For expected messages, flag value tells if message should be further
processed (flag is ``true``) or not (flag is ``false``). If further
processing is disabled, processor callbacks that pass information extracted
from the message are not called. But even in this case, base processor
callbacks such as :func:`message_received` and :func:`message_end` are still
called.

  +----------+-----------------------------------------------+
  |          | flag value when calling :func:`message_begin` |
  |          +-----------------------+-----------------------+
  | on exit  | true                  | false                 |
  +==========+=======================+=======================+
  | true     | message was expected, | unexpected message,   |
  |          | parse it              | ignore it             |
  +----------+-----------------------+-----------------------+
  | false    | message was expected, | unexpected message,   |
  |          | do not parse          | do not ignore         |
  +----------+-----------------------+-----------------------+

Note that if processor does not change the flag, then default handling is
requested. For unexpected message the value of the flag when
calling :func:`message_begin` is ``false``. If not changed, it tells receive
operation to not ingore this message and thus it will throw error. For
expected message value of the flag is ``true`` when
calling :func:`message_begin`. If not changed, message will be parsed and its
contents passed to the processor.

.. _proto_auth:

Establishing and terminating a session
--------------------------------------
.. todo:: Capability negotiation

New session is established via authentication handshake which is initiated
by client sending ``authenticateStart`` message.

.. function::  Op snd_AuthenticateStart(const char* mechanism, \
                                        bytes data, bytes response)
               Op snd_AuthenticateContinue(bytes data)

  Send messages used in authentication handshake.

.. note:: Data passed to :func:`snd_AuthenticateStart`
  or :func:`snd_AuthenticateContinue` must remain valid for the duration of
  the send operation.

.. function:: Op rcv_AuthenticateReply(Auth_processor)

  Wait for server's reply during authentication handshake. Processor's
  methods :func:`auth_ok`, :func:`auth_continue` or :func:`auth_fail` are
  called when server sends the corresponding message. Processor will be also
  informed if server sends ``Error`` packet or some other, unexpected
  packet.

.. uml::

  !include class.cnf

  interface Auth_processor {
    auth_ok(bytes data);
    auth_continue(bytes data);
    auth_fail(string msg);
  }

  Auth_processor -|> Error_processor
  Error_processor -|> Processor_base
  interface Error_processor
  interface Processor_base


Resetting current session.

.. function:: Op snd_SessionReset()

  Request resetting of the current session. Server replies with ``Ok``
  message which should be processed
  using :func:`rcv_Reply` method.


Terminating session and/or connection:

.. function:: Op snd_SessionClose()
              Op snd_Close()

  First method requests closing of the current session. Second one informs
  about the fact that client will close the connection. In both cases server
  replies with ``Ok`` message which should be processed
  using :func:`rcv_Reply` method.


Processing simple ``Ok`` or ``Error`` reply from server.

.. function:: Op rcv_Reply(Reply_processor)


.. _proto_sql_queries:

Executing SQL queries
---------------------

To execute SQL statement one sends ``StmtExecute`` request.

.. function:: Op snd_StmtExecute(string stmt)

  Send SQL statement for execution.

.. versionadded:: post-ms1

.. function:: Op snd_StmtExecute(string stmt, Row_source params)

  Send SQL statement containing parameter placeholder for execution with
  given parameter values.


Server reply to `StmtExecute` can consists of zero, one or more result sets.
It should be processed using :func:`rcv_MetaData`, :func:`rcv_Rows` and
:func:`rcv_StmtReply` methods as described below.


.. versionadded:: post-oct

Another way of executing an SQL statement is by preparing it first.

.. function:: Op snd_PrepareStmt(stmt_id_t id, string stmt)

  Prepare given SQL statement and assign it given numeric id.


After preparing statement one can send a request to execute it. This opens a
cursor for accessing result sets produced by the statement. The numeric id
for the cursor is given when requesting execution of the statement.

.. function:: Op snd_PreparedStmtExecute(stmt_id_t id, cursor_id_t cid)

  Request execution of prepared statement with given `id`. The cursor
  opened for the results will have id given by `cid`.

.. function:: Op snd_PreparedStmtExecute(stmt_id_t stmt, \
                                         cursor_id_t cur, \
                                         Row_source params)

  Execute prepared statement with parameters, providing values for these
  parameters via row source object.

See :ref:`Query_results` for a description of how to fetch and process rows
and meta-data information form a cursor.

After fetching results from a cursor opened by statement execute request,
that cursor should be closed. Similar, a statement which is no longer needed
should be closed.

.. function:: Op snd_CursorClose(cursor_id_t cur)
              Op snd_PreparedStmtClose(stmt_id_t stmt)

  Send request to close given cursor or statement.


After any of :func:`snd_PrepareStmt`, :func:`snd_PreparedStmtExecute`,
:func:`snd_CursorClose` or :func:`snd_PreparedStmtClose` requests one
should call :func:`rcv_StmtReply` to process server's reply with
a :class:`Stmt_processor` (see below).


.. todo::
  - Polling cursor state
  - Handling asynchronous notifications from server
  - Providing values for prepared statement parameters


.. _Query_results:

Examining query results
-----------------------

SQL statement executed in the server can produce zero, one or more result
sets consisting of rows and meta-data information. Result sets are processed
using :func:`rcv_Result` and the following hierarchy of processors:

.. uml::

  !include class.cnf

  interface Row_processor {
    Process rows of a result set
    sent by server.
    --
    row_begin(row_count_t row) : bool
    row_end(row_count_t row)
    ..
    col_null(col_count_t pos)
    ..
    col_begin(col_count_t pos) : size_t
    col_data(col_count_t pos, bytes data) : size_t
    col_end(col_count_t pos, size_t data_len)
    ..
    done(bool eod, bool more)
  }

  Row_processor --|> Error_processor
  interface Error_processor
  Error_processor --|> Processor_base
  interface Processor_base

  interface Mdata_processor {
    Process result set meta-data
    --
    col_count(col_count_t count)
    col_type(col_count_t pos, int type)
    col_name(col_count_t pos, string name, string orginial)
    col_table(col_count_t pos, string table, string original)
    col_schema(col_count_t pos, string schema, string catalog)
    col_charset(col_count_t pos, string cs)
    col_decimals(col_count_t pos, int decimals)
  }

  Mdata_processor --|> Error_processor

  interface Stmt_processor {
    prepare_ok()
    execute_ok()
    rows_affected(row_count_t)
    last_insert_id(row_count_t)
    cursor_close_ok()
    stmt_close_ok()
  }

  Stmt_processor -|> Error_processor


In case of direct execution with :func:`snd_StmtExecute` server replies with
meta-data and rows for all result sets produced by the statement. Client
should process this information with the following sequence of calls.

1. :func:`rcv_MetaData`,
2. if there are rows in the result set, then :func:`rcv_Rows`,
3. if there are more result sets then repeat from 1,
4. :func:`rcv_StmtReply`.

Processing server relpy starts with reading meta-data for the next resultset
(if any).

.. function:: Op rcv_MetaData(Mdata_processor mdp)

  Create operation which receives result set meta-data (if any) and passes
  information to given metadata processor as follows:

  .. uml::

    participant "rcv_MetaData()" as rcv
    participant "Mdata_processor" as prc

    loop for each column
      rcv -> prc: col_type(...)
      rcv -> prc: col_name(...)
      ...
    end
    rcv -> prc: col_count(N)

  If there are no rows in the result set then the only callback made
  is :func:`col_count(0)<>`. In this case client should not call
  :func:`rcv_Rows` for this result set, but continue 
  with :func:`rcv_StmtReply`.

If there are rows in the result set they should be processed
with :func:`rcv_Rows`

.. function:: Op rcv_Rows(Row_processor rp)

  Create operation which reads rows of a result set and passes them to given
  row processor (see :ref:`rset_rows`). Row processor is informed if all rows
  in the current result set have been processed and whether there are more
  result sets that follow.

If there are no more result sets in the reply, client should finish
processing it with a call to :func:`rcv_StmtReply`:

.. function:: rcv_StmtReply(Stmt_processor sp)

  Create operation which finalizes processing of server reply after a
  command, invoking :class:`Stmt_processor` callbacks as follows:

  .. uml::

    participant "rcv_StmtReply()" as rcv
    participant "Stmt_processor" as prc

    opt rows were affected
    rcv -> prc: rows_affected(N)
    end
    opt last insert id modified
    rcv -> prc: last_insert_id(ID)
    end
    rcv -> prc: execute_ok()


.. note:: In all above diagrams, interaction with the processor can be
  interrupted at any moment by reporting error via :func:`error` callback.


After :func:`snd_StmtExecute` call client is responsible for processing
complete server reply by performing :func:`rcv_MetaData`, :func:`rcv_Rows`
and :func:`rcv_StmtReply` calls as described above until all result sets are
processed or error is reported.

Asynchronous receive operation created by :func:`rcv_Rows` or
:func:`rcv_MetaData` should read all requested rows or all column
descriptions sent by the server. Even if these operations are interrupted by
error thrown from processor callback, they should discard all remaining
messages that server is supposed to send (so that, after reporting error,
they are completed).

Note that message processor can stop processing a result set at any
moment by returning `false` from :func:`message_end` callback. In that case
processing should be resumed with another call to :func:`rcv_MetaData`,
:func:`rcv_Rows` or :func:`rcvStmtReply` depending on the moment where processing
of server reply was stopped.


.. _rset_rows:

Processing result set rows
--------------------------

The :func:`rcv_Rows` operation calls row processor callbacks as follows:

  .. uml::

    participant "rcv_Rows()" as rcv
    participant "Row_processor" as prc

    loop for each row
      rcv -> prc: row_begin(...)
      loop for each column
        alt null column
          rcv -> prc: col_null()
        else column with data
          rcv -> prc: col_begin()
          loop while processor requests data
            rcv -> prc: col_data()
          end
          rcv -> prc: col_end()
        end
      end
      rcv -> prc: row_end(...)
    end
    alt no more result sets
    rcv -> prc: done(true, false)
    else another result set will follow
    rcv -> prc: done(true, true)
    end


.. function:: bool Row_processor::row_begin(row_count_t row)
              bool Row_processor::row_end(row_count_t row)

  These methods are called before and after sending fields of next row,
  respectively. The row number passed as argument is counting rows received
  in a single call to :func:`rcv_Rows` or :func:`rcv_CursorRows`
  starting from 0.

  If :func:`row_begin` returns `false` then this row is
  skipped and no more processor methods are called for this row except the
  finall :func:`row_end`.

  If :func:`row_end` returns `false` then no more rows are processed -- the
  asynchronous operation is interrupted and another one can be created to
  process the remaining rows.

.. function:: void Row_processor::done(bool eod, bool more)

  This method is called when all requested rows have been processed or there
  are no more rows in the current result set. In latter case parameter
  `eod` is true. If `eod` is true then parameter `more` tells if server
  reply contains more result sets. If this is the case (`more` is `true`)
  then next result set should be processed with another call to
  :func:`rcv_MetaData`.


After :func:`row_begin` call, for each field in the row one
of :func:`col_XXX` methods is called, depending on the type of the
corresponding column and whether filed has value or is null.
The :func:`col_XXX` methods accept 0-based column position as the first
argument.

.. function:: void Row_processor::col_null(col_count_t pos)

  This method is called if the corresponding field value is null. No other
  methods are called for this field.

.. function:: size_t Row_processor::col_begin(col_count_t pos)

  This method is called for fields containing data (not null). The raw
  data of the field will be sent via separate :func:`col_data` calls until
  the final :func:`col_end` call. Returned value indicates maximum amount of
  data to be sent in first :func:`col_data` call. If returned value is 0
  then contents of this field will not be sent and a call to :func:`col_end`
  will follow. The meaning of column data is given by meta-data information
  (column type).

.. function:: size_t Row_processor::col_data(col_conunt_t pos, bytes data)

  This method is called one or more times to pass raw bytes of a row field.
  The amount of data passed in a single call is not bigger than the limit
  returned from :func:`col_begin` or previous :func:`col_data` call. If this
  method returns 0 then remaining data (if any) is discarded and a
  :func:`col_end` call follows.

.. function:: void Row_processor::col_end(col_count_t pos, size_t data_len)

  This method is called after :func:`col_begin` when all requested field
  bytes have been passed to the processor or there are no more bytes to
  pass. The `data_len` argument holds the total length of the field.


:class:`Row_processor` callbacks are called while receive operation is
reading :class:`Row` messages from the server. For each message operation
calls :func:`message_begin` (before :func:`row_begin`) and
:func:`message_end` (after :func:`row_end` and :func:`done` if present).

Also, somewhere in the sequence of callbacks, a :func:`message_received`
callback is made. This happens when complete message payload has been
received from the connection. In the current implementation this will happen
before :func:`row_begin` call but code using protocol API should not rely on
this. If processor requests raw message data in :func:`message_begin` call
then there will be additional :func:`message_data` calls
before :func:`message_received` one.

On the return from :func:`message_begin` processor can request to skip
additional callbacks for this message. In this case only requested
:class:`Processor_base` callbacks are called (including
:func:`message_end`) and the final :func:`row_end` and :func:`done`.

If :func:`row_end` callback returns `false` then processing of the row
sequence is interrupted and the asynchronous operation created with
:func:`rcv_Rows` ends at this point. To process remaining rows,
another :func:`rcv_Rows` operation should be created. This must be done until
all requested rows are processed which is reported by :func:`done` callback.
After this point it is not valid to call :func:`rcv_Rows` again.

If row processor throws error from any of its callbacks, then
:func:`rcv_Rows` operation will also report error, but first it will read and
discard all remaining rows from the sequence.


Sending CRUD requests
---------------------

The following :class:`Protocol` methods are used to send CRUD commands to the
server.  Datatypes used to specify request parameters are described below.
Optional parameters such as `expr` are specified as pointers -- passing
`NULL` value means that this parameter will be not present in the request.

.. function::   Op& snd_Find(api::Db_obj coll, Data_model dm, \
                             api::Expression *expr, api::Projection *proj, \
                             api::Grouping *group, api::Ordering *order, \
                             api::Limit *limit)

  Create operation that sends Find request with the following parameters:

    :`coll`:  the collection to fetch data from;
    :`dm`:    data model - either `TABLE` or `DOCUMENT` or `DEFAULT`;
    :`expr`:  selection criteria;
    :`proj`:  projection to apply to results;
    :`group`: grouping specification;
    :`order`: ordering specificiation;
    :`limit`: limit the result.


.. function::  Op& snd_Insert(api::Db_obj coll, Data_model dm, \
                              api::Columns *dest, Row_source rs)

  Create operation that sends Insert request with the following parameters:

    :`coll`:  the collection to insert into;
    :`dm`:    data model - either `TABLE` or `DOCUMENT` or `DEFAULT`;
    :`dest`:  destination columns for inserted values;
    :`rs`:    :class:`Row_source` object which provides data to be inserted;

.. function::  Op& snd_Delete(api::Db_obj coll, Data_model dm, \
                              api::Expression *expr, api::Ordering *order, \
                              api::Limit *limit)

  Create operation that sends Delete request with the following parameters:

    :`coll`:  the collection to delete from;
    :`dm`:    data model - either `TABLE` or `DOCUMENT` or `DEFAULT`;
    :`expr`:  criteria that selects documents/rows to delete;
    :`order`: ordering specificiation;
    :`limit`: limit the number of deleted documents/rows.

.. todo:: Update operation

Server reply after one of these requests should be processed as described in :ref:`Query_results`.

The following interfaces are used to specify CRUD request parameters.

.. uml::

  !include class.cnf

  interface "api::Db_obj" {
    get_name() : string
    get_schema() : string*
  }

  interface "api::Expression::Processor" {
    str(const char *charset, bytes value)
    opaque(int fmt, bytes value)
    var(string name)
    id(string name, Db_obj *table)
    id(string name, Db_obj *table, Doc_path path)
    id(Doc_path path)
    op(const char *name, Expr_list args)
    call(Db_obj func, Expr_list args)
    placeholder()
    placeholder(string name)
    placeholder(unsigned pos)
  }

  interface "api::Expression" {
    process(Processor)
  }

  interface Expr_list {
    count() : unsigned
    get_expr(unsigned pos) : api::Expr
  }

  interface Doc_path {
    length() : unsigned
    get_type(unsigned pos)  : Type
    get_name(unsigned pos)  : string
    get_index(unsigned pos) : unsigned
  }

  enum "Doc_path::Type" {
    MEMBER,
    INDEX,
    ASTERISK,
    DOUBLE_ASTERISK,
    INDEX_ASTERISK
  }

  interface "api::Projection" {
    get_alias(unsigned pos) : string*
  }

  "api::Projection" --> Expr_list

  interface "api::Grouping" {
    get_filter() : api::Expr
  }

  "api::Grouping" --> Expr_list

  interface "api::Ordering" {
    get_direction(unisgned pos) : Direction
  }

  "api::Ordering" --> Expr_list

  enum "Ordering::Direction" {
    ASCENDING,
    DESCENDING
  }

  interface "api::Columns" {
    count() : col_count_t
    get_name(col_count_t pos) : string
    get_path(col_count_t pos) : Doc_path*
  }

  interface "api::Limit" {
    get_count() : row_count_t
    get_offset() : row_count_t*
  }


Specifying collections
......................

Collection on which given CRUD request should operate is specified by its name
and optional schema name. Method :func:`api::Db_obj::get_name` returns the
name and :func:`api::Db_obj::get_schema` returns schema name or NULL if
collection has no schema name.

Specifying expressions
......................

Interface :class:`api::Expression` is used to describe expressions that are
paremeters to some CRUD requests. An object passed to :func:`snd_XXX` function
as :class:`api::Exprssion` parameter should implement method :func:`process`
which is used to visit the syntax tree of the expression using a visitor of
type :class:`api::Expression::Processor`. Such visitor is used by Protocol
implementation to build message corresponding to the expression.


If `e` is an expression object and `ep` is expression processor implementing
:class:`Expression::Processor` interface then a call to `e.process(ep)` will
call appropriate methods of `ep` to describe expression represented
by object `e`.

For example, if object `e` represents expression ```foo` > 7`` then
`e.process(ep)` will call ``ep.op(">", args)`` where `args` is a list of two
expressions describing arguments of the operator. Inside :func:`op` callback,
processor `ep` can look into these arguments by invoking::

  args.get_expr(0).process(*this);
  args.get_expr(1).process(*this);

This way arbitrary expression can be processed in any way by defining appropriate
expression processor. Protocol API implementation will use it to build messages
that describe the expression.

.. note:: Protocol API does not specify how to implement
  :class:`api::Expression`. User of the API can implement it in whatever way he
  wants as long as upon call to :func:`process` processor methods are correctly
  called to describe the intended expression.

Projections
...........

Projection used by Find request is just a list of expressions with optional
aliases. Class :class:`api::Projection` derives from :class:`Expr_list` and adds
method :func:`Projection::get_alias` which returns alias of an expression or
`NULL` if expression has no alias.


Grouping specification
......................

Such specification consists of a list of expressions whose values are used to
aggregate the results (corresponds to SQL ``GROUP BY`` clause) and optional
expression to filter the groups after aggregation (corresponds to SQL ``HAVING``
clause). Class :class:`api::Grouping` derives from :class:`Expr_list` and adds
method :func:`Grouping::get_filter` which returns filter expression.


Ordering and Limit specifications
.................................

Ordering specification consists of a list of expressions whose values are used to
sort the results plus, for each expressin an information whether sorting is
ascending or descending. Class :class:`api::Ordering` derives from
:class:`Expr_list` and adds method :func:`Ordering::get_direction` which returns
direction constant: `ASCENDING` or `DESCENDING`.

Limit specification specifies the number of rows/documents to fetch and optional
offset. Function :func:`Limit::get_count` returns the number of rows/documents
and :func:`Limit::get_offset` returns the offset or NULL if none is specified.
