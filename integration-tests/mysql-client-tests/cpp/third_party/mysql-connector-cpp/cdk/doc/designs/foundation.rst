CDK Foundation classes
======================

.. note:: Interface definitions below should be treated as high-level general
  descriptions. Full details such as exact types and type modifiers, whether
  arguments are passed by value or reference etc. will be determined by
  implementation.


Basic types and functions
-------------------------
Apart from standard C++ types, CDK uses the following basic types.

:byte:   For storing single byte/octet - equivalent to :class:`unsigned char`.
:bytes:  Represents a continuous area in memory (see :ref:`buffers`).
:option_t: Three valued type with values `YES`, `NO` and `UNKNOWN`.
:time_t: Identifies an instance in time with millisecond granularity.
:string: Wide strings that can store arbitrary characters (specializes
         :class:`std::wstring`).
:char_t: For storing single characters - equivallent to :class:`wchar_t`.

.. function:: time_t get_time()

  Returns timestamp for the moment when this function was called.

.. note:: :class:`time_t` is a numeric type and one can get timepoints in
  future/past by doing arithmetic on :class:`time_t` values. For example:
  ``get_time() + 1000`` will be a time point 1s from now.


Serialization for scalar types
------------------------------
Constants `Type::XXX` define value types for which serialization routines
are defined by CDK Foundation. For each type `Type::XXX` an encoder/decoder for
that type is an object of class :class:`Codec\<Type::XXX>`.

.. class:: Codec<Type::STRING>

  Encode strings using UTF-8 encoding.

.. todo:: Numeric conversions

Any :class:`Codec\<T>` class defines methods:

.. function:: size_t Codec<T>::from_bytes(bytes raw, X &val)

  Convert value of type :class:`T` represented by raw bytes to value of
  native C++ type :class:`X` and store it in `val`. Returns number of bytes
  from `raw` buffer used to decode the value. Specializations of this
  method template are defined only  for types :class:`X` for which conversion
  exists.

.. function:: size_t Codec<T>::to_bytes(X val, bytes raw)

  Convert value of native C++ type :class:`X` to value of type :class:`T`
  and write raw bytes representation of this value into buffer `raw`. Returns
  number of bytes written to the output buffer. The buffer must be big enough
  to hold the representation - if it is not then this method throws error.


Events
------

.. versionadded:: post-ms1
  No event notification infrastructure for the first milestone.

Some objects produce asynchronous notifications about events that happen during
the life time of the object. Possible events are reported to a listener object
if one was registered with the event source. This listener object must implement
appropriate Listener interface with methods for all events that can be reported
by given event source.

.. uml::

  !include class.cnf

  package "CDK Foundation - Events" {

  !include designs/events.if!0
  !include designs/events.if!1

  }

Interface Event_source<Listener>
................................
Event source object for a given ``Listener`` type must implement the following methods.

.. function:: void Event_source<Listener>::set_listener(Listener)

  Register a listener object that will be notified about events by calling
  methods from ``Listener`` interface. If a listener was registered previously,
  it will be unregistered first.

.. function:: void Event_source<Listener>::unset_listener()

  Un-register a listener, if one was registered before.


Interface Listener<Source>
..........................
Any listener object for event source of type ``Source`` should implement at least
the following methods.

.. function:: void Listener<Source>::registered(Source)

  Called when listener is registered with event source, passing this event source
  as argument.

.. function:: void Listener<Source>::unregistered(Source)

  Called when listener is un-registered from event source.

.. function:: void Listener<Source>::error(Source, Error)

  Called when error happens. We assume that any event source can generate
  error events. Note: source object is passed to allow single listener handle
  several sources.

Apart from above methods, a concrete listener interface should define methods
that are called to report events to the listener.


.. _foundation_async:

Asynchronous operations
-----------------------

Asynchronous object represents an on-going or completed asynchronous operation.
Several such operations can execute at the same time. When completed,
asynchronous operation can return a result of some type T.

.. versionadded:: post-ms1
  In the first milestone there is no ``Async_op_listener`` and ``Async_op``
  does not have to extend ``Event_source``.

.. uml::

  !include class.cnf
  !include designs/events.if!0
  !include designs/events.if!1

  package "CDK Foundation - Async" {

  !include designs/async.if!2
  !include designs/async.if!3
  !include designs/async.if!0
  !include designs/async.if!1

  }

Interface Async_op<T>
.....................

.. function:: bool Async_op<T>::is_completed()

  Tells if operation is completed. This operation should be non-blocking.

.. function:: bool Async_op<T>::cont()

  Continue the operation until it blocks waiting for next event. Returns true
  if operation has completed during this call or if it was already completed (in
  which case :func:`continue` does nothing).

.. function:: void Async_op<T>::wait()

  Waits for the operation to complete. If operation is already completed then
  does nothing.

.. function:: void Async_op<T>::wait(time_t deadline)

  Waits until operation is completed or given ``deadline`` has passed. Wait
  does not have to end exactly at the given deadline - it is only an indication
  that after ``deadline`` :func:`wait` should end at earliest possibility.

.. function:: void Async_op<T>::cancel()

  Cancel an on-going operation. If operation is completed then does nothing.

.. function:: T Async_op<T>::get_result()

  Get result of completed operation. If operation is not completed, waits for
  it to complete before returning the result.

.. function:: Event_info* Async_op<T>::waits_for()

  If operation is blocked waiting for some event to happen (such as incoming
  data) this method returns information about the event. Otherwise (operation not
  waiting for anything) it returns NULL.


Assuming that ``x`` is an asynchronous object, application can complete the
operation represented by ``x`` either by calling ``x.wait()`` (in which case it will
block waiting for the operation to complete) or using an active poll loop::

  while (!x.cont())
  {
    // possibly do other things
  }

Another possibility is to integrate the operation with an existing asynchronous
event loop such as one provided by ``boost::asio``.


Event information
.................

The event info object returned by :func:`Async_op<T>::waits_for` implements method
:func:`Event_info::type` which informs about type of the event for which given asynchronous
operation is waiting. If no additional information about the event is available then
the returned event type is ``OTHER``. Otherwise the event info object provides additional
information about the event via interface extending ``Event_info`` one and appropriate for
given event type.

Events of type ``ASYNC_OP`` correspond to completing another asynchronous operation
for which the given one is waiting. The event info object for ``ASYNC_OP`` events
implements ``Async_op_event`` interface which defines method :func:`async_op`.
This method returns the asynchronous operation that is waited for.


Integrating with third-party async event loops
..............................................

Instead of calling ``x.cont()`` in active poll loop, application can arrange for it
to be called from an existing asynchronous event loop only when an event for
which the operation is waiting happens.

This assumes that event returned by ``x.waits_for()`` is detectable by the
asynchronous framework in use. For example, assume that asynchronous object is
waiting for input on a socket. The event info object returned by
``x.waits_for()`` should give information about the socket on which operation is
waiting. Application should arrange for getting notified when input data is
available on the socket.  How this is done depends on the asynchronous framework
in use - assume that application registers a callback that gets called when
input data is available (this is how it can be done with ``boost::asio``). The
callback should then call ``x.cont()`` to allow the operation to progress. After
this, if operation is not completed, application can call ``x.waits_for()`` to see
what is the next event the operation is waiting for and again arrange for
detection of the event and calling ``x.cont()`` when it happens.


.. _foundation_io:

Stream I/O
----------

.. todo::
  - Interface to use stream's internal buffers
  - Stream listeners
  - Pushing bytes back to input stream

In CDK stream-oriented I/O is performed as follows::

  Conn conn(...);
  conn.connect();

  buffers bufs(...);
  Conn::Read_op rd(conn, bufs);

  while (!rd.cont())
  {
    // do other things
  }

  size_t bytes_read= rd.get_result();

In above example ``Conn`` is a class that satisfies connection class requirements
specified below. Part of these requirements is that the class defines type
``Conn::Read_op`` which is an asynchronous operation that fills given buffers with
data from connection's input stream.

It is also possible to specify deadline for filling the buffers::

  Conn conn(...);
  conn.connect();

  buffers bufs(...);
  Conn::Read_op rd(is, bufs, get_time() + 1000);

Third argument of ``Conn::Read_op`` constructor specifies deadline for filling
the buffer. In above example read operation will report error if buffers could not be
filled within 1 second from the time when operation was created.

CDK provides the following connection classes which are defined in ``cdk::foundation::connection``
namespace

:TCPIP:
  Connection to and endpoint over TCP/IP protocol. Endpoint is identified by IP address
  and port or service name.


.. versionadded:: post-ms1
  In the first milestone there are no stream listeners and stream interfaces
  do not extend ``Event_source`` one.

.. uml::
  :width: 100%

  !include class.cnf
  !include designs/events.if!0
  !include designs/events.if!1

  package "CDK Foundation - I/O API" {

  !include designs/stream.if

  }

  package "CDK Foundation - Connection Classes" {

  class "connection::TCPIP" {
    Connection over TCP/IP protocol
    .. Constructors ..
    TCPIP(unsigned short port)
    TCPIP(string host, string service)
    TCPIP(string host, unsigned short port)
  }

  "connection::TCPIP" --|> Connection

  }


Read/Write operation requirements
.................................
Class ``X`` implements read operation for stream class ``S`` if it is an
asynchronous operation that returns result of type ``size_t`` and it defines the
following constructors:

.. function:: X(S s, buffers bufs)
              X(S s, buffers bufs, time_t deadline)

  Creates read operation that fills given buffers with data from stream ``s``.
  This operation completes successfully only when all given buffers are
  filled with data. If deadline is given, operation will be successful only if
  all buffers were filled before this deadline.

Class ``X`` implements read-some operation if it meets the same requirements as
above but the semantics of operation created with the constructors is different:
operation does not have to fill the buffers completely, but it can end
successfully if at least one byte has been read (before the deadline).

Requirements for write and write-some operations are analogous, but these operations
write bytes from given buffers to the stream. Write operation succeeds only if all bytes
have been written, wite-some if at least one byte has been written.

The result returned by read operation when it is completed is the number of bytes
placed in the buffer. For write operation the result is the number of bytes that
has been sent to the output stream.

Asynchronous I/O operation can wait for events of type ``SOCKET_RD`` or ``SOCKET_WR``
(see below).

Errors that can be reported by read/write operations:

:Error_eos:
  Read operation could not be created/completed because stream has ended.

:Error_timeout:
  Operation could not complete before deadline specified when it was created.

:Error_no_connection:
  Attempt to create i/o operation for a connection object that was not yet connected
  or for which connection attempt has failed (see below).

.. note::
    Asynchronous operations report errors to a listener, if one is registered,
    and also by throwing errors in calls of methods such as :func:`wait` or :func:`cont`.

.. note::
    Deadline specified for an i/o operation is independent and different from deadline that
    can be specified for :func:`wait` mehod. If operation has deadline t1 and one calls
    ``op.wait(t2)`` then this call will throw ``Error_timeout`` if t1 < t2 and operation does
    not complete before t1. If t1 > t2 or if operation completes before t1 (as expected) then
    ``op.wait(t2)`` will return as usual.


Input/Output stream requirements
................................
Class ``S`` implements input (output) stream if it implements ``Input_stream``
(``Output_stream``) interface and defines classes:

- ``S::Read_op`` (``S::Write_op``) that implements read (write) operation for ``S``,
- ``S::Read_some_op`` (``S::Write_some_op``) that implements read-some (write-some)
  operation for ``S``.

To perform I/O operation on a stream one creates an instance of ``Read/Write_op`` or
``Read/Write_some_op`` passing to the constructor the stream instance, sequence of
buffers and possibly a deadline for the operation. Using buffer sequences, like in
``boost::asio``, allows to easily compose packets from fragments without a need to copy
bytes around.

Apart from being used to create asynchronous I/O operations, streams implement the
following ``Input/Output_stream`` interface methods.

.. function:: bool Input_stream::eos()

  Returns true when end of stream has been reached and no more data is expected in it (for
  example connection was closed). Note that if this method returns ``false`` it does not mean
  that it will be possible to read anything from the stream. But if it returns ``true`` then
  we know that reading is not possible. Attempt to create read operation for stream whose
  :func:`eos` method returns ``true`` throws ``Error_eos`` error.

.. function:: bool Input_stream::has_bytes()

  Returns true if there are some bytes in the stream that can be consumed right now. If this
  method returns ``false`` then read operation for the stream will have to wait for new data
  to arrive before it can complete.

.. function:: bool Output_stream::is_ended()

  Returns true when output stream has "ended" and no more bytes can be written to it. If this
  method returns ``false``, it does not mean that it will be actually possible to write more
  bytes to the stream. But if it returns ``true`` then we know that no more bytes can be written.
  Attempt to create write operation for stream whose :func:`is_ended` method returns ``true``
  throws ``Error_eos`` error.

.. function:: bool Output_stream::has_space()

  Returns true if it is possible to send some bytes to the output stream at the moment. If
  this method returns ``false`` then write operation for the stream will have to wait
  until writing is possible before it can complete.

.. function:: void Output_stream::flush()

  Ensure that all bytes written to the stream have been sent to the other end.
  Note that this does not mean that the other end has received them. This method
  might block until all buffered bytes are sent out.


Connection requirements
.......................

Class ``C`` is a connection class if it meets input and output stream requirements and,
on top of that, it implements ``Connection`` interface. An instance of connection class
``C`` is initially in disconnected state. One has to call :func:`connect` method to
establish connection and only then instance can be used to create read/write operations.

.. function:: void Connection::connect()

  Establish the connection. Does nothing if connection is already established. It can be
  used to re-establish connection after it was closed either by the other end or with
  :func:`close` call (that is, when :func:`is_closed` returns ``true``). In this case
  any data that was not consumed from the input stream or not sent to the output stream
  after connection was closed is discarded and streams start to serve data of the newly
  re-established connection.

  This method throws errors if it was not possible to connect to the other end. If
  :func:`connect` was not called for a connection instance, or if it failed, an attempt to
  create one of read/write operations defined by connection class will throw
  ``Error_no_connection`` error.

.. function:: bool is_connected()

  Returns ``true`` if a successful call to :func:`connect` was made. If this method returns
  ``false`` then both :func:`is_valid` and :func:`check_valid` return ``NO``. However,
  if :func:`is_connected` returns ``true`` it does not imply anything about values returned
  by :func:`is_valid` or :func:`check_valid`.

.. function:: void Connection::close()

  Close connection. Does nothing if connection is already closed.
  After closing connection :func:`is_closed` returns ``true`` and
  both :func:`is_valid` and :func:`check_valid` return ``YES`` (so that connection is
  still considered valid). Read/write operations created before connection was closed
  remain valid (and can still complete). However, after closing connection, its output stream
  is ended (no more bytes can be written) and, after consuming all remaining bytes,
  its input stream moves to eos state too. Thus further attempts to create i/o operations
  for the connection will throw ``Error_eos`` errors.

.. function:: bool Connection::is_closed()

  Returns ``true`` if connection is closed, either after calling :func:`close` or because
  the other end has closed it. See :func:`close` for description how closed connection
  behaves with respect to i/o operations.

.. versionadded:: post-ms1
  Methods below do not have to be implemented for the first milestone.

.. function:: void destroy()

  Close a connection and free all allocated resources. Connection instance on which
  :func:`destroy` was called is no longer valid and can not be used except for checking
  its validity with :func:`is_valid` and :func:`check_valid` (both return ``NO``).
  Any i/o operations created for a connection can become invalid after destroying this
  connection - attempt to use them has undefined behavior.

.. function:: option_t Connection::is_valid()

  Local, lightweight check if connection is functional. No I/O should be
  performed in this method.

.. function:: option_t Connection::check_valid()

  More thorough check for connection validity that might involve communication
  with the other end.


Handling multiple I/O requests
..............................

User code can create new i/o operation while previous one is still in progress:

.. code-block:: cpp
  :linenos:
  :emphasize-lines: 4

  Conn conn(...);
  conn.connect();
  Conn::Read_op rd1(conn, buf1);
  Conn::Read_op rd2(conn, buf2);

Operation ``rd2`` created in line 4 should read bytes to ``buf2`` *after* operation ``rd1`` is
completed. Implementation of class ``Conn`` can use different strategies:

- Allow only one operation at a time - in this case constructor of ``rd2`` will throw error if
  ``rd1`` is not completed when it is invoked.
- Allow creating several operations and queue them internally.

.. note:: Even when queue is implemented, it might be not possible to construct new i/o operation
  because queue capacity is limited.


I/O Events
..........

Asynchronous read/write operations created by :func:`read` and :func:`write`
methods can wait for the following events:

 +---------------+--------------------------------------+----------------------+
 | Event type    | Description                          | Event info interface |
 +===============+======================================+======================+
 | ``SOCKET_RD`` | Data is available on a given socket. | ``Event_rd_event``   |
 +---------------+--------------------------------------+----------------------+
 | ``SOCKET_WR`` |  Data can be written to a socket.    | ``Event_wr_event``   |
 +---------------+--------------------------------------+----------------------+

The object returned by :func:`Async_op::waits_for` implements interface
``Socket_rd_event`` or ``Socket_wr_event`` corresponding to the type of the event.

.. uml::

  !include class.cnf
  !include designs/async.if!2

  package "CDK Foundation - I/O events" {

  interface Socket_event {
    Event: it is possible to do
    I/O on a socket.
    --
    socket()  : socket_t
    get_buffer() : bytes
  }

  Socket_event --|> Event_info

  interface Socket_rd_event {
    Event: it is possible to read
    bytes from socket
    --
    bytes_read(size_t)
  }

  Socket_rd_event --|> Socket_event

  interface Socket_wr_event {
    Event: it is possible to write
    bytes to socket
    --
    bytes_written(size_t)
  }

  Socket_wr_event --|> Socket_event

  }


.. function:: socket_t Socket_event::socket()

  Return native OS handle to the socket for which we are waiting.

.. function:: bytes Socket_event::get_buffer()

  Return memory area where data from the socket should be stored or from
  which data should be written to the socket. User code that detects that data
  is available on the socket can read some of this data into provided buffer
  before calling :func:`continue` method of the read operation. Similar,
  user code that detects that data can be written to the socket can already
  write some bytes from the provided buffer (see below).

.. function:: void Socket_rd_event::bytes_read(size_t howmuch)

  If user code handling asynchronous operation that waits for ``SOCKET_RD``
  event reads some bytes from the socket while checking that data is available
  on it, it should call this method to inform how much bytes have been read.
  The bytes should be stored in memory area returned by :func:`get_buffer()`.

.. function:: void Socket_wr_event::bytes_written(size_t howmuch)

  If user code handling asynchronous operation that waits for ``SOCKET_WR``
  event wrote some bytes to the socket while checking that output is possible
  on it, it should call this method to inform how much bytes have been written.
  The bytes should be taken from memory area returned by :func:`get_buffer()`.

.. _buffers

Buffers
.......

The read and write operations accept a sequence of contiguous memory buffers
as place to store bytes or read bytes from. Buffers are filled or read in order -
when one buffer ends, next one is used. Each buffer is described by object
of type ``bytes`` which represents a continuous area in memory.

.. uml::

  !include class.cnf

  package "CDK Foundation - Buffers" {

    !include designs/base.if!0
    !include designs/base.if!1

  }

Methods of ``buffers`` class:

.. function:: unsigned buffers::buf_count()

  Return number of buffers in the sequence.

.. function:: bytes buffers::get_buffer(unsigned pos)

  Return buffer indicated by its 0-based position in the sequence.

.. function:: size_t buffers::length()

  Returns total length of all buffers in the sequence.

.. function:: buffers(bytes buf)
              buffers(byte *buf, size_t len)
              buffers(byte *begin, byte *end)

  Construct a single-buffer sequence.

.. function:: buffers(bytes buf, buffers rest)

  Construct a sequence starting with given buffer ``buf`` and including all
  buffers from another buffer sequence ``rest``.

.. function:: buffers(buffers base, size_t offset)

  Construct buffer sequence which is like the base sequence but data is stored
  or read starting from the given offset in the base buffers. The first buffer
  of the new sequence is the buffer of base sequence in which the byte specified
  by offset is stored. Length and buffer count are updated accordingly.
  If specified offset is past the end of base buffer sequence then this constructor
  throws "out of bounds" error.


Methods of ``bytes`` class:

.. function:: byte* bytes::begin()
              byte* bytes::end()

  Method :func:`begin` returns pointer to the first byte in the area,
  :func:`end` to one byte past last byte of the area. If :func:`begin`
  returns ``NULL`` then this is null area that contains no data. If
  ``begin() == end()`` then this is an area of size 0.

.. function:: size_t bytes::size()

  Return number of bytes in the area. Returns 0 for null area.

.. Note:: Instances of ``bytes`` and ``buffers`` do not own memory they describe.
   Allocating and freeing this memory is left to the application. Class ``buffers``
   is only a way of describing a sequence of buffers allocated elsewhere.


Iterators
---------

Iterators are used to iterate over sequence of items such as diagnostic
information, meta-data info and other. Each iterator should implement the
following interface to move through the sequence:

.. uml::

  !include class.cnf

  package "CDK Foundation - Iterator" {

  !include designs/iterator.if

  }

.. function:: bool Iterator::next()

  Moves iterator to the next item in the sequence (if possible). Returns
  ``false`` if there are no more items in the sequence (iterator is at the
  last item). One has to call next() before accessing the first item in the
  sequence.


Using above interface one can iterate through the sequence as follows::

  Iterator it= ...;

  while(it.next())
  {
    ...
  }

Particular iterator implementation must extend Iterator interface with
additional methods for accessing the current item in the sequence.


Hiding Implementation
=====================

Why to hide
-----------

Let us consider adding class ``cdk::Socket`` which implements ``Connection`` interface.
This class would be declared in public CDK header ``socket.h`` so that code could
use it as follows::

  #include <mysql/cdk/socket.h>

  cdk::Socket sock(...);
  ...

Now, if ``Socket`` is implemented using ``boost::asio`` library, definition of the
class will look something like this::

  #include <boost/ip/tcp/socket.hpp>

  class Socket
  {
    boost::ip::tcp::socket  m_sock;
    ...
  };

This creates a dependency of CDK public header ``socket.h`` on boost headers. It also
means that CDK ABI depends on boost ABI: whenever layout of ``boost::tp::tcp::socket``
is changed by boost the layout of our ``cdk::Socket`` would also change. We do not want
that - we want to have complete control over CDK ABI.

Thus all CDK public headers should work without a need to include any third-party
library headers. Only using headers available on the build platform (such as C++ runtime
library headers) is OK.

How to hide
-----------

Simple idea is to store in ``cdk::Socket`` a pointer to internal implementation object
that will hold implementation specific data. However, we still want to follow RAII pattern
so that in code like this::

  {
    cdk::Socket sock(...);

    ...
  }

the internal implementation object is properly destroyed when ``Socket`` instance goes
out of scope. Also, the type of implementation object can not be fully defined in the
public header, which should look something like this::

  class Socket
  {
    class Impl;
    Impl  *m_impl;
    ...
  };

Then implementation of ``Socket`` must remember to initialize ``m_impl`` and to
delete the implementation in the destructor.

To make this process less error-prone, some template magic is used so that author of
``Socket`` class can declare it as follows (in ``socket.h``)::

  #include <opaque_impl.h>

  class Socket : opaque_impl<Socket>
  {
    Socket(...);
    void foo();
    ...
  }

The exact type of internal implementation object is declared outside of the public
headers (in ``socket.cc``) using :func:`IMPL_TYPE` macro::

  #include "socket.h"
  #include <opaque_impl.i>

  class Socket_impl
  {
    void bar();
    ...
  };

  IMPL_TYPE(Socket, Socket_impl);

Line ``IMPL_TYPE(Socket, Socket_impl)`` declares that internal implementation
for ``Socket`` is of type ``Socket_impl``. It creates appropriate specializations
for ``opaque_impl<Socket>`` base class and its methods. It uses infrastructure
that is included with ``<opaque_impl.i>`` (note the difference with ``<opaque_impl.h>``
which is used in public headers).

When constructing ``Socket`` instance, one can pass parameters to the base class
``opaque_impl<Socket>`` so that they will be used when constructing the internal
implementation object. For technical reasons, these parameters must be preceded by
a phony ``NULL`` argument::

  Socket::Socket(int port) : opaque_impl<Socket>(NULL, port)
  {
    ...
  }

The internal implementation object can be accessed using :func:`get_impl` method
(inherited from ``opaque_impl<Socket>``)::

  void Socket::foo()
  {
    get_impl().bar();
    ...
  }

Base ``opaque_impl<Socket>`` class takes care of destroying internal implementation
object when ``Socket`` instance is destroyed - the latter does not even have to define
a destructor.

Default and copy constructors
.............................

If we want to define default or copy constructor for ``Socket`` class that uses
``opaque_impl<Socket>``, we must be able to create a default instance of the internal
implementation object or to create a copy of the implementation object.

Normally, the base class ``opaque_impl<Socket>`` does not have default nor copy
constructor, because it is not known if the underlying implementation class has them.
If this is the case: the implementation type ``Socket_impl`` defines appropriate
default/copy constructors, a support for them can be added to ``opaque_impl<Socket>``
with :func:`IMPL_DEFAULT` and :func:`IMPL_COPY` macros::

  IMPL_TYPE(Socket, Socket_impl);
  IMPL_DEFAULT(Socket);
  IMPL_COPY(Socket);

With these declarations, ``Socket`` can define default/copy constructor like this::

  Socket::Socket()
  {
    // Do additional initialization
  }

  Socket::Socket(const Socket &s) : opaque_impl<Socket>(s)
  {
    // Do additional initialization
  }

Default constructor for ``Socket`` uses ``opaque_impl<Socket>`` default constructor
defined with :func:`IMPL_DEFAULT` macro. This constructor builds internal
implementation object using ``Socket_impl`` default constructor. Similar for
copy constructors.

If :func:`IMPL_DEFAULT` or :func:`IMPL_COPY` are used but underlying implementation
type does not have corresponding default/copy constructor, compilation will fail.


.. ifconfig::False

  Interface Capability_info
  .........................
  Not all interface methods can be implemented by all objects that implement given
  interface. For example, a simple cursor object can not be able to implement the
  :func:`Cursor::seek` method. If one attempts to call :func:`seek` on such a simple
  implementation, an "unimplemented feature" error should be thrown.

  To be able to check up-front which interface features are supported by a given
  implementation, this implementation can also implement ``Capability_info``
  interface  with single method:

  .. function::  option_t has_capability(Capability)

    Return ``YES`` if given capability is supported, ``NO`` if it is not supported
    (and attempt to use it would throw error) and ``UNKNOWN`` if it can not
    be determined at the moment if the capability is supported or not.

  source instance is created, it can be queried to find out if given data source
  supports prepared statements::

    Data_source ds(...);

    if (NO == ds.has_capability(PREPARED_STATEMENTS))
      throw "this code requires prepared statements";

  If :func:`has_capability` returns ``YES`` then we know that sessions created
  for this data source will support prepared statements. But if it returns
  ``UNKNOWN``, a session for this data source still can support prepared
  statements or not::

    Session s(ds,...);

    if (NO == s.has_capability(PREPARED_STATEMENTS))
      throw "this code requires prepared statements";


