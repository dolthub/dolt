Design Principles
=================

- Data source abstraction - a data source is identified by instance of data
  source class and different types of data sources have corresponding classes
  with constructors suitable for that particular type of data source.
  Implementation details such as drivers, driver managers etc. are hidden behind
  this abstraction.

- RAII - Constructors do full initialization, objects automatically free
  resources when they go out of their scope. The intended usage pattern is this:

  ::

    {
      Session s(...)
      ...
    }

  and not this:

  ::

   {
     smart_ptr<Session> sptr= foo.get_session();
     sptr->init(...);
     ...
   }

- Errors reported via exceptions - if method returns it means operation has been
  successfully performed. Example: if commit() returns transaction id it means
  that the transaction has been successfully committed - otherwise an error should
  be thrown.

- Visitor pattern to access data without a need to buffer it internally (for
  example the Row_processor interface and Cursor::next_row(Row_processor)).

- Asynchronous operation - methods can return an object representing an
  on-going, asynchronous operation that can be queried for completion,
  waited-for, cancelled etc. It should be possible to integrate with existing
  async-io frameworks such as boost::asio.

- Managing object ownership - for example: a reply object belongs to a session -
  if session is closed or destroyed, the corresponding reply object becomes
  invalid - an attempt to use it raises exception.

- Handling meta-data and type conversions is left to application code - CDK
  gives direct access to raw bytes as received from the data store.

- Allow different ways of submitting queries and commands (SQL, CRUD or others):
  the exact apis for this are defined outside the CDK.

- Bare wire access to the underlying protocol: no emulations, client-side
  session tracing etc. For example getting current session's schema possible
  only if supported by the protocol.


CDK Structure
=============

The main functionality of CDK will be accessed using the following main objects
which implement Core CDK API and its extensions. Core CDK API provides general
framework for handling data store sessions and processing query results without
defining how queries or commands look like and how they are submitted in
a session. The latter is defined by extended APIs such as CRUD or SQL.

.. uml::

  title Session-level objects

  object "data source" as ds {
    Represents a transactional data store.
  }

  object options {
    Describes options for
    data store session.
  }

  object session {
    Allows accessing and manipulating
    data in a data store.
  }

  object result {
    Describes result of an operation
    performed in a data store session.
  }

  object cursor {
    Iterates over rows in a result set.
  }

  ds      <-- session : constructed\nfrom
  options <- session : constructed\nfrom

  session --> result : session\noperations
  result <-- cursor : constructed\nfrom

  object "row processor" as rp {
    Processes rows returned
    form a cursor
  }

  rp .left> cursor : used with

  object "session listener" as sl {
    Gets notified about
    session related events
  }

  sl .left> session : register with


Above objects are implemented in terms of lower-level objects that handle
communication using supported protocols. It is also possible to use these
protocol-level objects directly.

.. uml::

  title Protocol-level objects

  object endpoint {
    Represents a connection endpoint
    for some physical transport media
    such as TCP/IP
  }

  note bottom
    Different types of endpoints to handle
    different transport media.
  end note

  object "connection options" as opt {
    Describes connection characteristics
  }

  object connection {
    Represents physical connection
    with an endpoint
  }

  connection -> endpoint : constructed\nfrom
  connection --> opt : constructed\nfrom

  object protocol {
    Has methods for sending and
    receiving protocol messages
    over a connection
  }

  note bottom
    Different types of protocol objects to
    handle different supported protocols.
    Each protocol has its own API.
  end note

  protocol -> connection : constructed\nfrom

  object "message processor" as mp {
    Processes protocol messages when protocol
    object receives them.
  }

  mp ..> protocol: used with


CDK also provides a number of base types and classes that can be useful
for other components. They form CDK Foundation which includes generic error
classes, I/O abstraction, asynchronous operations and similar.


Implementation Traits
=====================

CDK interfaces use some implementation defined types that are not specified by
the CDK itself. Each implementation should chose these types so that they best
match the range of data sources that are supported by that implementation.
Implementation traits are the selection of types chosen by a particular
implementation.  One can think of CDK interfaces as templates being parametrized
by implementation traits.

Implementation should define the following numeric types:

:row_count_t:  numbers big enough to count rows in a single table
:column_pos_t: numbers big enough to count columns of a single table
:savepoint_id_t: numbers used for transaction savepoint ids
:batch_pos_t:    numbers big enough to count commands inside a single batch

and the following additional types (which are not assumed to be numeric):

:Table_ref:         values of this type identify tables in the data store

:Type_info:         describes type of values stored in a table column

:Format_info:       describes format in which values are serialized into
                    byte sequences

:Column_info:       describe additional column characteristics

:Reply_init:        values of this type are used to initialize reply
                    instances (see below)

:transaction_id_t:  transaction identifiers used by the data store


Initialization of Reply objects
-------------------------------
A reply object should support assignment from Reply_init type defined by
implementation traits. Suppose that function foo()  returns values of
Reply_init type::

  Reply_init foo(...);

Then the following code should work::

  Reply r;
  r= foo(...);

It means that Reply object should have a default constructor (which constructs
"empty" reply object) and then it should define assignment operator::

  Reply& Reply::operator=(Reply_init init);

which stores in the Reply instance information obtained from Reply_init value
that will be used to implement reply object methods.

The exact choice of Reply_init type is done by implementation and is not
restricted by Core API specs. For example, Reply_init could be a pointer to
Session class provided that all information about current reply can be obtained
from the session.


.. _Type_system:

Scalar value type system
------------------------

CDK is not making any assumptions about what types of values are exchanged
between client and a data store and how values of various types are
represented as sequences of raw bytes. Instead, implementation defines
classes that convert between raw byte representation and native C++ types.

Know scalar types are given by implementation-defined enumeration
:class:`Type_info`. For example, it can contain constants ``STRING`` and
``NUMBER`` which represent known types.

Values of each type can be represented as sequences of bytes using one of the
representation formats defined for that type. For example, values of type
``STRING`` can be represented using different character encodings.
Information about representation format for values of some type is given by
values of type :class:`Format_info`. If `fi` is such a value and :class:`TTT`
is a type from :class:`Type_info` then ``fi.for_type(TTT)`` should return
`true` only if format described by `fi` is applicable to values of
type :class:`TTT`.

.. function:: bool Format_info::for_type(Type_info t)

  Returns true if this format is applicable to values of type t.

Representation format information can be used in two ways. First, one can
create encoder/decoder for values of a given type using given representation
format::

  Format_info fi;

  assert(fi.for_type(STRING));

  Codec<STRING> codec(fi);

.. function:: Codec<T>::Codec(Format_info fi)

  Create encoder/decoder for values of type :class:`T` using representation
  format `fi`. This constructor throws error if format `fi` is not applicable
  to values of type :class:`T`.

Encoder/decoder should define the following methods.

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


Apart from creating encoder/decoder one can examine representation format
directly. This can be done by creating instance of appropriate
:class:`Format` template specialization::

  Format_info fi;

  assert(fi.for_type(STRING));

  Format<STRING> fmt(fi);
  cout <<"character encoding: " <<fmt.cs_name();

.. function:: Format<T>::Format(Format_info fi)

  Create an object that can be used to examine information about
  representation format for values of type :class:`T` given by `fi`.
  This constructor throws error if format `fi` is not applicable
  to values of type :class:`T`.

Methods implemented by :class:`Format\<T>` instances are
implementation-defined and can be different for each implementation and each
type supported by it.
