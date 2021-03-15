==============
MySQLX Session
==============

The MySQLX Session is an implementation of extended Core Session API that uses
MySQLX protocol to communicate with MySQL server.

Synopsis
========
A MySQLX Session implementation is defined in the following
header::

  #include <mysql/cdk/mysqlx/session.h>
  using namespace cdk;


To create new session one has to have a valid connection object and options
object which gives information such as user/password

.. code::

  connection::TCPIP conn(...);
  mysqlx::Session::Options opt(...);

  mysqlx::Session sess(conn, opt);


Checking diagnostics information:

.. code::

  if (NO == sess.is_valid() || NO == sess.check_valid())
  {
    cerr <<"Could not create session" <<endl;
    report_diagnostics(sess, Diagnostics::ERROR, "Session");
  }

The following helper function is used to print diagnostic information stored in
an object that implements Diagnostics interface:

.. code::

  void report_diagnostics(api::Diagnostics &obj,
                          api::Diagnostics::Level lvl,
                          const string &prefix)
  {
    api::Diagnostics::Iterator &entries = obj.get_entries();
    while (entries.is_valid())
    {
      cerr <<prefix <<": " <<entries.entry() <<endl;
      entries.next();
    }
  }


Issuing SQL queries:

.. code::

   // issue a query
   Reply r= sess.sql("SELECT * FROM table");

   // wait for reply
   r.wait();

   // check diagnostics

   if (0 < r.entry_count(ERROR))
   {
      report_diagnostics(r, Diagnostics::ERROR, "Reply");
      throw "Error when executing query";
   }

   if (0 < r.entry_count(WARNINGS))
   {
      cerr <<"Warnings when executing query" <<endl;
      report_diagnostics(r, Diagnostics::WARNING, " ");
   }

   if (!r.has_results())
     throw "No results from the query";


Read rows of the reply:

.. code::

   // Create cursor to access rows from the next result set of r
   Cursor c(r);

   // Create row processor object
   My_row_processor rp(c);

   // Iterate over rows in the cursor
   while (c.get_row(rp))
   {}

A row processor implements callback methods which are called by
c.get_row() as result rows arrive:

.. code::

   class My_row_processor: api::Row_processor
   {
     byte m_buf[1024];  // buffer for storing row fields
     size_t m_howmuch;  // how much of m_buf[] contains valid data

     api::Meta_data &m_md; // meta information about rows

     My_row_processor(Cursor &c) :m_md(c)
     {
        cout <<"Fetching rows of a result set" <<endl;
     }

     bool row_begin(row_count_t pos)
     {
       cout <<"== Row #" <<pos <<endl;
     }

     void end_of_data()
     {
       cout <<"End of data" <<endl;
     }

     size_t field_begin(field_pos_t pos)
     {
       cout <<"- field #" <<pos <<;
       cout <<" (" <<m_md.col_info(pos).name() <<"): " <<endl;
       m_howmuch= 0;
       return sizeof(m_buf);
     }

     void field_end(field_pos_t pos)
     {
       Type_info   ti= m_md.type(pos);
       Format_info fi= m_md.format(pos);

       // Interpret bytes in m_buf as a value of given type in given
       // format, then print it on cout.

       cout <<endl;
     }

     size_t field_data(field_pos_t, bytes data)
     {
       memcpy(m_buff + m_howmuch, data.begin(), data.length());
       m_howmuch += data.length();
       return sizeof(m_buff) - m_howmuch;
     }

   };


MySQLX Session class
====================

The MySQLX Session is implemented by :class:`cdk::mysqlx::Session` class defined in header
``<mysql/cdk/mysqlx/session.h>``.

.. uml::

  !include class.cnf
  !include designs/diagnostics.if
  !include designs/core_api.if!0
  !include designs/core_api.if!1

  class "cdk::mysqlx::Session" as XSession {
     MySQL session over MySQLX protocol
    .. Constructor ..
    Session(Connection, Options)
    .. SQL queries ..
    sql(string): Reply_init
    .. CRUD operations ..
    TBD
  }

  XSession --|> Session
  XSession --|> Transaction

  interface "cdk::Session::Options" as Options {
    Generic session options
    --
    user(): string
    password(): bytes
  }

To create a new session object, one requires a connection object and options which
define data such as user credentials that are required during establishing new sesion

.. function:: cdk::mysqlx::Session::Session(Connection conn, Options opt)

  Create new session that will communicate over given connection. The ``opt`` argument
  is an options object that gives information required for establishing new session.
  Once created, session is ready to be used. All authentication and initial session
  setup is done in the constructor.

  Constructor throws errors if valid session could not be created. This is also the
  case if server reported errors during handshake. These errors should be stored in
  the diagnostic area of Session instance, but constructor also throws an error informing
  about the failure.

  Newly created object implements Core API interfaces: :ref:`Session` and
  :ref:`Transaction`.


Issuing SQL queries and getting results
=======================================

.. function:: Reply_init cdk::mysqlx::Session::sql(string query)

  Issue SQL query to the server and return initializer for :class:`Reply`
  object that can be used to access query results (see below).

Class :class:`mysqlx::Reply` implements Core API :class:`Reply` interface to
give access to server reply to a query or command (see :ref:`Reply`).
A new instance of :class:`mysqlx::Reply` is created using initializer of
implementation-defined type :type:`Reply_init`:

.. function:: cdk::mysqlx::Reply(Reply_init initializer)

  Create reply object from a given initializer.

Since :func:`Session::sql` returns a reply initializer, the following code
should work::

  Session s;
  Reply r(s.sql(...));

Also, an assignment should invoke correct :class:`Reply` constructor::

  Session s;
  Reply r= s.sql(...);

.. note:: Implementing ``mysqlx::Reply::operator=(Reply_init)`` can make
  above assignment more efficient.

Making another assignemnt to a reply object should discard the current
reply (implicitly calling :func:`Reply::discard`) and replace it with a new
reply::

  Reply r= s.sql(...);
  r= s.sql(...); // implicit discard() of previous reply


Reading rows of a result set
============================

Class :class:`mysqlx::Cursor` implements Core API :class:`Cursor` interface
to iterate over rows of a result-set contained in a reply to a query (see
:ref:`Cursor`). An instance of :class:`mysqlx::Cursor` can be constructed
form :class:`mysqlx::Reply` instance:

.. function:: cdk::mysqlx::Cursor::Cursor(Reply r)

  Create a cursor instance which can iterate over rows from next result-set
  of the given reply. If reply contains no (more) result-sets then error is
  thrown.

  If given reply contains several result-sets, then created cursor gives
  access to the first result-set in the sequence and this result set is
  removed from the sequence. Thus creating next cursor instance for the
  same reply object will give access to the next result-set in the sequence.
  Note that :class:`Reply` interface has methods like
  :func:`Reply::skip_result` which also shorten the sequence.

Accessing multiple result-sets
------------------------------

Creating cursor for a reply object ``r`` gives access to the current
result-set from the sequence of multiple result-sets present in the reply.
Creating next cursor instance for the same reply ``r`` gives access to
the next result-set. If this is done before previous cursor was closed, the
cursor is closed implicitly::

  Reply r;
  Cursor c1(r);
  Cursor c2(r); // implicit close of c1


Discarding replies
------------------

Reply object has :func:`discard` method that discards this reply and all
the remaining result-sets contained in it. If a cursor was created to
access a result-set from a reply object then this reply can not be discarded:
a call to :func:`discard` will throw error in that case (see :ref:`Reply`).

.. note:: To implement this, reply object must probably hold a reference
  to cursor instance(s) that was created for that reply.


Asynchronous operation
----------------------

Reading rows from a cursor is an asynchronous operation. If `c` is a cursor then
 ``c.get_rows(rp)`` starts an asynchronous operation which will fetch rows from
the cursor and will pass them to the row processor. The call to :func:`get_rows`
returns immediately, before the operation is completed. The progress of the
operation can be controlled using :class:`Async_op` interface methods which are
implemented by cursor instance::

  c.get_rows(rp,100);
  while (!c.is_completed())
    c.cont();
  c.wait();
  row_count_t rows_feteched= c.get_result();

When completed, this asynchronous operation returns result which is the number of
rows that have been fetched by the operation.

Another call to :func:`get_rows` creates new fetch operation, but first waits for
the previous operation (if any) to complete (as if doing implicit call
to :func:`wait`). Thus, for example, the following code::

  c.get_rows(rp,10);
  c.get_rows(rp,10);

is equivalent to::

  c.get_rows(rp,10);
  c.wait();
  c.get_rows(rp,10);

The asynchronous fetch operation is interrupted if row processor callback throws
an error. In this case a call to :func:`cont` or :func:`wait` throws an error.
Fetching of rows stops at the last successfully processed one, and new
:func:`get_rows` operations can be used to fetch the remaining rows.
Calling :func:`get_result` after such interruption of fetch operation tells how
many rows were successfully processed before the interruption. For example::

  try {

    c.get_rows(rp, 10);
    c.wait();

  }
  catch(Error)
  {
    // fetch remaining rows
    c.get_rows(rp, 10 - c.get_result());
  }


Result set meta-data
====================

Interface :class:`mysqlx::Meta_data` is a specialization of Core API interface
:class:`Meta_data` used to access meta-data information about rows of a result
set (see :ref:`Meta_data`). This interface is implemented by
:class:`mysqlx::Cursor` class so that given a cursor instance one can get
meta-data information about rows that are accessed through this cursor.

Interface :class:`mysqlx::Meta_data` uses :class:`mysqlx` specific variants of
types :class:`Type_info`, :class:`Format_info` and :class:`Column_info` that
represent meta-data information.

.. uml::

  !include class.cnf

  enum "mysqlx::Type_info" {
    Types of values stored in a row
    --
    NUMBER
    STRING
    DOCUMENT
    ...
  }

  class "mysqlx::Format_info" {
    Format in which value of given type
    can be stored.
    --
    for_type(Type_info): bool
  }

  class "mysqlx::Column_info" as CI {
   Other information about a column
   of a result set
   --
   name(): string
   column() : Column_ref*
  }


Column information
------------------

Column in a result set can have its name, which is given by
:func:`Column_info::name` method (it is an empty string if column has no
name). Such a column can also correspond to a table column. In such case
:func:`Column_info::column()` returns a non-NULL pointer to
:class:`Column_ref` instance which describes a table column.

.. uml::

  !include class.cnf

  class Column_ref {
    table() : Table_ref*
  }

  class Table_ref {
    schema() : Schema_ref*
  }

  class Schema_ref {
    catalog() : string
  }

  class Object_ref {
    Generic reference to
    an object in a data store
    --
    name() : string
  }

  Column_ref --|> Object_ref
  Table_ref --|> Object_ref
  Schema_ref --|> Object_ref

A :class:`Column_ref` instance can represent column in some specific table:
in this case :func:`Column_ref::table` returns non-NULL pointer to
:class:`Table_ref` instance. Otherwise, if :func:`Column_ref::table` is NULL
then this refers to a named column with implicit table which must be deduced
from context.


Value types and their representation formats
--------------------------------------------

Current implementation supports 2 basic types of scalar values with
corresponding constants in :class:`mysqlx::Type_info` enumeration:

:NUMBER:  a numeric value (either integer or floating point)
:STRING:  a character string

The corresponding representation formats are described by :class:`Format\<T>`
instances as described in :ref:`Type_system`.


Numeric formats
...............

Numeric format determines how a number (integer or float) is represented as
a sequence of bytes. The following numeric formats are recognized:

:BINARY:  Plain binary encoding for unsigned integer numbers.
:TWO_COMPLEMENT: Two's complement binary encoding for signed integers.
:SINGLE: Single precision float numbers using IEEE encoding.
:DOUBLE: Double precision float numbers using IEEE encoding.

Formats ``DOUBLE`` and ``SINGLE`` use known number of bits, the integer
formats can use different bit sizes, as returned by
:func:`Format<NUMBER>::bit_size()` function.

If bit size spans several bytes then :func:`Format<NUMBER>::endianess()`
informs about the order of these bytes in memory (for ``LITTLE`` endian
least-significant byte comes first). 

Function :func:`Format<NUMBER>::is_signed()` informs if given numeric format
supports representing negative numbers (this is not the case only for
``BINARY`` format).

Function :func:`Format<NUMBER>::precision()` returns a number of significant
digits after decimal point (if it is fixed). For integer formats this
function returns 0. For ``DOUBLE`` and ``SINGLE`` it returns
``VARIABLE_PRECISION`` constant which is > 0.

.. uml::

  !include class.cnf

  enum Numeric_format {
    Number encoding formats
    --
    BINARY
    TWO_COMPLEMENT
    SINGLE
    DOUBLE
  }

  enum Endianness {
    Order of bytes for
    multi-byte representations
    --
    LITTLE
    BIG
  }

  class "Format<NUMBER>" {
    Information about numeric
    value representation
    --
    Format(Format_info)
    ..
    format() : Numeric_format
    endianness() : Endianness
    is_signed() : bool
    bit_size()  : uint
    precision() : uint
  }


Example code that converts numeric column value to appropriate C++ type and
reports it via :func:`col_val` callback:

.. code::

  void col_val(int);
  void col_val(unsigned int);
  void col_val(float);
  void col_val(double);

  void report_value(Format_info fi, bytes data)
  {
    assert(fi.for_type(NUMBER));
    Format<NUMBER> fmt(fi);

    switch (fmt.format())
    {
    case BINARY:
    case TWO_COMPLEMENT:

      if (fmt.bit_size() != 8*sizeof(int))
        throw "can handle only standard integers";
      if (fmt.endianness() != native_endianness)
        throw "can handle only native endianness";

      if (BINARY == fmt.format())
        col_val(*(unsigned*)data.begin());
      else
        col_val(*(int*)data.begin());

      break;

    case SINGLE:
      col_val(*(float*)data.begin());
      break;

    case DOUBLE:
      col_val(*(double*)data.begin());
    }
  }


String formats
..............

String format determines how a sequence of characters is represented in
memory. Class :class:`Format\<STRING>` specializes standard C++ class
:class:`std::codecvt` (appropriate specialization of the template) so that,
apart from using :class:`Codec\<STRING>` one can use conversion mechanisms
provided by C++ runtime library
(see http://en.cppreference.com/w/cpp/locale/wstring_convert).

.. uml::

  !include class.cnf

  class "Format<STRING>" as SF {
    Information about character
    string representation
    --
    Format(Format_info)
    ..
    cs_name() : string
  }

  class "std::codecvt" as codec

  SF -|> codec


Example usage for decoding bytes received from server given string column
format information can look as follows.

.. code::

  Meta_data md(..);

  // Assuming that column 0 is of type STRING
  assert(STRING == md.type(0));

  Format<STRING> fmt(md.format(0));

  std::wstring_convert<Format<STRING>> conv(fmt);

  // Assuming data from server is in buf convert it to C++ wide string
  bytes buf;
  wstring str = conv.from_bytes((char*)buf.begin(), (char*)buf.end());


Additionally, function :func:`Format<STRING>::cs_name` returns the name
of the character set encoding as reported by the server.


Encoders and decoders
.....................

See :ref:`Type_system`.

Example:

.. code::

  void col_val(long int);
  void col_val(unsigned long int);
  void col_val(double);
  void col_val(wstring);

  void report_value(Type_info ti, Format_info fi, bytes data)
  {
    switch (ti)
    {
    case NUMBER:
      {
        Format<NUMBER> fmt(fi);
        Codec<NUMBER>  codec(fi);

        // report non-integer numbers as double values

        if (fmt.precision() > 0)
        {
          double val;
          codec.from_bytes(data, val)
          col_val(val);
          return;
        }

        if (fmt.is_signed())
        {
          long val;
          codec.from_bytes(data, val)
          col_val(val);
          return;
        }
        else
        {
          unsigned long val;
          codec.from_bytes(data, val)
          col_val(val);
          return;
        }
      }

    case STRING:
      {
        Codec<STRING> codec(fi);
        wstring val;
        codec.from_bytes(data, val);
        col_val(val);
      }
    }
  }
  
