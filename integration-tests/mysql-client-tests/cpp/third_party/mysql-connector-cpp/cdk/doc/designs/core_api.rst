Core CDK API
============
.. todo::
  - Prepared statements API
  - Batch execution API

The Core CDK API consists of a set of interfaces which can be defined as
abstract virtual classes. Interfaces provide only core functionality and to be
useful must be extended by the full API. At this level it is also left open what
objects will implement each interface, how these objects are created etc. For
example, it is not specified here how one creates a data store session or how
one gets a result of a query. Only the interfaces that these objects should
implement are defined.

Apart from standard C++ types and types from data store traits, Core interfaces
use these base types and classes which are part of CDK Foundation. The exact
specification and implementation of these types and classes is to be defined by
CDK Foundation design.

:option_t:        a 3-valued type which represents Boolean values YES/NO
                  and a third UNKNOWN value.

:timestamp_t:     values of this type represent points in time

:bytes:           values of this type represent sequences of bytes of
                  known length

:Error:           base class for all error classes

:Diagnostics:     interface for objects that store diagnostic information such
                  as errors, warnings etc.

:Async_op:        interface implemented by objects representing
                  asynchronous operations with methods for querying
                  operation state, waiting for an operation completion
                  and cancelling it.

:Iterator:        interface implemented by iterators that iterate over sequences
                  of items.

:Listener:        interface implemented by listeners which register itself to
                  be notified about events.

:Event_source:    interface implemented by objects that can send event notifications
                  to registered listeners.

Core CDK defines the following interfaces:

 ===================== ==================================================
  Interface             Description
 ===================== ==================================================
 Session               Basic operations on data store session.
 Transaction           Manipulating transaction state.
 Reply                 Examining data store reply to a command.
 Batch_reply           Examining reply to a batch of commands.
 Cursor                Iterate over rows of a result set.
 Blob                  Access data from BLOB fields.
 Meta_data             Describes a column in a result set.
 ===================== ==================================================

.. uml::

  !include class.cnf

  package "CDK Foundation" {

  !include designs/iterator.if
  !include designs/async.if!0
  !include designs/events.if
  !include designs/diagnostics.if!0

  }

  package "Core CDK API" {

  !include designs/core_api.if!0
  !include designs/core_api.if!1
  !include designs/core_api.if!2
  !include designs/core_api.if!3
  !include designs/core_api.if!4
  !include designs/core_api.if!5

  }


Some of the above interfaces use callback interfaces that must be implemented
by code that uses Core CDK API:

 ===================== ==================================================
  Callback Interface    Description
 ===================== ==================================================
 Out_param_processor   Get values of stored routine output parameters.
 Row_processor         Get values of row fields.
 Session_listener      Get notified about session related events.
 Trx_listener          Get notified about transaction related events.
 Reply_listener        Get notified about events related to command/query
                       reply.
 ===================== ==================================================

.. uml::

  !include class.cnf

  package "CDK Foundation" {

  !include designs/async.if!1
  !include designs/events.if!1

  }


  package "Core CDK - Callbacks" {

  interface Session_listener {
    Get notifications about session events
  }

  Session_listener "Session" --|> Listener


  interface Trx_listener {
    Get notifications about transaction events
  }

  Trx_listener --|> Listener


  interface Reply_listener

  Reply_listener --|> Async_op_listener


  interface Row_processor {
    Get values of row fields
    --
    row_begin(row_count_t pos) : bool
    row_end(row_count_t pos)
    field_begin(field_pos_t pos) : size_t
    field_end(field_pos_t pos)
    field_null(field_pos_t pos)
    field_blob(field_pos_t pos)
    field_data(field_pos_t pos, bytes data) : size_t
    end_of_data()
  }

  Row_processor --|> Async_op_listener

  interface Out_param_processor {
    Get values of stored routine output parameters
  }

  Out_param_processor --|> Row_processor

  }


Other interfaces

.. uml::

  !include class.cnf

  package "Core CDK - Other" {

  interface Op_statistics {
    table() : Table_ref
    table_op() : enum { DROPPED, CREATED, NONE }
    rows_scanned() : row_count_t
    rows_updated() : row_count_t
    rows_deleted() : row_count_t
    rows_inserted() : row_count_t
  }

  interface Op_time_info {
    time_received() : timestamp_t
    time_started() : timestamp_t
    time_completed() : timestamp_t
  }

  }



.. note:: Interface definitions below should be treated as high-level general
  descriptions. Full details such as exact types and type modifiers, whether
  arguments are passed by value or reference etc. will be determined by
  implementation.


.. _Session:

Interface Session
-----------------

Data store session allows querying and manipulating data stored in the data
store. :class:`Session` interface extends :class:`Diagnostics` one so that it is possible
to get diagnostic information about the session. Note that diagnostics for individual
queries and operations executed in a session is available via :class:`Reply` interface
(see below) - session diagnostics is only for things that are not related to any
particular query/command execution but to the session itself.

Core API does not define methods for executing queries and other data
store operations. However, a data store session should implement the following
methods:

.. function:: option_t Session::is_valid()
              option_t Session::check_valid()

   Check if given session is valid. Function :func:`!is_valid` performs a
   lightweight, local check while :func:`!check_valid` might communicate
   with the data store to perform this check. Both :func:`!is_valid`
   and :func:`!check_valid` return ``UNKNOWN`` if session state could not
   be determined.

.. function:: void Session::clear_errors()

   Clear diagnostic information that accumulated for the session. :class:`Diagnostics`
   interface methods such as :func:`Diagnostics::entry_count` and
   :func:`Diagnostics::get_entries` report only new diagnostics entries since last
   call to :func:`!clear_errors` (or since session creation if :func:`!clear_errors` was
   not called).

.. function:: void Session::close()

   Close session and free all allocated resources before session object itself
   is destroyed. Using session after :func:`!close` throws an error.


.. _Transaction:

Interface Transaction
---------------------
Transaction collects data modification requests which are executed in the data
store when transaction is committed. The :class:`Transaction` interface provides methods
for manipulating transaction state, but it does not define how data modification
requests are submitted to the transaction - such methods must be added above
Core API layer.

:class:`Transaction` interface extends :class:`Diagnostics` one to provide diagnostic info
related to transaction execution. Reported diagnostic entries are since last
commit/rollback or since transaction was created if no commit/rollback was done
yet. Note that transaction diagnostics does not include diagnostics for the
commands executed inside the transaction - these are provided via :class:`Reply`
interface.

.. function:: bool Transaction::has_changes()

   Returns true if there are any data modification requests collected in the
   transaction.

.. function:: transaction_id_t Transaction::commit()

   Commit the transaction. Returns identifier for the newly committed
   transaction. Whether new transaction is started after :func:`!commit` or
   not (and new transaction object must be created to start a new one) is
   implementation defined.

.. function:: void Transaction::rollback(savepoint_id_t sp = 0)

   Rollback transaction to the given savepoint. Savepoint id 0 (the default)
   means beginning of the transaction.

.. function:: void Transaction::savepoint(savepoint_id_t id)

   Create a savepoint with given id. If a savepoint with the same id was
   created earlier in the same transaction, then it is replaced by the new one. It
   is an error to create savepoint with id 0, which is reserved for the beginning
   of the current transaction.

Implementation options:

  One possibility is that session object implements :class:`Transaction` interface.
  This fits the case when there is only one transaction per session. This single transaction
  can be manipulated via session object. Another possibility is that there can be several
  concurrent transactions for a single session and then each transaction is represented by
  a separate object implementing :class:`Transaction` interface. For example::

    Session s(...);
    Transaction t1(s);
    Transaction t2(s);
    ...
    t1.commit();
    t2.rollback();


.. _Reply:

Interface Reply
---------------

Reply interface defines methods to examine generic reply to a data store command
or query including statistic information about query/command execution. Such
reply can contain one or more result sets, each of which is a sequence of rows.
It can also contain values of output parameters of a stored routine call. Reply
can contain diagnostic information for a command, including errors in case the
command execution failed.

Reply interface informs about reply to a single command. For a reply to
execution of a batch of commands see :ref:`Batch_reply` below.

Reply to a command is an asynchronous operation. This means that once reply
object is created, one has to wait until it "completes" before using the reply.
Reply methods that assume that reply object is complete, should do implicit
wait, so that the following code works as expected::

  Reply r= s.sql(...);
  if (r.has_results())
  { ... }

The :func:`has_result` call will implicitly wait until ``r`` is completed.

.. function:: bool Reply::has_results()

   Method :func:`!has_results` returns true if there are result sets included in the
   reply. To access these result sets one has to create a cursor. The exact way of
   creating cursors for result sets of a reply is defined by implementation. Once a
   cursor is created for the first result set, this result set can be "consumed" so
   that it is no longer accessible. In that case :func:`!has_results()` informs if
   there are more result sets left in the reply. In other words,
   :func:`!has_results()` informs about result sets that can be still "consumed"
   and if it returns false it means that all of them have been processed.

.. function:: void Reply::skip_result()

   Skip a result set (if reply has one) without creating a cursor for it (and
   thus avoiding allocation of cursor resources). If reply has several result sets
   then the next one becomes available.

.. function:: bool Reply::has_out_params()
              void Reply::get_out_params(Out_param_processor)

   Method :func:`!has_out_params` informs if this reply contains values for output
   parameters of a stored routine call. The values of the output parameters can be
   processed with a given processor by calling :func:`!get_out_params` method.

.. function:: row_count_t Reply::affected_rows()

   Inform about how many rows have been affected by the operation. This is
   overall number - more detailed information can be obtained with
   :func:`get_statistics` (if supported by the implementation).

.. function:: Op_statistics Reply::get_statistics()

   Get detailed statistics about data changes introduced by the operation.
   Returned object is an iterator over entries which describe changes in
   individual tables (see below).

.. function:: Op_time_info  Reply::get_time_info()

   Get timing information for the operation (see below).

.. function:: void Reply::discard()

   Discard the reply freeing all allocated resources before the reply
   object is destroyed. Related objects such as cursors created for this reply are
   also freed. Using Reply instance after calling :func:`!discard` on it throws
   an error. If there are cursors for result-sets from this reply and these
   cursors are not closed then :func:`discard` throws error.


.. Note:: Reply interface does not have method to count result sets contained in the
  reply or access them by their position. This is because underlying protocol can
  make it impossible to know the number of result sets in advance - for example if
  result sets are "streamed" one after another.


The :class:`Op_statistics` interface used to get detailed info about data changes
introduced by an operation extends :class:`Iterator` interface with the following methods
for describing data changes in a single table:

.. function:: Table_ref Op_statistics::table()

  Returns the table affected by the operation.

.. function:: enum Op_statistics::table_op()

   Informs if the table was dropped or created by the operation. The returned
   enumeration is: ``DROPPED``, ``CREATED``, ``NONE``.

.. function:: row_count_t Op_statistics::rows_scanned()
              row_count_t Op_statistics::rows_updated()
              row_count_t Op_statistics::rows_deleted()
              row_count_t Op_statistics::rows_inserted()

  Inform about number of rows scanned/updated/deleted/inserted in this table
  by the operation. If table was dropped these functions return 0.


Information about timing of operation execution in the store can be obtained
from object returned by :func:`Reply::get_time_info`. This object implements the
following ``Op_time_info`` interface:

.. function:: timestamp_t Op_time_info::time_received()

  Time when operation was received by data store.

.. function:: timestamp_t Op_time_info::time_started()
              timestamp_t Op_time_info::time_completed()

  Times when execution of the operation in the store started and ended.


.. _Batch_reply:

Interface Batch_reply
---------------------
Interface :class:`Batch_reply` allows iterating over a sequence of replies from an
execution of a batch of commands. It extends :class:`Reply` interface which can be
used to examine the current reply and :class:`Iterator` interface which is used
to move through the sequence of replies.

:class:`Batch_reply` extends :class:`Async_op` interface (via :class:`Reply` one).
When batch reply object first reaches "ready" state, it means that the first reply
in the sequence is available. Upon :func:`Batch_reply::next` call, the current reply
is discarded and one should wait for the batch reply object to become "ready"
again, before processing the next reply. However, implicit waits will be done
by methods from :class:`Reply` interface if needed.

It is not assumed that all commands in the batch generated a reply. For that
reason Batch_reply interface defines a method that tells which command the
current reply is for:

.. function:: batch_pos_t  Batch_reply::reply_for()

  Informs which command in the batch the current reply is for. Positions of
  commands in the batch are 0-based.


.. _Cursor:

Interface Cursor
----------------
A cursor can be used to iterate over rows of a result set. It is defined by
implementation how to create a cursor instance for iterating over a result set
from a given reply object. For example, cursor object can be constructed from
a reply object.

Rows from the result set can be fetched and passed to a row processor defined by
user code. Depending on implementation, it can be possible to move/set the
current position of the cursor. :class:`Cursor` provides meta-data information about
columns of the result set via :class:`Meta_data` interface which it extends
(see below).

Fetching rows from a cursor is an asynchronous operation - a call to
:func:`Cursor::get_rows` returns immediately and row data is passed to the row
processor asynchronously. This asynchronous operation is controlled via
:class:`Async_op` interface which :class:`Cursor` extends.

Handling a situation where new :func:`Cursor::get_rows` request is submitted
before previous one has completed is defined by the implementation: either
new :func:`!get_rows` waits for previous one to complete or it is added
to a queue of such requests to be handled when possible. If queuing is implemented
then :func:`Async_op::cancel` should clear the queue on top of cancelling
the currently active fetch operation.

.. function:: void Cursor::get_rows(Row_processor rp, row_count_t count)

   Fetch given amount of rows from the cursor and pass them to a row processor,
   one-by-one. This method returns immediately after starting an asynchronous
   operation that is controlled using methods from :class:`Async_op` interface.

.. function:: bool Cursor::get_row(Row_processor rp)

   Convenience method that calls ``get_rows(rp, 1)`` to fetch a single row, then
   waits for this operation to complete and then returns true if a row was fetched
   or false if there are no more rows in the cursor.

.. function:: void Cursor::close()

   Close cursor and free all resources before it is destroyed. Using the cursor
   after :func:`!close` throws an error.

.. function:: void Cursor::rewind()
              void Cursor::seek(enum from, \
                                row_count_t count =0, \
                                enum direction =FORWARD)

   Method :func:`!seek` changes current position within the cursor. Convenience
   method :func:`!rewind` is equivalent to ``seek(BEGIN)``. If current position
   of the cursor can not be changed then these methods should throw error.
   Possible starting positions for :func:`seek` are: ``BEGIN``, ``END`` and
   ``CURRENT``. Possible directions are: ``BACK`` and ``FORWARD``.


.. _Meta_data:

Interface Meta_data
-------------------
Methods of this interface provide information about columns of a result set or
fields of a row. The types used to represent value type information, serialization
format and other column characteristics are defined by implementation traits.

.. function:: field_pos_t Meta_data::col_count()

   Inform about number of columns in the result set.

.. function:: Type_info   Meta_data::type(field_pos_t pos)

   Describe type of values stored in the given column.

.. function:: Format_info Meta_data::format(field_pos_t pos)

   Describe format in which column values are serialized (the same format is
   used for all rows in the set).

.. function:: Column_info Meta_data::col_info(field_pos_t pos)

   Give other information about the column (if any).


Interface Out_param_processor : Row_processor
---------------------------------------------
For now it is assumed that values of output parameters are reported the same as
values of fields of a single row in a result set. See below for definition of
Row_processor interface.


Interface Row_processor
-----------------------
An object implementing :class:`Row_processor` interface is used to examine data from
a result set (via :func:`Cursor::get_rows` method).

.. function:: bool Row_processor::row_begin(row_count_t pos)
              void Row_processor::row_end(row_count_t pos)

   Methods called before and after processing single row. The ``pos`` parameter
   starts from 0 and is increased by 1 for each row processed in a single call to
   :func:`Cursor::get_rows` (note: it is not position within the cursor). If
   :func:`!row_begin` returns ``false`` then given row is skipped
   (no field data will be passed to the processor). If row is skipped then
   :func:`row_end` is not called for that row.

.. function:: size_t Row_processor::field_begin(field_pos_t pos)
              void Row_processor::field_end(field_pos_t pos)

   Called before and after processing one filed within a row. The ``pos``
   parameter indicates 0-based position of the field within the row. Method
   :func:`!field_begin` returns the amount of space available for storing field
   data - following :func:`field_data` calls should respect this limit. If 0 is
   returned then given field is skipped without calling :func:`!field_end` for it.
   The amount of available space can be adjusted by :func:`filed_data` method
   (see below).

.. function:: void Row_processor::field_null(field_pos_t pos)

   Called if given field is ``NULL``. Methods :func:`field_begin` and
   :func:`field_end` are not called in that case.

.. function:: size_t Row_processor::field_data(field_pos_t pos, bytes data)

   Called to pass data stored in a given field. This data can be sent in
   several chunks using several calls to :func:`!field_data` with the same field
   position. End of data is indicated by :func:`field_end` call. Method
   :func:`field_data` returns the currently available space for storing the data.
   The chunks of data passed via the following :func:`!field_data` calls should not
   exceed this space limit. If :func:`!field_data` returns 0 then it means that
   processor is not interested in seeing any more data for this field and remaining
   data (if any) will be discarded (followed by :func:`field_end` call)

.. function:: void Row_processor::end_of_data()

   Called when there are no more rows in the result set. Note that if a
   requested number of rows has been passed to row processor then this method
   is not called - it is called only if end of data is detected before passing
   the last of requested rows.


Listener interfaces
-------------------
.. todo:: Session_listener, Trx_listener, Reply_listener

