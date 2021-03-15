.. Client Development Kit documentation master file, created by
   sphinx-quickstart on Tue Feb  3 13:14:22 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Client Development Kit's documentation!
==================================================

Client SDK (aka CDK) is a foundation library for building C/C++ based connectors.
It abstracts-away the details of communication with data store and the protocol
used for this communication. A connector built on top of CDK should be able to
support different communication media or different protocols for free.

The functionality of Client SDK is exposed via CDK API consisting of a set of
types, classes and templates. This API is not intended to be visible to the
final users and it does not cover all the aspects handled by typical connector
(for example, type conversions are not handled by CDK). Instead, a connector
built over CDK has to implement connector-specific API calls using CDK objects.

The basic functionalities of CDK are provided via :class:`Session`, :class:`Reply`
and :class:`Cursor` classes. CDK provides transparent resource management using
RAII pattern and support for asynchronous operations.

Synopsis
--------

.. note:: This synopsis is just a vision where we are heading. Not everything
  is implemented yet and things might change as we go. See usage section for
  examples of code that works with current implementation of CDK.

::

  #include <mysql/cdk.h>

  using namespace cdk;

Create data source instance representing a MySQL server
accessed via TCP/IP connection using MySQL X protocol. Constructor
parameters determine address, port etc.

::

  ds::TCPIP   ds(...);

Create a session for the data source

::

  ds::Options opt(user, password);
  opt.set_default_schema("test");

  Session  s(ds, opt);

Use SQL API to create test table. Reply to a query is an asynchronous object -
we can wait for it to ensure that reply was received and is available. The API
makes implicit waits if required.

::

  Reply r;
  r= s.sql("DROP TABLE IF EXISTS t1");
  r= s.sql("CREATE TABLE t1(id INT, doc JSON)");
  r.wait();

Use CRUD api to insert and query data. Row source object ``rs`` defines data
to be inserted into the table.

::

  Row_source rs(...);
  Table_ref  t("test", "t1");

  r= s.insert(t, rs);
  r.wait();

Reply object gives some basic information about the result of the operation.

::

  cout <<"Affected rows: " <<r.affected_rows() <<endl;
  if (r.has_errors())
  {
    cout <<"Errors during insert operation:" <<endl;
    // print error information
  }

To query data in a table one can create filter object which describes
select criteria. The criteria is defined by the type of the filter object
and constructor parameters.

::

  Filter filter(...);
  r= s.find(t, select);

A result of ``s.find()`` query contains rows - to access them one
needs to create a cursor and define a row processor object which will
handle data from the cursor

::

  Cursor c(r);
  Row_processor rp(...);

  while (c.next_row(rp))
  {
    // Access BLOB data in the row - treat it as description of
    // a document (note: document handling is outside of SDK scope).

    Blob b(c, pos);
    Document doc(b);
    cout <<doc <<endl;
  }


Usage
-----

.. toctree::
  :maxdepth: 2

  usage.rst

Designs
-------

.. toctree::
   :maxdepth: 2

   designs/general.rst
   designs/session_mysqlx.rst
   designs/core_api.rst
   designs/foundation.rst
   designs/errors.rst
   designs/protocol.rst

Implementation
--------------

.. toctree::
   :maxdepth: 2

   coding.rst
   ms1.rst

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

