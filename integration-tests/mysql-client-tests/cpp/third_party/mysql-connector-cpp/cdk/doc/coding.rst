=======================
 CDK code organization
=======================

CDK main code repository is at ``git@myrepo:cdkbase``. Structure of the repository
is as follows:

:cmake:    Local modules used by cmake.

:common:   Common sources shared by all ng components.

  :protocol:  Protobuf definitions for new protocol packets.

:doc:  Project documentation. Specifications are inside ``designs`` sub-folder.

:include:  Public CDK headers. All headers are inside ``mysql/cdk`` folder so that
  if this location is in include path then includes have form::

  #include <mysql/cdk/*.h>

  The ``mysql`` sub-folder contains top-level ``cdk.h`` to be used like this::

  #include <mysql/cdk.h>

  This top-level include file should include all core CDK components. Similar,
  folder ``mysql/cdk`` contains file ``foundation.h`` which should be used as::

  #include <mysql/cdk/foundation.h>

  and should include individual public headers from ``mysql/cdk/foundation``
  folder.

  Folder ``mysql/cdk/foundation`` contains public headers for CDK foundation
  such as:

  :types.h:  Base types definitions such as ``byte``, ``buffers``, ``string`` etc.
  :error.h:  Error infrastructure.
  :async.h:  ``Async_op`` and friends.
  :stream.h: I/O API.

:foundation:  Implementation of CDK Foundation classes. Unit tests are inside
  ``tests`` sub-folder.

:protocol:  Implementation of protocol classes. Each concrete protocol has own
  sub-folder (currently only ``mysqlx``):

  :test:   Test protocol based on mongodb.
  :mysqlx: The new protocol.
  :mysql:  Old MySQL protocol.

:core:  Infrastructure for core CDK API (``Session`` and friends).

:mysqlx:  Implementation of MySQLX Session class.

:mysql:  Implementation of CDK over the old protocol (not now).


Public and local headers
========================
All public headers, and only public headers, are stored in ``include/mysql/cdk`` folder.
These headers should be self-contained: each header should compile on its own. Headers
should try to include as little as possible and avoid including complete type/class
definitions if possible (to use type ``X&`` or ``X*`` it is enough to declare ``class X;``
without full definition of class ``X``).

Headers from ``include/mysql/cdk`` are for end user application code, not for our own
implementation code (but of course, our implementation files need to include them). Any
local implementation headers should be stored together with implementation files (that is,
for example, in ``foundation/`` and not in ``include/mysql/cdk/foundation/``).

When including local headers, use the form::

#include "header.h"
#include "../foo/foo.h"

The paths are relative to the location of the source file that includes them. When
including public headers, use the form::

#include <mysql/cdk/foo/foo.h>

.. note:: An alternative would be to include our public headers like this::

  #include "../include/mysql/cdk/foo/foo.h"

  This way, we would have guarantee that headers from the same source tree as
  implementation files are always used, regardless of how include path is set.
  But then moving implementation files around would be a problem.

Headers are guarded agains double inclusion with macros of the form::

  #ifndef CDK_XXX_YYY_H
  #define CDK_XXX_YYY_H
  ...
  #endif

for include header named <mysql/cdk/xxx/yyy.h>

Dependencies on external headers
--------------------------------
Public CDK headers should not include any third-party library headers to avoid ABI
compatibility problems. Such third-party library headers should be included only
from implementation files and all implementation-specific data should be hidden from
public headers (for example, using ``opaque_impl<>`` template).

Public headers check
--------------------
Project includes target ``Headers`` which checks that all public headers are
self contained. This target compiles auto-generated code that includes each public
header alone.

For this to work, public headers must be declared inside ``CMakeLists.txt`` files
using :func:`ADD_HEADERS` and :func:`ADD_HEADERS_DIR` macros (see
``include/mysql/cdk/CMakeLists.txt``). The ``Headers`` target is defined by
:func:`ADD_HEADERS_TARGET` macro (see ``include/CMakeLists.txt``). This infrastructure
is defined in ``cmake/headers.cmake``.


Namespaces
==========
All CDK code is in ``cdk`` global namespace. There are sub-namespaces for individual
components:

:cdk\:\:foundation:     For foundation code.
:cdk\:\:protocol\:\:xxx:  For protocol ``xxx`` implementation,
  for example ``cdk::protocol::mysqlx::Protocol``.
:cdk\:\:xxx:            CDK implementation over protocol ``xxx``,
  for example ``cdk::mysqlx::Session``.

The core classes such as ``Session`` live in the top-level ``cdk`` namespace. Code can
use further sub-namespaces inside each of these to avoid name clashes. Like
``cdk::api::Session`` for session API, to not clash with ``cdk::Session`` which is an
implementation of this API.

Unit tests
==========

Any code added to the repo should have a unit tests that uses that code.

Unit tests for each component are defined in ``tests`` sub-folder of that component
folder, for example ``foundation/tests``. Source files containing unit tests should
be declared using cmake macro::

  ADD_NG_TESTS(<list of source files>)

We use gtest framework to define tests. If unit tests depend on other libraries, such
as Boost, these libraries should be declared with::

  ADD_TEST_LIBRARIES(<list of libraries>)

Additional include directories required by unit tests should be declared with::

  ADD_TEST_INCLUDES(<list of include paths>)

All unit tests can be run using ``run_unit_tests`` app built by the project.

CTest integration
-----------------

Project defines target ``update_test_groups`` which generates ``TestGroups.cmake`` file
which defines ``CTest`` tests for each unit test group defined by unit test sources. For
example, all unit tests in ``Framework`` group will be defined as single ``CTest`` entry.
The standard ``run_tests`` target generated by ``CTest`` runs these test groups defined
by ``TestGroups.cmake`` file (which is generated in build location).

Generated file ``TestGroups.cmake`` is included from project's cmake files. After running
``update_test_groups`` target one has to run ``cmake`` again for test group definitions
to be addded to the project.

Other coding guidelines
=======================

- We use general NG coding guidelines:
  https://stbeehive.oracle.com/teamcollab/wiki/MySQLng:Coding+Guidelines

- Unless really needed, all classes should disable copy constructor.

- In public headers declare only public interface, implementation details should
  be defined only in implementation files.

- Hide class members as much as possible - be conscious whether a member should be
  ``public`` or ``protected`` - assume private as default and change only if needed.
  The same goes with inheritance - use public one only if really needed (which most
  often is the case, but not always).

- Remember that classes intended to be extended and which need a destructor, should
  define a virtual one.
