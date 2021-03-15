/*
 * Copyright (c) 2015, 2019, Oracle and/or its affiliates. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0, as
 * published by the Free Software Foundation.
 *
 * This program is also distributed with certain software (including
 * but not limited to OpenSSL) that is licensed under separate terms,
 * as designated in a particular file or component or in included license
 * documentation.  The authors of MySQL hereby grant you an
 * additional permission to link the program and your derivative works
 * with the separately licensed software that they have included with
 * MySQL.
 *
 * Without limiting anything contained in the foregoing, this file,
 * which is part of MySQL Connector/C++, is also subject to the
 * Universal FOSS Exception, version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

#ifndef MYSQL_COMMON_CONSTANTS_H
#define MYSQL_COMMON_CONSTANTS_H

#define DEFAULT_MYSQL_PORT  3306
#define DEFAULT_MYSQLX_PORT 33060

// ----------------------------------------------------------------------------

/*
  Common constants
  ================

  Warning: Values of these constants are part of the public API. Changing them
  is a non backward compatible API change.

  Note: Value of 0 is reserved for special uses and thus constant values
  are always > 0.

  Note: the empty END_LIST macro at the end of list macros helps Doxygen
  correctly interpret documentation for the list item.
*/

#define OPT_STR(X,Y,N) X##_str(Y,N)
#define OPT_BOOL(X,Y,N) X##_bool(Y,N)
#define OPT_NUM(X,Y,N) X##_num(Y,N)
#define OPT_ANY(X,Y,N) X##_any(Y,N)


#undef END_LIST
#define END_LIST


#define CLIENT_OPTION_LIST(x)                                                  \
  OPT_BOOL(x,POOLING,1) /*!< disable/enable the pool. (Enabled by default)*/   \
  OPT_NUM(x,POOL_MAX_SIZE,2) /*!< size of the pool. (Defaults to 25)*/         \
  OPT_NUM(x,POOL_QUEUE_TIMEOUT,3) /*!< timeout for waiting for a connection in
  the pool (ms). (No timeout by default)*/                                     \
  OPT_NUM(x,POOL_MAX_IDLE_TIME,4)/*!< time for a connection to be in the pool
  without being used (ms).(Will not expire by default)*/                       \
  END_LIST


#define SESSION_OPTION_LIST(x)                                               \
  OPT_STR(x,URI,1)         /*!< connection URI or string */                  \
  /*! DNS name of the host, IPv4 address or IPv6 address */                  \
  OPT_STR(x,HOST,2)                                                          \
  OPT_NUM(x,PORT,3)        /*!< X Plugin port to connect to */               \
  /*!
    Assign a priority (a number in range 1 to 100) to the last specified
    host; these priorities are used to determine the order in which multiple
    hosts are tried by the connection fail-over logic (see description
    of `Session` class)
  */                                                                         \
  OPT_NUM(x,PRIORITY,4)                                                      \
  OPT_STR(x,USER,5)        /*!< user name */                                 \
  OPT_STR(x,PWD,6)         /*!< password */                                  \
  OPT_STR(x,DB,7)          /*!< default database */                          \
  /*!
    Specify \ref SSLMode option to be used. In plain C code the value
    should be a `#mysqlx_ssl_mode_t` enum constant.
  */ \
  OPT_ANY(x,SSL_MODE,8)                                                      \
  /*! path to a PEM file specifying trusted root certificates*/              \
  OPT_STR(x,SSL_CA,9)                                                        \
  /*!
    Authentication method to use, see \ref AuthMethod. In plain C code the value
    should be a `#mysqlx_auth_method_t` enum constant.
  */ \
  OPT_ANY(x,AUTH,10)                                                        \
  OPT_STR(x,SOCKET,11)    /*!< unix socket path */                          \
  /*!
    Sets connection timeout in milliseconds. In C++ code can be also set to
    a `std::chrono::duration` value.
  */ \
  OPT_NUM(x,CONNECT_TIMEOUT,12)                                              \
  /*!
    Specifies connection attributes (key-value pairs) to be sent when a session
    is created. The value is a JSON string (in C++ code can be also a `DbDoc`
    object) defining additional attributes to be sent on top of the default
    ones. Setting this option to `false` (in C++ code) or NULL (in plain C code)
    disables sending any connection attributes (including the default ones).
    Setting it to `true` (in C++ code) or empty string (in plain C code)
    requests sending only the default attributes which is also the default
    behavior when this option is not set.
  */ \
  OPT_STR(x,CONNECTION_ATTRIBUTES,13)                                        \
  /*!
    List of allowed TLS protocol versions, such as "TLSv1.2". The value is a
    string with comma separated versions. In C++ code it can also be an
    iterable container with versions.
  */                                                                         \
  OPT_STR(x,TLS_VERSIONS, 14)                                                \
  /*!
    List of allowed TLS cipher suites. The value is a string with
    comma separated IANA cipher suitenames (such as
    "TLS_RSA_WITH_3DES_EDE_CBC_SHA"). In C++ code it can also be an iterable
    container with names.
    Unknown cipher suites are silently ignored.
  */                                                                         \
  OPT_STR(x,TLS_CIPHERSUITES, 15)                                            \
  /*!
    If enabled (true) will check hostname for DNS SRV record and use its
    configuration (hostname, port, priority and weight) to connect.
  */                                                                        \
  OPT_BOOL(x, DNS_SRV, 16)                                                  \
  OPT_ANY(x,COMPRESSION,17) /*!< enable or disable compression */
  END_LIST


/*
  Names for options supported in the query part of a connection string and
  how they map to session options above.

  Note: when adding new options to this list, also update doxygen docs
  for mysqlx::SessionSettings URL ctor (include\mysqlx\devapi\settings.h) and
  for mysqlx_get_session_from_url() (include\mysqlx\xapi.h).
*/

#define URI_OPTION_LIST(X)  \
  X("ssl-mode", SSL_MODE)   \
  X("ssl-ca", SSL_CA)       \
  X("auth", AUTH)           \
  X("connect-timeout", CONNECT_TIMEOUT) \
  X("connection-attributes",CONNECTION_ATTRIBUTES)\
  X("tls-versions", TLS_VERSIONS) \
  X("tls-ciphersuites", TLS_CIPHERSUITES) \
  X("compression", COMPRESSION) \
  END_LIST


#define SSL_MODE_LIST(x) \
  x(DISABLED,1)        /*!< Establish an unencrypted connection.  */ \
  x(REQUIRED,2)        /*!< Establish a secure connection if the server supports
                          secure connections. The connection attempt fails if a
                          secure connection cannot be established. This is the
                          default if `SSL_MODE` is not specified. */ \
  x(VERIFY_CA,3)       /*!< Like `REQUIRED`, but additionally verify the server
                          TLS certificate against the configured Certificate
                          Authority (CA) certificates (defined by `SSL_CA`
                          Option). The connection attempt fails if no valid
                          matching CA certificates are found.*/ \
  x(VERIFY_IDENTITY,4) /*!< Like `VERIFY_CA`, but additionally verify that the
                          server certificate matches the host to which the
                          connection is attempted.*/\
  END_LIST


#define AUTH_METHOD_LIST(x)\
  x(PLAIN,1)       /*!< Plain text authentication method. The password is
                      sent as a clear text. This method is used by
                      default in encrypted connections. */ \
  x(MYSQL41,2)     /*!< Authentication method supported by MySQL 4.1 and newer.
                      The password is hashed before being sent to the server.
                      This authentication method works over unencrypted
                      connections */ \
  x(EXTERNAL,3)    /*!< External authentication when the server establishes
                      the user authenticity by other means such as SSL/x509
                      certificates. Currently not supported by X Plugin */ \
  x(SHA256_MEMORY,4)  /*!< Authentication using SHA256 password hashes stored in
                         server-side cache. This authentication method works
                         over unencrypted connections.
                      */ \
  END_LIST

/*
  Types that can be reported by MySQL server.
*/

#define RESULT_TYPE_LIST(X) \
  X(BIT,        1)   \
  X(TINYINT,    2)   \
  X(SMALLINT,   3)   \
  X(MEDIUMINT,  4)   \
  X(INT,        5)   \
  X(BIGINT,     6)   \
  X(FLOAT,      7)   \
  X(DECIMAL,    8)   \
  X(DOUBLE,     9)   \
  X(JSON,       10)  \
  X(STRING,     11)  \
  X(BYTES,      12)  \
  X(TIME,       13)  \
  X(DATE,       14)  \
  X(DATETIME,   15)  \
  X(TIMESTAMP,  16)  \
  X(SET,        17)  \
  X(ENUM,       18)  \
  X(GEOMETRY,   19)  \
  END_LIST


/*
  Check options for an updatable view.
  @see https://dev.mysql.com/doc/refman/en/view-check-option.html
*/

#define VIEW_CHECK_OPTION_LIST(x) \
  x(CASCADED,1) \
  x(LOCAL,2) \
  END_LIST

/*
  Algorithms used to process views.
  @see https://dev.mysql.com/doc/refman/en/view-algorithms.html
*/

#define VIEW_ALGORITHM_LIST(x) \
  x(UNDEFINED,1) \
  x(MERGE,2) \
  x(TEMPTABLE,3) \
  END_LIST

/*
  View security settings.
  @see https://dev.mysql.com/doc/refman/en/stored-programs-security.html
*/

#define VIEW_SECURITY_LIST(x) \
  x(DEFINER,1) \
  x(INVOKER,2) \
  END_LIST


#define LOCK_MODE_LIST(X) \
  X(SHARED,1)    /*!< Sets a shared mode lock on any rows that
                      are read. Other sessions can read the rows,
                      but cannot modify them until your transaction
                      commits. If any of these rows were changed by
                      another transaction that has not yet committed,
                      your query waits until that transaction ends
                      and then uses the latest values. */ \
  X(EXCLUSIVE,2) /*!< For index records the search encounters,
                      locks the rows and any associated index entries, the same
                      as if you issued an UPDATE statement for those rows. Other
                      transactions are blocked from updating those rows,
                      from doing locking in LOCK_SHARED, or from reading
                      the data in certain transaction isolation levels. */ \
  END_LIST

#define LOCK_CONTENTION_LIST(X) \
  X(DEFAULT,0) /*!< Block query until existing row locks are released.  */ \
  X(NOWAIT,1) /*!< Return error if lock could not be obtained immediately.  */ \
  X(SKIP_LOCKED,2) /*!< Execute query immediately, excluding items that are
                        locked from the query results.  */ \
  END_LIST

#define COMPRESSION_MODE_LIST(x) \
  x(DISABLED,1)        /*!< Disables the compression.  */ \
  x(PREFERRED,2)       /*!< Request compression, but not return error
                         if compression is requested, but could not be 
                         used */ \
  x(REQUIRED,3)        /*!< Request compression and return error if
                         compression is not supported by the server */ \
  END_LIST

// ----------------------------------------------------------------------------


#define COLLECTION_OPTIONS_OPTION(X)\
  X(REUSE,1) /*!< Use existing collection. Expects boolean value
                  @anchor OPT_COLLECTION_REUSE */ \
  X(VALIDATION,2) /*!< Collection validation options. Expects
                       CollectionValidation or a json string.*/ \
  END_LIST

#define COLLECTION_VALIDATION_OPTION(X)\
  X(SCHEMA,1) /*!< Collection validation schema, as defined by
                    https://dev.mysql.com/doc/refman/8.0/en/json-validation-functions.html#function_json-schema-valid
                */ \
  X(LEVEL,2) /*!< Defines level of validation on the collection, see
                  \ref CollectionValidation_Level "CollectionValidation::Level".
                  In plain C code the value should be
                  \ref opt_collection_validation_level "mysqlx_collection_validation_level_t".
                  */ \
  END_LIST


  // Schema Validation Level

//Windows defines STRICT as a macro... undefine it
#ifdef STRICT
  #undef STRICT
#endif

#define COLLECTION_VALIDATION_LEVEL(X)\
  X(OFF,1) /*!< No validation will be done on the collection. */ \
  X(STRICT,2) /*!< All collection documents have to comply to validation schema.
               */ \
  END_LIST



#endif
