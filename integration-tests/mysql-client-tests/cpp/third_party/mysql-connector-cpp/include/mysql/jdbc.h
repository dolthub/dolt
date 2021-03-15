/*
 * Copyright (c) 2018, 2019, Oracle and/or its affiliates. All rights reserved.
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

#include "../jdbc/mysql_connection.h"
#include "../jdbc/mysql_driver.h"
#include "../jdbc/mysql_error.h"
#include "../jdbc/cppconn/build_config.h"
#include "../jdbc/cppconn/config.h"
#include "../jdbc/cppconn/connection.h"
#include "../jdbc/cppconn/datatype.h"
#include "../jdbc/cppconn/driver.h"
#include "../jdbc/cppconn/exception.h"
#include "../jdbc/cppconn/metadata.h"
#include "../jdbc/cppconn/parameter_metadata.h"
#include "../jdbc/cppconn/prepared_statement.h"
#include "../jdbc/cppconn/resultset.h"
#include "../jdbc/cppconn/resultset_metadata.h"
#include "../jdbc/cppconn/statement.h"
#include "../jdbc/cppconn/sqlstring.h"
#include "../jdbc/cppconn/warning.h"
#include "../jdbc/cppconn/version_info.h"
#include "../jdbc/cppconn/variant.h"
