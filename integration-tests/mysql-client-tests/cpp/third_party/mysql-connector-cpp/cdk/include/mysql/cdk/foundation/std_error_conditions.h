/*
 * Copyright (c) 2015, 2018, Oracle and/or its affiliates. All rights reserved.
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

#ifndef CDK_FOUNDATION_STD_ERROR_CONDITIONS_H
#define CDK_FOUNDATION_STD_ERROR_CONDITIONS_H

#include <errno.h>

/*
  List taken from: http://en.cppreference.com/w/cpp/error/errc
*/

#define STD_COND_LIST(X) \
  X (address_family_not_supported,  EAFNOSUPPORT)     \
  X (address_in_use,                EADDRINUSE)       \
  X (address_not_available,         EADDRNOTAVAIL)    \
  X (already_connected,             EISCONN)          \
  X (argument_list_too_long,        E2BIG)            \
  X (argument_out_of_domain,        EDOM)             \
  X (bad_address,                   EFAULT)           \
  X (bad_file_descriptor,           EBADF)            \
  X (bad_message,                   EBADMSG)          \
  X (broken_pipe,                   EPIPE)            \
  X (connection_aborted,            ECONNABORTED)     \
  X (connection_already_in_progress, EALREADY)        \
  X (connection_refused,            ECONNREFUSED)     \
  X (connection_reset,              ECONNRESET)       \
  X (cross_device_link,             EXDEV)            \
  X (destination_address_required,  EDESTADDRREQ)     \
  X (device_or_resource_busy,       EBUSY)            \
  X (directory_not_empty,           ENOTEMPTY)        \
  X (executable_format_error,       ENOEXEC)          \
  X (file_exists,                   EEXIST)           \
  X (file_too_large,                EFBIG)            \
  X (filename_too_long,             ENAMETOOLONG)     \
  X (function_not_supported,        ENOSYS)           \
  X (host_unreachable,              EHOSTUNREACH)     \
  X (identifier_removed,            EIDRM)            \
  X (illegal_byte_sequence,         EILSEQ)           \
  X (inappropriate_io_control_operation, ENOTTY)      \
  X (interrupted,                   EINTR)            \
  X (invalid_argument,              EINVAL)           \
  X (invalid_seek,                  ESPIPE)           \
  X (io_error,                      EIO)              \
  X (is_a_directory,                EISDIR)           \
  X (message_size,                  EMSGSIZE)         \
  X (network_down,                  ENETDOWN)         \
  X (network_reset,                 ENETRESET)        \
  X (network_unreachable,           ENETUNREACH)      \
  X (no_buffer_space,               ENOBUFS)          \
  X (no_child_process,              ECHILD)           \
  X (no_link,                       ENOLINK)          \
  X (no_lock_available,             ENOLCK)           \
/*  X (no_message_available,          ENODATA)        */\
  X (no_message,                    ENOMSG)           \
  X (no_protocol_option,            ENOPROTOOPT)      \
  X (no_space_on_device,            ENOSPC)           \
/*  X (no_stream_resources,           ENOSR)          */\
  X (no_such_device_or_address,     ENXIO)            \
  X (no_such_device,                ENODEV)           \
  X (no_such_file_or_directory,     ENOENT)           \
  X (no_such_process,               ESRCH)            \
  X (not_a_directory,               ENOTDIR)          \
  X (not_a_socket,                  ENOTSOCK)         \
/*  X (not_a_stream,                  ENOSTR)         */\
  X (not_connected,                 ENOTCONN)         \
  X (not_enough_memory,             ENOMEM)           \
  X (not_supported,                 ENOTSUP)          \
  X (operation_canceled,            ECANCELED)        \
  X (operation_in_progress,         EINPROGRESS)      \
  X (operation_not_permitted,       EPERM)            \
  X (operation_not_supported,       EOPNOTSUPP)       \
  X (operation_would_block,         EWOULDBLOCK)      \
/*  X (owner_dead,                    EOWNERDEAD)     */\
  X (permission_denied,             EACCES)           \
  X (protocol_error,                EPROTO)           \
  X (protocol_not_supported,        EPROTONOSUPPORT)  \
  X (read_only_file_system,         EROFS)            \
  X (resource_deadlock_would_occur, EDEADLK)          \
  X (resource_unavailable_try_again, EAGAIN)          \
  X (result_out_of_range,           ERANGE)           \
/*  X (state_not_recoverable,         ENOTRECOVERABLE)*/\
/*  X (stream_timeout,                ETIME)          */\
  X (text_file_busy,                ETXTBSY)          \
  X (timed_out,                     ETIMEDOUT)        \
  X (too_many_files_open_in_system, ENFILE)           \
  X (too_many_files_open,           EMFILE)           \
  X (too_many_links,                EMLINK)           \
  X (too_many_symbolic_link_levels, ELOOP)            \
  X (value_too_large,               EOVERFLOW)        \
  X (wrong_protocol_type,           EPROTOTYPE)       \


namespace cdk {
namespace foundation {

#define STD_COND_ENUM(A,B)  A = B,

struct errc
{
  enum code {
    no_error = 0,
    STD_COND_LIST(STD_COND_ENUM)
  };
};

}}  // cdk::foundation


#endif
