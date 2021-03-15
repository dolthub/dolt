/*
 * Copyright (c) 2014, 2018, Oracle and/or its affiliates. All rights reserved.
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
 * which is part of <MySQL Product>, is also subject to the
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
#include "process_launcher.h"
#include "exception.h"

#include <string>
#include <sstream>


#ifdef WIN32
#  include <windows.h>
#  include <tchar.h>
#  include <stdio.h>
#else
#  include <stdio.h>
#  include <unistd.h>
#  include <sys/types.h>
#  include <stdlib.h>
#  include <string.h>
#  include <sys/wait.h>
#  include <string.h>
#  include <poll.h>
#  include <errno.h>
#  include <signal.h>
#  include <fcntl.h>
#endif


#ifdef LINUX
#  include <sys/prctl.h>
#endif


using namespace ngcommon;

#ifdef WIN32

void Process_launcher::start()
{
  SECURITY_ATTRIBUTES saAttr;

  saAttr.nLength = sizeof(SECURITY_ATTRIBUTES);
  saAttr.bInheritHandle = TRUE;
  saAttr.lpSecurityDescriptor = NULL;

  if (!CreatePipe(&child_out_rd, &child_out_wr, &saAttr, 0))
    report_error("Failed to create child_out_rd");

  if (!SetHandleInformation(child_out_rd, HANDLE_FLAG_INHERIT, 0))
    report_error("Failed to create child_out_rd");

  // force non blocking IO in Windows
  DWORD mode = PIPE_NOWAIT;
  //BOOL res = SetNamedPipeHandleState(child_out_rd, &mode, NULL, NULL);

  if (!CreatePipe(&child_in_rd, &child_in_wr, &saAttr, 0))
    report_error("Failed to create child_in_rd");

  if (!SetHandleInformation(child_in_wr, HANDLE_FLAG_INHERIT, 0))
    report_error("Failed to created child_in_wr");

  // Create Process
  std::string s = this->cmd_line;
  const char **pc = args;
  while (*++pc != NULL)
  {
    s += " ";
    s += *pc;
  }
  char *sz_cmd_line = ( char *)malloc(s.length() + 1);
  if (!sz_cmd_line)
    report_error("Cannot assign memory for command line in Process_launcher::start");
  _tcscpy(sz_cmd_line, s.c_str());

  BOOL bSuccess = FALSE;

  ZeroMemory(&pi, sizeof(PROCESS_INFORMATION));

  ZeroMemory(&si, sizeof(STARTUPINFO));
  si.cb = sizeof(STARTUPINFO);
  if (redirect_stderr)
    si.hStdError = child_out_wr;
  si.hStdOutput = child_out_wr;
  si.hStdInput = child_in_rd;
  si.dwFlags |= STARTF_USESTDHANDLES;

  bSuccess = CreateProcess(
    NULL,          // lpApplicationName
    sz_cmd_line,     // lpCommandLine
    NULL,          // lpProcessAttributes
    NULL,          // lpThreadAttributes
    TRUE,          // bInheritHandles
    0,             // dwCreationFlags
    NULL,          // lpEnvironment
    NULL,          // lpCurrentDirectory
    &si,           // lpStartupInfo
    &pi);          // lpProcessInformation

  if (!bSuccess)
    report_error(NULL);

  CloseHandle(child_out_wr);
  CloseHandle(child_in_rd);

  //DWORD res1 = WaitForInputIdle(pi.hProcess, 100);
  //res1 = WaitForSingleObject(pi.hThread, 100);
  free(sz_cmd_line);
}


uint64_t Process_launcher::get_pid()
{
  return (uint64_t)pi.hProcess;
}


int Process_launcher::wait()
{
  DWORD dwExit = 0;
  if (GetExitCodeProcess(pi.hProcess, &dwExit))
  {
    if (dwExit == STILL_ACTIVE)
    {
      WaitForSingleObject(pi.hProcess, INFINITE);
    }
  }
  else
  {
    DWORD dwError = GetLastError();
    if (dwError != ERROR_INVALID_HANDLE)  // not closed already?
      report_error(NULL);
  }
  return dwExit;
}


void Process_launcher::close()
{
  DWORD dwExit;
  if (GetExitCodeProcess(pi.hProcess, &dwExit))
  {
    if (dwExit == STILL_ACTIVE)
    {
      if (!TerminateProcess(pi.hProcess, 0))
        report_error(NULL);
      // TerminateProcess is async, wait for process to end.
      WaitForSingleObject(pi.hProcess, INFINITE);
    }
  }
  else
  {
    report_error(NULL);
  }

  if (!CloseHandle(pi.hProcess))
    report_error(NULL);
  if (!CloseHandle(pi.hThread))
    report_error(NULL);

  if (!CloseHandle(child_out_rd))
    report_error(NULL);
  if (!CloseHandle(child_in_wr))
    report_error(NULL);

  is_alive = false;
}


int Process_launcher::read_one_char()
{
  char buf[1];
  BOOL bSuccess = FALSE;
  DWORD dwBytesRead, dwCode;

  while (!(bSuccess = ReadFile(child_out_rd, buf, 1, &dwBytesRead, NULL)))
  {
    dwCode = GetLastError();
    if (dwCode == ERROR_NO_DATA) continue;
    if (dwCode == ERROR_BROKEN_PIPE)
      return EOF;
    else
      report_error(NULL);
  }

  return buf[0];
}

int Process_launcher::read(char *buf, size_t count)
{
  BOOL bSuccess = FALSE;
  DWORD dwBytesRead, dwCode;
  int i = 0;

  while (!(bSuccess = ReadFile(child_out_rd, buf, count, &dwBytesRead, NULL)))
  {
    dwCode = GetLastError();
    if (dwCode == ERROR_NO_DATA) continue;
    if (dwCode == ERROR_BROKEN_PIPE)
      return EOF;
    else
      report_error(NULL);
  }

  return dwBytesRead;
}


int Process_launcher::write_one_char(int c)
{
  CHAR buf[1];
  BOOL bSuccess = FALSE;
  DWORD dwBytesWritten;

  bSuccess = WriteFile(child_in_wr, buf, 1, &dwBytesWritten, NULL);
  if (!bSuccess)
  {
    if (GetLastError() != ERROR_NO_DATA)  // otherwise child process just died.
      report_error(NULL);
  }
  else
  {
    return 1;
  }
  return 0; // so the compiler does not cry
}


int Process_launcher::write(const char *buf, size_t count)
{
  DWORD dwBytesWritten;
  BOOL bSuccess = FALSE;
  bSuccess = WriteFile(child_in_wr, buf, count, &dwBytesWritten, NULL);
  if (!bSuccess)
  {
    if (GetLastError() != ERROR_NO_DATA)  // otherwise child process just died.
      report_error(NULL);
  }
  else
  {
    // When child input buffer is full, this returns zero in NO_WAIT mode.
    return dwBytesWritten;
  }
  return 0; // so the compiler does not cry
}


void Process_launcher::report_error(const char *msg)
{
  DWORD dwCode = GetLastError();
  LPTSTR lpMsgBuf;

  if (msg != NULL)
  {
    throw Exception::runtime_error(msg);
  }
  else
  {
    FormatMessage(
      FORMAT_MESSAGE_ALLOCATE_BUFFER |
      FORMAT_MESSAGE_FROM_SYSTEM |
      FORMAT_MESSAGE_IGNORE_INSERTS,
      NULL,
      dwCode,
      MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
      (LPTSTR)&lpMsgBuf,
      0, NULL);
    std::ostringstream msgerr;
    msgerr << "SystemError: ";
    msgerr << lpMsgBuf;
    msgerr << " with error code " << dwCode << ".";
    throw Exception::runtime_error(msgerr.str());
  }
}


uint64_t Process_launcher::get_fd_write()
{
  return (uint64_t)child_in_wr;
}


uint64_t Process_launcher::get_fd_read()
{
  return (uint64_t)child_out_rd;
}


#else

void Process_launcher::start()
{
  if( pipe(fd_in) < 0 )
  {
    report_error(NULL);
  }
  if( pipe(fd_out) < 0 )
  {
    report_error(NULL);
  }

  // Ignore broken pipe signal
  signal(SIGPIPE, SIG_IGN);

  childpid = fork();
  if(childpid == -1)
  {
    report_error(NULL);
  }

  if(childpid == 0)
  {
#ifdef LINUX
    prctl(PR_SET_PDEATHSIG, SIGHUP);
#endif

    ::close(fd_out[0]);
    ::close(fd_in[1]);
    while( dup2(fd_out[1], STDOUT_FILENO) == -1 )
    {
      if(errno == EINTR) continue;
      else report_error(NULL);
    }

    if(redirect_stderr)
    {
      while( dup2(fd_out[1], STDERR_FILENO) == -1 )
      {
        if(errno == EINTR) continue;
        else report_error(NULL);
      }
    }
    while( dup2(fd_in[0], STDIN_FILENO) == -1 )
    {
      if(errno == EINTR) continue;
      else report_error(NULL);
    }

    fcntl(fd_out[1], F_SETFD, FD_CLOEXEC);
    fcntl(fd_in[0], F_SETFD, FD_CLOEXEC);

    execvp(cmd_line, (char * const *)args);
    // if exec returns, there is an error.
    // TODO: Use explain_execvp if available
    exit(errno);
  }
  else
  {
//    int status;

    ::close(fd_out[1]);
    ::close(fd_in[0]);
    /*
    _s_pollfd[0].fd = fd_out[0];
    _s_pollfd[0].events = POLLIN;
    _s_pollfd[1].fd = fd_in[1];
    _s_pollfd[1].events = POLLOUT;
    */
  }
}


void Process_launcher::close()
{
  if(::kill(childpid, SIGTERM) < 0 && errno != ESRCH)
    report_error(NULL);
  if(errno != ESRCH)
  {
    sleep(1);
    if(::kill(childpid, SIGKILL) < 0 && errno != ESRCH)
      report_error(NULL);
  }

  /*
  while(::close(fd_out[0]) == -1)
  {
    if(errno == EINTR) continue;
    else report_error(NULL);
  }

  while(::close(fd_in[1]) == -1)
  {
    if( errno == EINTR ) continue;
    else report_error(NULL);
  }*/

  ::close(fd_out[0]);
  ::close(fd_in[1]);
  is_alive = false;
}


int Process_launcher::read_one_char()
{
  int c;
  do
  {
    if((c = ::read(fd_out[0], &c, 1)) >= 0)
      return c;
    if(errno == EAGAIN) continue;
    if(errno == EPIPE) return 0;
    if(errno)
      report_error(NULL);
  } while(true);
  return 0;
}


int Process_launcher::read(char *buf, size_t count)
{
  int n;
  do {
    if((n = ::read(fd_out[0], buf, count)) >= 0)
      return n;
    if(errno == EAGAIN) continue;
    if(errno == EPIPE) return 0;
    if(errno)
      report_error(NULL);
  } while(true);
  return 0;
}


int Process_launcher::write_one_char(int c)
{
  if(::write(fd_in[1], &c, 1) < 0)
    return 1;
  if(errno == EPIPE) return 0;
  report_error(NULL);
  return 0;
}


int Process_launcher::write(const char *buf, size_t count)
{
  int n;
  if((n = ::write(fd_in[1], buf, count)) >= 0)
    return n;
  if(errno == EPIPE) return 0;
  report_error(NULL);
  return 0;
}


void Process_launcher::report_error(const char *msg)
{
  char sys_err[ 64 ];
  int errnum = errno;
  if(msg == NULL)
  {
    strerror_r(errno, sys_err, sizeof(sys_err));
    std::ostringstream msgerr;
    msgerr << "SystemError: ";
    msgerr << sys_err;
    msgerr << " with error code " << errnum << ".";
    throw Exception::runtime_error(msgerr.str());
  }
  else
  {
    throw Exception::runtime_error(msg);
  }
}


uint64_t Process_launcher::get_pid()
{
  return (uint64_t)childpid;
}


/*
 * Waits for the child process to finish.
 * throws an error if the wait fails, or the child return code if wait syscall does not fail.
 */
int Process_launcher::wait()
{
  int status;
  int exited;
  int exitstatus;
  pid_t ret;

  do
  {
    ret = ::wait(&status);
    exited = WIFEXITED(status);
    exitstatus = WEXITSTATUS(status);
    if(ret == -1)
    {
      if(errno == ECHILD) break;  // no children left
      if((exited == 0) || (exitstatus != 0))
      {
        report_error(NULL);
      }
    }
  }
  while(ret == -1);

  return exitstatus;
}


uint64_t Process_launcher::get_fd_write()
{
  return (uint64_t)fd_in[1];
}


uint64_t Process_launcher::get_fd_read()
{
  return (uint64_t)fd_out[0];
}


#endif

void Process_launcher::kill()
{
  close();
}
