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

/*
  Implementation of mysqlx protocol API
  =====================================
*/

// Note: on Windows this includes windows.h

#include <mysql/cdk/foundation/common.h>
#include <google/protobuf/io/zero_copy_stream.h>

/*
  Note: On Windows the INIT_ONCE structure was added only in later
  releases but can be available in previous ones via service packs.
*/

#ifdef _WIN32
  static INIT_ONCE log_handler_once;
  #define EXECUTE_ONCE(A, B) InitOnceExecuteOnce(A, B, NULL, NULL)
#else
  #include <pthread.h>
  pthread_once_t log_handler_once = PTHREAD_ONCE_INIT;
  #define EXECUTE_ONCE(A, B) pthread_once(A, B)
#endif

#include "protocol.h"

PUSH_SYS_WARNINGS_CDK
#include <memory.h> // for memcpy
POP_SYS_WARNINGS_CDK


#ifdef DEBUG_PROTOBUF

PUSH_SYS_WARNINGS_CDK
#include <iostream>
POP_SYS_WARNINGS_CDK

#define MSG_CLIENT_name_client(MSG,N,C) case msg_type::cli_##N: return #MSG;
#define MSG_SERVER_name_client(MSG,N,C)
#define MSG_CLIENT_name_server(MSG,N,C)
#define MSG_SERVER_name_server(MSG,N,C) case msg_type::N: return #MSG;

namespace cdk {
namespace protocol {
namespace mysqlx {

const char* msg_type_name(Protocol_side side, msg_type_t type)
{
  switch (side)
  {
  case SERVER:
    switch (type)
    {
      MSG_LIST(name_server)
      default: return "<unknown msg type>";
    }
  case CLIENT:
    switch (type)
    {
      MSG_LIST(name_client)
      default: return "<unknown msg type>";
    }
  }
  return NULL;
}

}}}  // cdk::protocol::mysqlx

#endif


IMPL_PLAIN(cdk::protocol::mysqlx::Protocol);
IMPL_PLAIN(cdk::protocol::mysqlx::Protocol_server);


using namespace cdk::foundation;
using namespace google::protobuf;
using namespace cdk::protocol::mysqlx;


namespace cdk {
namespace protocol {
namespace mysqlx {


/*
  Protobuf log handler initialization.
*/

static void log_handler(LogLevel level, const char* filename, int line, const std::string& message);

#ifdef _WIN32
BOOL CALLBACK log_handler_init(PINIT_ONCE, PVOID, PVOID*)
{
  SetLogHandler(&log_handler);
  return TRUE;
}
#else
static void log_handler_init()
{
  SetLogHandler(log_handler);
}
#endif


/*
  Base protocol implementation
  ============================
*/


Protocol_impl::Protocol_impl(Protocol::Stream *str, Protocol_side side)
  : m_str(str), m_side(side)
  , m_msg_state(PAYLOAD)
  , m_msg_size(0)
{
  // Warning can be disabled because the handler is not called, only registered
  PUSH_MSVC17_WARNINGS_CDK
    EXECUTE_ONCE(&log_handler_once, &log_handler_init);
  POP_MSVC17_VARNINGS_CDK

    // Allocate initial I/O buffers

  m_wr_size = m_rd_size = 1024;
  m_rd_buf = (byte*)malloc(m_rd_size);
  m_wr_buf = (byte*)malloc(m_wr_size);

  if (!m_rd_buf)
    throw_error("Could not allocate initial input buffer");

  if (!m_wr_buf)
    throw_error("Could not allocate initial output buffer");

}

Protocol_impl::~Protocol_impl()
{
  free(m_rd_buf);
  free(m_wr_buf);
  delete m_str;
}

class Invalid_msg_error : public Error_class<Invalid_msg_error>
{
  unsigned m_state;
  msg_type_t m_type;

public:

  Invalid_msg_error(msg_type_t type, unsigned state)
    : Error_base(NULL, cdkerrc::generic_error),
      m_state(state), m_type(type)
  {}

private:

  void do_describe(std::ostream &out) const
  {
    out <<"Message of type " <<(unsigned)m_type
        <<" is not valid in protocol state " <<m_state;
  }
};

void Protocol_impl::start_Pipeline()
{
  m_pipeline = true;
}

Protocol::Op& Protocol_impl::snd_Pipeline()
{
  m_snd_op.reset();
  m_snd_op.reset(new Op_snd_pipeline(*this));
  return *m_snd_op;
}

void Protocol_impl::clear_Pipeline()
{
  m_pipeline = false;
  m_pipeline_size = 0;
}


Protocol::Op& Protocol_impl::snd_start(Message &msg, msg_type_t msg_type)
{

#ifdef DEBUG_PROTOBUF

  using std::cerr;
  using std::endl;

  cerr << endl;
  cerr << ">>>> Sending message >>>>" << endl;
  cerr << "of type " << msg_type << ": "
      << msg_type_name(CLIENT, msg_type) << endl;
  cerr << msg.DebugString();
  cerr << ">>>>" << endl << endl;

#endif

  //First delete completed OP, so that if Snd_op() throws exception m_snd_op
  //will not point to old OP.
  m_snd_op.reset();
  m_snd_op.reset(new Op_snd(*this, msg_type, msg));
  return *m_snd_op;
}


/*
  Helper function which creates protobuf message object of type
  indicated by msg_type identifier. Interpretation of msg_type_t
  values depends on whether we look at server-side or client-side
  messages.
*/

Message* mk_message(Protocol_side side, msg_type_t msg_type)
{
    /*
      The case clauses for switch statements that generate
      messages of appropriate type are again generated using
      MSG_SERVER() and MSG_CLIENT() entries in MSG_LIST()
      macro (see protocol/mysqlx.h for definition and description).
    */

#define MSG_CLIENT_MK_CLI(MSG,N,C) \
  case msg_type::cli_##N: return new MSG();
#define MSG_SERVER_MK_CLI(MSG,N,C)
#define MSG_SERVER_MK_SRV(MSG,N,C) \
  case msg_type::N: return new MSG();
#define MSG_CLIENT_MK_SRV(MSG,N,C)

    switch (side)
    {
    case SERVER:
      switch (msg_type)
      {
        MSG_LIST(MK_SRV)
        // TODO: proper error
        default: THROW("unknown server message type");
      }; break;

    case CLIENT:
      switch (msg_type)
      {
        MSG_LIST(MK_CLI)
        default: THROW("unknown server message type");
      }; break;

    default: THROW("unknown protocol side");
    }
}



/*
  Protobuf error logger
  =====================

  This method is registered with Protobuf and called when warning/error/fatal
  situation occurs in Protobuf (such as parsing error etc).
*/

static void log_handler(
  LogLevel level, const char* /*filename*/, int /*line*/,
  const std::string& message
)
{
  switch(level)
  {
    case LOGLEVEL_FATAL:
    case LOGLEVEL_ERROR:
      /*
        With this code the error description is:

          MMM: Protobuf error (cdk:NNN)

        where MMM is the message and NNN is the protbuf error code.

        TODO: Change description to:

          Protobuf error: MMM (cdk:NNN)
      */
      throw_error(cdkerrc::protobuf_error, message);

    case LOGLEVEL_WARNING:
    case LOGLEVEL_INFO:
    default:
    {
      // just ignore for now
      // TODO: this could be used for logging in the future
    }
  }
}

/*
  Implementation of protobuf's ZeroCopyOutputStream which stores
  data in the given memory buffer.
  ==============================
*/

class ArrayStream : public google::protobuf::io::ZeroCopyOutputStream
{
  byte *m_buf;
  size_t m_size;
  size_t m_bytes_count;

  public:

  ArrayStream(byte * buf, size_t size) : m_buf(buf), m_size(size),
                                         m_bytes_count(0)
  {}

  virtual bool Next(void ** data, int * size)
  {
    if (m_bytes_count >= m_size)
      return false;

    *data = m_buf + m_bytes_count;
    *size = (int)(m_size - m_bytes_count);
    m_bytes_count = m_size; // We always guess that all buffer is used

    return true;
  }

  virtual void BackUp(int count)
  {
    assert((int)m_bytes_count >= count);
    m_bytes_count -= count;
  }

  int64 ByteCount() const
  {
    return (int64)m_bytes_count;
  }
};

/*
  Writing/reading message frames
  ==============================
*/

void Protocol_impl::write_msg(msg_type_t msg_type, Message &msg)
{
  if (m_wr_op)
    THROW("Can't write message while another one is being written");

  msg_size_t net_size = static_cast<unsigned>(msg.ByteSize()) + 1;

  if (!resize_buf(CLIENT, header_length + net_size))
    THROW("Not enough memory for output buffer");

  // Serialize message

  assert(m_wr_size < (size_t)std::numeric_limits<int>::max());

  if (!msg.SerializeToArray((void*)(wr_buffer() + header_length),
                            (int)(wr_size() - header_length)))
  {
    m_pipeline = false;
    m_pipeline_size = 0;
    throw_error(cdkerrc::protobuf_error, "Serialization error!");
  }

  byte *wr_buf = wr_buffer();
  size_t total_write_size = 0;

  if (m_compressor.m_compression_type != Compression_type::NONE &&
      net_size > m_compress_threshold)
  {
    HTONSIZE(net_size);
    memcpy((void*)wr_buf, (const void*)&net_size, sizeof(net_size));
    NTOHSIZE(net_size);
    wr_buf[header_length - 1] = (byte)msg_type;

    // Do not take into account the msg type when using compression
    msg_size_t payload_size = net_size - 1;
    msg_size_t compressed_size = (msg_size_t)m_compressor.
      do_compress(m_wr_buf, payload_size + header_length);

    if (compressed_size == 0)
      throw_error("Failed to compress the data");

    /*
      Two messages are required in order to ensure that
      the message type and uncompressed size are sent before
      the payload.
    */
    Mysqlx::Connection::Compression first_fields;
    Mysqlx::Connection::Compression compression_payload;

    first_fields.set_client_messages(
      static_cast<::Mysqlx::ClientMessages_Type>(msg_type));
    first_fields.set_uncompressed_size(payload_size + header_length);
    byte *cmp_out_buf = m_compressor.get_out_buf();
    compression_payload.set_payload(cmp_out_buf, compressed_size);

    // The Compressed message will add only a few bytes
    // to the compressed payload. It should not be more than 128.
    if (!resize_buf(CLIENT, compressed_size + 128))
      THROW("Not enough memory for output buffer");

    wr_buf = wr_buffer();

    ArrayStream astr(wr_buf + header_length, wr_size());
    first_fields.SerializePartialToZeroCopyStream(&astr);
    compression_payload.SerializePartialToZeroCopyStream(&astr);

    // First 4 bytes of frame length are not counted as payload
    net_size = static_cast<msg_size_t>(astr.ByteCount()) + 1;
    msg_type = msg_type::cli_Compression;

  }

  // Construct message header
  HTONSIZE(net_size);
  memcpy((void*)wr_buf, (const void*)&net_size, sizeof(net_size));
  wr_buf[header_length - 1] = (byte)msg_type;
  // Convert net_size back to original endian before using it later
  NTOHSIZE(net_size);
  total_write_size = net_size + header_length - 1;

  // Create write operation to send message payload
  m_pipeline_size += total_write_size;

  if (!m_pipeline)
  {
    write(wr_buf);
  }
}


void Protocol_impl::write(byte *buf)
{
  m_wr_op.reset(m_str->write(buffers(buf, m_pipeline_size)));
  clear_Pipeline();
}

void Protocol_impl::write()
{
  write(m_wr_buf);
}


bool Protocol_impl::wr_cont()
{
  if (!m_wr_op)
    return true;

  if (!m_wr_op->cont())
    return false;

  m_wr_op.reset();
  return true;
}


void Protocol_impl::wr_wait()
{
  if (m_wr_op)
  {
    m_wr_op->wait();
    m_wr_op.reset();
  }
}


void Protocol_impl::read_header()
{
  if (HEADER == m_msg_state)
    return;

  m_msg_state= HEADER;

  if (m_msg_compressed_type)
  {
    /*
      If we are processing compressed messages, and there is more compressed
      data, uncompress next message header. Otherwise (no more compressed data),
      get out of compressed mode and proceed to reading next message header
      from the input stream.
    */

    if (!m_compressor.uncompression_finished())
    {
      if (!m_compressor.uncompress(m_rd_buf, 5))
        THROW("Error uncompressing the message header");
      return;
    }
    else
    {
      m_msg_compressed_type = 0;
      m_compressor.reset();  // clean up compressor
    }
  }


  if (m_rd_op)
    THROW("can't read header when reading payload is not completed");

  // Read length and message type
  m_rd_op.reset(m_str->read(buffers(m_rd_buf, 5)));
}


void Protocol_impl::read_payload()
{
  if (PAYLOAD == m_msg_state)
    return;

  if (HEADER != m_msg_state)
    THROW("payload can be read only after header");

  m_msg_state = PAYLOAD;

  // Nothing to do if message has no payload.

  if (0 == m_msg_size)
    return;

  if (!resize_buf(SERVER, m_msg_size))
    THROW("Not enough memory for input buffer");

  /*
    If we process compressed data, request compressor to decompress next
    payload. Otherwise read payload directly from input stream.
  */

  if (m_msg_compressed_type)
  {
    if (!m_compressor.uncompress(m_rd_buf, m_msg_size))
      THROW("Error uncompressing the message payload");
    return;
  }

  if (m_rd_op)
    THROW("can't read payload when reading header is not completed");

  m_rd_op.reset(m_str->read(buffers(m_rd_buf, m_msg_size)));
}


bool Protocol_impl::rd_cont()
{
  // First try to finish m_rd_op, if set.

  if (m_rd_op && !m_rd_op->cont())
    return false;

  // Call rd_process when IO is finished (orthere is no IO to begin with).

  m_rd_op.reset();
  rd_process();

  // We are done only if rd_process() did not set up a new IO operation.

  return !m_rd_op;
}


void Protocol_impl::rd_wait()
{
  while (!rd_cont())
  {
    // Note: rd_cont() returns false only if there is pending IO
    assert(m_rd_op);
    m_rd_op->wait();
  }
}


bool Protocol_impl::resize_buf(Protocol_side side, size_t requested_size)
{
  byte*  &buf = (side == SERVER ? m_rd_buf :m_wr_buf);

  size_t &buf_size = (side == SERVER ? m_rd_size : m_wr_size);

  if (requested_size < buf_size)
    return true;

  // Note that since requested_size >= buf_size, the buffer size is
  // at least doubled here.

  size_t new_size = buf_size + requested_size;
  byte *ptr = (byte*)realloc(buf, new_size);

  // If allocating buffer with margin failed, try allocating
  // exact required amount.

  if (!ptr)
  {
    if (side == CLIENT)
      new_size = m_pipeline_size + requested_size;
    else
      new_size = requested_size;
    ptr = (byte*)realloc(buf, new_size);
  }

  if (!ptr)
    return false;

  buf_size = new_size;
  buf = ptr;

  return true;
}

#define GET_PAYLOAD_SIZE(S, B) S = *(msg_size_t*)(B); \
                           NTOHSIZE(S)


/*
  Note: Called from rd_wait() or rd_cont() when the async IO m_rd_op
  is completed.
*/

void Protocol_impl::rd_process()
{
  /*
    At this point m_rd_op is completed. We have any more
    work to do here only if we are in HEADER mode and we
    need to parse the header data that is now available.
  */

  if (HEADER != m_msg_state)
    return;

  if (m_msg_compressed_type == 0)
  {
    GET_PAYLOAD_SIZE(m_msg_size, m_rd_buf);
    m_msg_size--;
    // The read buffer already contains the message type
    m_msg_type = m_rd_buf[4];

    if (m_msg_type == msg_type::Compression)
    {
      m_msg_compressed_type = m_msg_type;
      // Make sure the reading buffer is large enough
      if (!resize_buf(SERVER, m_msg_size))
        THROW("Not enough memory for input buffer");

      m_rd_op.reset(m_str->read(buffers(m_rd_buf, m_msg_size)));
      m_preamble = true;
      return;
    }
  }
  else
  {
    /*
      We are processing compressed frame, looking for next message (since
      m_msg_state is HEADER).

      If compressor was not initialized yet, we are looking at the 5 bytes
      preamble containing info about compressed data and we can initalize
      compressor using that data and request uncompressing first payload size.

      Otherwise (compressor already intialized) m_rd_buf contains 4 byte
      size of the next payload which is stored in m_msg_size.
    */

    if (m_preamble)
    {
      m_preamble = false;
      m_compressed_msg.Clear();
      if (!m_compressed_msg.ParseFromArray(m_rd_buf, (int)m_msg_size))
        throw_error("Invalid Compression message");

      m_compressor.set_compressed_buf((byte*)m_compressed_msg.payload().data(),
        m_compressed_msg.payload().length(),
        (size_t)m_compressed_msg.uncompressed_size());

      if (!m_compressor.uncompress(m_rd_buf, 5))
        throw_error("Error uncompressing the message header");

      GET_PAYLOAD_SIZE(m_msg_size, m_rd_buf);
      --m_msg_size; // Subtract 1 byte of msg type, which we already know
      m_msg_type = (msg_type_t)m_rd_buf[4];
    }
    else
    {
      if (!m_compressor.uncompression_finished())
      {
        GET_PAYLOAD_SIZE(m_msg_size, m_rd_buf);
        --m_msg_size; // Subtract 1 byte of msg type, which we already know
      }
    }
  }
}

void
Protocol_impl::set_compression(Compression_type::value compression_type,
                                    size_t threshold)
{
  m_compressor.set_compression_type(compression_type);
  m_compress_threshold = threshold;
}

/*
  Processing incoming messages
  ============================
*/


/*
  Main logic of receive operation which reads messages and passes data
  to message processor.

  The async flag tells whether a call to do_read_msg() should proceed until
  complete message is read and processed (if async is false) or whether it
  can return before completing the operation (if async is true) in which case
  another call to do_read_msg() will push it further.

  Returns false if the operation is not completed yet.
*/


bool Op_rcv::do_read_msg(bool async)
{
  while (!m_completed)
  {
    switch (m_stage)
    {
      case HEADER:
      {
        // first make sure header is read.

        if (!async)
          m_proto.rd_wait();
        else if (!m_proto.rd_cont())
          return false;

        m_msg_type = m_proto.m_msg_type;

        // Check whether message is expected and whether operation
        // should stop here.

        Next_msg next = next_msg(m_msg_type);

        if (STOP == next)
        {
          finish(false);  // false = do not read next message
          if (async)
            return true;
          continue;
        }

        /*
          Call message_begin(). The Boolean flag passed to this method
          tells if this message is expected. The processor can alter the
          flag to change the way message is processed below.
        */

        bool flag = (EXPECTED == next);

        if (!m_error && m_prc)
        {
          try
          {
            m_read_window= m_prc->message_begin(m_msg_type, flag);
            m_call_message_end = true;
          }
          catch (...)
          {
            save_error();
            m_call_message_end = false;
          }
        }

        // Interpret disposition how to handle the message

        m_skip = false;
        if (UNEXPECTED == next)
        {
          /*
            If message was not expected, the initial value of the flag was
            false. If the processor has not changed it, we throw unexpected
            message error here. Otherwise, the processor told us to skip this
            unexpected message and continue processing.
          */
          if (!flag)
          {
            finish(false);
            THROW("unexpected message");
          }
          m_skip = true;
        }
        else if (!flag)
        {
          /*
            If message was expected, the initial value of the flag was true.
            If the processor has changed it then it means it does not want
            to see this message and we can skip it.
          */
          m_skip = true;
        }

        // Start reading payload

        m_proto.read_payload();
        m_stage = PAYLOAD;

        // fall-through to payload processing phase
      }

      case PAYLOAD:
      {
        // Ensure that payload is read

        if (!async)
          m_proto.rd_wait();
        else if (!m_proto.rd_cont())
          return false;

        m_msg_size = m_proto.m_msg_size;

        // process the payload

        if (m_prc && !m_error)
        {
          process_payload();
        }

        /*
          call message_end() - the return value can tell us to stop
          processing here regardless of the current state.
        */

        bool stop = false;
        if (m_prc && m_call_message_end)
        {
          try
          {
            stop = !m_prc->message_end();
          }
          catch(...)
          {
            save_error();
          }
        }

        m_stage = DONE;

        /*
          Pass true to finish() to read next message if process_next()
          tells us so and the processor has not interrupted the processing.

          Note: it is important to always call process_next() because derived
          classes rely on it being called after processing each message to
          do final chores.
        */

        bool done = finish(process_next() && !stop);

        if (async)
          return done;
      }
      break;

    case DONE:
      return true;
    }
  }

  return true;
}


bool Op_rcv::do_cont()
{
  return do_read_msg(true);
}

void Op_rcv::do_wait()
{
  do_read_msg(false);
}


/*
  Process received message payload. After parsing, the parsed message
  is processed with process_msg() method which can be overridden by
  derived classes.
*/

void Op_rcv::process_payload()
{
  assert(m_prc);
  assert(PAYLOAD == m_stage);

  // Send raw message bytes to m_prc if requested (m_read_window > 0).

  try {

    byte *cur_pos = m_proto.m_rd_buf;
    byte *end_pos = cur_pos + m_msg_size;

    /*
      Note: read_header() checks if message fits into the buffer and
      throws error if this is not the case.
    */

    assert(m_msg_size <= m_proto.m_rd_size);

    while (cur_pos < end_pos && m_read_window)
    {
      size_t new_window = m_prc->message_data(bytes(cur_pos,
                                  cur_pos + m_read_window < end_pos ?
                                  m_read_window : end_pos - cur_pos));
      cur_pos += m_read_window;
      m_read_window= new_window;
    }
    m_prc->message_received(m_msg_size);

  }
  catch(...)
  {
    save_error();
    return;
  }

  if (m_skip)
    return;

  // Parse message.

  scoped_ptr<Message> m_msg;
  m_msg.reset(mk_message(m_proto.m_side, m_msg_type));

  if (m_msg_size > 0)
  {
    try {
      assert(m_msg_size < (size_t)std::numeric_limits<int>::max());
      if (!m_msg->ParseFromArray(m_proto.m_rd_buf, (int)m_msg_size))
        throw_error(cdkerrc::protobuf_error, "Message could not be parsed");
    }
    catch (...)
    {
      save_error();
      m_msg.reset();
      return;
    }
  }

#ifdef DEBUG_PROTOBUF

  using std::cerr;
  using std::endl;

  cerr << endl;
  cerr << "<<<< Received message <<<<" << endl;
  cerr << "of type " << m_msg_type <<": "
       << msg_type_name(SERVER, m_msg_type) << endl;
  cerr << m_msg->DebugString();
  cerr << "<<<<" << endl << endl;

#endif

  // Pass data from parsed message to processor

  process_msg(m_msg_type, *m_msg);
}


/*
  Finish processing the current message and optionally start reading
  the next one (if read_next is true). If no more messages are read and
  there is saved error, throw it.

  Returns false if the operation should process another message, true
  if it is completed now.
*/

bool Op_rcv::finish(bool read_next)
{
  if (read_next)
  {
    read_msg();
    return false;
  }

  m_completed= true;
  throw_saved_error();
  return true;
}

}}}  // cdk::protocol::mysqlx



/*
  Explicit specialization is needed because Protocol constructor is templated
  (and thus no code is generated unless template is used).
*/
template cdk::foundation::opaque_impl<Protocol>::opaque_impl(void*, Protocol::Stream*);
template cdk::foundation::opaque_impl<Protocol_server>::opaque_impl(void*, Protocol::Stream*);


/*
  Implementation of Protocol methods using the internal implementation.
*/

namespace cdk {
namespace protocol {
namespace mysqlx {


// Client-side API

class Rcv_reply : public Op_rcv
{
public:

  Rcv_reply(Protocol_impl &proto) : Op_rcv(proto)
  {}

  void resume(Reply_processor &prc)
  {
    read_msg(prc);
  }

  Next_msg do_next_msg(msg_type_t type)
  {
    return msg_type::Ok == type ? EXPECTED : UNEXPECTED;
  }

  void do_process_msg(msg_type_t type, Message &msg)
  {
    if (msg_type::Ok != type)
      THROW("wrong message type");

    Mysqlx::Ok &ok= static_cast<Mysqlx::Ok&>(msg);
    static_cast<Reply_processor&>(*m_prc).ok(ok.msg());
  }

};


Protocol::Op& Protocol::snd_SessionReset(bool keep_open)
{
  Mysqlx::Session::Reset reset;
  reset.set_keep_open(keep_open);
  return get_impl().snd_start(reset, msg_type::cli_SessionReset);
}

Protocol::Op& Protocol::snd_SessionClose()
{
  Mysqlx::Session::Close close;
  return get_impl().snd_start(close, msg_type::cli_SessionClose);
}

Protocol::Op& Protocol::snd_ConnectionClose()
{
  Mysqlx::Connection::Close close;
  return get_impl().snd_start(close, msg_type::cli_Close);
}

Protocol::Op& Protocol::rcv_Reply(Reply_processor &prc)
{
  return get_impl().rcv_start<Rcv_reply>(prc);
}


// Server-side API
// ===============
// TODO: Complete and adapt to protocol changes.


Protocol::Op& Protocol_server::snd_Ok(const string &msg)
{
  Mysqlx::Ok ok;
  ok.set_msg(msg);
  return get_impl().snd_start(ok, msg_type::Ok);
}

Protocol::Op& Protocol_server::snd_Error(short unsigned errc, const string &msg)
{
  // TODO: SQL state
  Mysqlx::Error err;
  err.set_severity(Mysqlx::Error_Severity_ERROR);
  err.set_sql_state("SQLST");
  err.set_code(errc);
  err.set_msg(msg);
  return get_impl().snd_start(err, msg_type::Error);
}


class Rcv_command : public Op_rcv
{
public:

  Rcv_command(Protocol_impl &proto) : Op_rcv(proto)
  {}

  void resume(Cmd_processor &prc)
  {
    read_msg(prc);
  }

  Next_msg next_msg(msg_type_t type)
  {
    switch (type)
    {
    case msg_type::cli_Close:
      return EXPECTED;
    default: return UNEXPECTED;
    }
  }

  void process_msg(msg_type_t, Message&);
};


void Rcv_command::process_msg(msg_type_t type, Message&)
{
  Cmd_processor prc= static_cast<Cmd_processor&>(*m_prc);

  switch (type)
  {
  case msg_type::cli_Close: prc.close(); return;
  default: THROW("not implemented command");
  }
};


Protocol::Op& Protocol_server::rcv_Command(Cmd_processor &prc)
{
  return get_impl().rcv_start<Rcv_command>(prc);
}

// ------------------------------------------------------------

size_t Processor_base::message_begin_internal(msg_type_t type, bool &flag)
{
  m_type = type;
  return message_begin(type, flag);
}

void Processor_base::message_received_internal(size_t bytes_read)
{
  m_bytes_read = bytes_read;
  message_received(bytes_read);
}

}}}
