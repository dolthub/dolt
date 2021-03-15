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
   Implementation of mysqlx protocol compression
   =============================================
 */

 // Note: on Windows this includes windows.h

#include <mysql/cdk/foundation/common.h>

#include "protocol.h"

PUSH_SYS_WARNINGS_CDK
#include <memory.h> // for memcpy
POP_SYS_WARNINGS_CDK



using namespace cdk::foundation;
using namespace cdk::protocol::mysqlx;


namespace cdk {
namespace protocol {
namespace mysqlx {

/*
  ZLib Compression Algorithm functions
  ====================================
*/

void Compression_zlib::init()
{
  if (m_zlib_inited)
    return;

  // Initial functions mapping, keep the internal implementation
  m_c_zstream.zalloc = Z_NULL;
  m_c_zstream.zfree = Z_NULL;
  m_c_zstream.opaque = Z_NULL;
  m_c_zstream.total_out = 0;

  // TODO: Make the compression level adjustable
  if (deflateInit(&m_c_zstream, 9) != Z_OK)
    throw_error("Could not initialize compression output stream");

  // Initial functions mapping, keep the internal implementation
  m_u_zstream.zalloc = Z_NULL;
  m_u_zstream.zfree = Z_NULL;
  m_u_zstream.opaque = Z_NULL;

  if (inflateInit(&m_u_zstream) != Z_OK)
    throw_error("Could not initialize compression input stream");

  m_zlib_inited = true;
}


size_t Compression_zlib::compress(byte *src, size_t len)
{
  size_t total_compressed_len = m_c_zstream.total_out;

  m_c_zstream.next_in = src;           // Input buffer with uncompressed data
  m_c_zstream.avail_in = (uInt)len;    // Length of uncompressed data

  /*
    TODO: Do smarter allocation for compression buffer since
    the upper bound might be quite redundant.
  */
  size_t deflate_size = deflateBound(&m_c_zstream, (uLong)len);

  // This will reallocate the buffer if needed and get its address
  m_c_zstream.next_out = m_protocol_compression.get_out_buf(deflate_size);

  m_c_zstream.avail_out = (uInt)m_protocol_compression.get_out_buf_len();

  int res = deflate(&m_c_zstream, Z_SYNC_FLUSH);
  if (res != Z_OK)
    return 0;

  return m_c_zstream.total_out - total_compressed_len;
}


size_t Compression_zlib::uncompress(byte *dst,
  size_t dest_size, size_t compressed_size,
  size_t &bytes_consumed)
{

  m_u_zstream.next_in = m_protocol_compression.get_inp_buf();
  m_u_zstream.avail_in = (uInt)compressed_size;

  m_u_zstream.next_out = dst;
  m_u_zstream.avail_out = (uInt)dest_size;
  int inflate_res = inflate(&m_u_zstream, Z_SYNC_FLUSH);

  if (inflate_res != Z_OK)
  {
    inflateReset(&m_u_zstream);
    return COMPRESSION_ERROR;
  }

  // The number of processed compressed bytes
  bytes_consumed = compressed_size - m_u_zstream.avail_in;

  // The number of uncompressed bytes
  return dest_size - m_u_zstream.avail_out;
}


Compression_zlib::~Compression_zlib()
{
  if (m_zlib_inited)
  {
    deflateEnd(&m_c_zstream);
    inflateEnd(&m_u_zstream);
  }
}


/*
  LZ4 Compression Algorithm functions
  ===================================
*/

void Compression_lz4::init()
{
  if (m_dctx && m_cctx)
    return;

  if (m_dctx == nullptr &&
    LZ4F_isError(LZ4F_createDecompressionContext(&m_dctx, LZ4F_VERSION)))
    throw_error("Error creating LZ4 decompression context");

  if (m_cctx == nullptr &&
    LZ4F_isError(LZ4F_createCompressionContext(&m_cctx, LZ4F_VERSION)))
    throw_error("Error creating LZ4 compression context");

  m_lz4f_pref.autoFlush = 1;
  m_lz4f_pref.frameInfo.contentSize = 0;
}


size_t Compression_lz4::compress(byte *src, size_t len)
{
  auto check_lz4_result = [this](size_t result)
  {
    if (LZ4F_isError(result))
    {
      LZ4F_freeCompressionContext(m_cctx);
      m_cctx = nullptr;
      throw_error("LZ4 compression error");
    }
  };

  if (len > LZ4_MAX_INPUT_SIZE)
    throw_error("Data for compression is too long");

  size_t wbuf_size = LZ4F_compressBound(len, &m_lz4f_pref);
  // Allocate wr buf and adjust the offset for writing data
  byte *dest_buf_adjusted = m_protocol_compression.get_out_buf(wbuf_size);

  // Update with the real buffer length
  wbuf_size = m_protocol_compression.get_out_buf_len();

  size_t begin_result = LZ4F_compressBegin(m_cctx, (void*)dest_buf_adjusted,
    wbuf_size, &m_lz4f_pref);
  check_lz4_result(begin_result);
  dest_buf_adjusted += begin_result;
  wbuf_size -= begin_result;

  void *src_adjusted = (void*)src;
  size_t compression_result = LZ4F_compressUpdate(m_cctx, (void*)dest_buf_adjusted,
    wbuf_size,
    src_adjusted, len, nullptr);
  check_lz4_result(compression_result);

  dest_buf_adjusted += compression_result;
  wbuf_size -= compression_result;

  assert(4 <= wbuf_size);
  size_t flush_result = LZ4F_compressEnd(m_cctx, (void*)dest_buf_adjusted,
    wbuf_size,
    nullptr);
  check_lz4_result(flush_result);

  return begin_result + flush_result + compression_result;
}


size_t Compression_lz4::uncompress(byte *dst,
  size_t dest_size, size_t compressed_size,
  size_t &bytes_consumed)
{

  size_t bytes_processed = 0;
  size_t initial_dest_size = dest_size;
  while (true)
  {
    size_t bytes_to_write = dest_size;
    size_t current_bytes_processed = compressed_size - bytes_processed;
    size_t result = LZ4F_decompress(m_dctx, (void*)dst, &bytes_to_write,
      (void*)(m_protocol_compression.get_inp_buf() + bytes_processed),
      &current_bytes_processed, nullptr);

    if (LZ4F_isError(result))
    {
      LZ4F_resetDecompressionContext(m_dctx);
      throw_error("Problem during LZ4 decompression");
    }

    if (dest_size < bytes_to_write)
    {
      throw_error("Decompression buffer is not large enough");
    }

    bytes_processed += current_bytes_processed;
    dst += bytes_to_write;        // Adjust the buffer writing position
    dest_size -= bytes_to_write;  // Adjust buffer size awailable for writing

    if (result == 0 || current_bytes_processed == 0 /*bytes_to_write == 0*/)
      break;
  }
  bytes_consumed = bytes_processed;
  return initial_dest_size - dest_size;
}


Compression_lz4::~Compression_lz4()
{
  if (m_dctx)
    LZ4F_freeDecompressionContext(m_dctx);
  if (m_cctx)
    LZ4F_freeCompressionContext(m_cctx);
}


/*
  ZStd Compression Algorithm
  ==========================
*/

void Compression_zstd::init()
{
  if (m_c_zstd && m_u_zstd)
    return;

  if (m_c_zstd == nullptr)
  {
    m_c_zstd = ZSTD_createCStream();
    if (ZSTD_isError(ZSTD_initCStream(m_c_zstd, -1)))
      throw_error("Error creating ZSTD compression stream");
  }

  if (m_u_zstd == nullptr)
  {
    m_u_zstd = ZSTD_createDStream();
    if (ZSTD_isError(ZSTD_initDStream(m_u_zstd)))
      throw_error("Error creating ZSTD decompression stream");
  }
}


size_t Compression_zstd::compress(byte *src, size_t len)
{
  size_t estimated_c_size = ZSTD_compressBound(len);
  ZSTD_outBuffer out_buffer{
    m_protocol_compression.get_out_buf(estimated_c_size),
    estimated_c_size, 0 };

  ZSTD_inBuffer in_buffer{ src, len, 0 };

  while (in_buffer.pos < in_buffer.size)
  {
    size_t result = ZSTD_compressStream(m_c_zstd, &out_buffer, &in_buffer);
    if (ZSTD_isError(result))
      throw_error("ZSTD compression error");
  }

  size_t flush_result = ZSTD_flushStream(m_c_zstd, &out_buffer);
  if (ZSTD_isError(flush_result))
    throw_error("ZSTD flush error");

  return out_buffer.pos;
}


size_t Compression_zstd::uncompress(byte *dst,
  size_t dest_size, size_t compressed_size,
  size_t &bytes_consumed)
{
  ZSTD_outBuffer out_buffer{ dst, dest_size, 0 };
  ZSTD_inBuffer in_buffer{ m_protocol_compression.get_inp_buf(), compressed_size, 0 };

  while (out_buffer.pos < out_buffer.size)
  {
    size_t result = ZSTD_decompressStream(m_u_zstd, &out_buffer, &in_buffer);
    if (ZSTD_isError(result))
      throw_error("ZSTD decompression error");

    // All input is consumed, do not attempt to go for another iteration
    if (in_buffer.pos >= in_buffer.size)
      break;
  }

  bytes_consumed = in_buffer.pos;
  return out_buffer.pos;
}


Compression_zstd::~Compression_zstd()
{
  if (m_u_zstd)
    ZSTD_freeDStream(m_u_zstd);
  if (m_c_zstd)
    ZSTD_freeCStream(m_c_zstd);
}


/*
  Protocol compression
  ====================
*/

Protocol_compression::Protocol_compression()
{ }


byte* Protocol_compression::get_out_buf(size_t size)
{
  if (m_c_out_size && size <= m_c_out_size)
    return m_c_out_buf;

  byte *tmp = (byte*)realloc(m_c_out_buf, size);
  if (!tmp)
    throw_error("Could not reallocate compression output buffer");

  m_c_out_buf = tmp;
  m_c_out_size = size;
  return m_c_out_buf;
}


size_t Protocol_compression::do_compress(byte *src, size_t len)
{
  if (!m_algorithm)
    throw_error("Unknown compression type");

  return m_algorithm->compress(src, len);
}

bool
  Protocol_compression::uncompress(byte *buf, size_t size)
{
  // If no more data is needed do not uncompress anything
  if (0 == size)
    return true;

  size_t orig_size = size;
  
  do
  {
    size -= do_uncompress(buf + orig_size - size, size);

    if (0 == size)
      return true;

    if (COMPRESSION_ERROR == size)
      return false;

  }while(size);

  return true;
}


size_t
  Protocol_compression::do_uncompress(byte *dst, size_t dest_size)
{
  size_t bytes_uncompressed = 0;
  size_t bytes_consumed = 0;

  /*
    ZSTD can consume the entire input when uncompressing 5 bytes
    of header and we need to call the uncompression again to obtain
    the rest of uncompressed data.
  */
  if (m_c_inp_size || m_u_total_size)
  {
    if (!m_algorithm)
      throw_error("Unknown compression type");

    bytes_uncompressed = m_algorithm->uncompress
    (dst, dest_size, m_c_inp_size, bytes_consumed);

    m_c_inp_offset += bytes_consumed;
    m_c_inp_size -= bytes_consumed;
    m_u_total_size -= bytes_uncompressed;
  }

  return bytes_uncompressed;
}


void Protocol_compression::set_compression_type
(Compression_type::value compression_type)
{
  m_compression_type = compression_type;
  switch (m_compression_type)
  {
  case Compression_type::DEFLATE:
    m_algorithm.reset(new Compression_zlib(*this));
    break;
  case Compression_type::LZ4:
    m_algorithm.reset(new Compression_lz4(*this));
    break;
  case Compression_type::ZSTD:
    m_algorithm.reset(new Compression_zstd(*this));
    break;
  case Compression_type::NONE:
    m_algorithm.reset();
    break;
  default:
    throw_error("Unknown compression type");
  }
}


Protocol_compression::~Protocol_compression()
{
  if (m_c_out_buf)
    free(m_c_out_buf);
}


}}}
