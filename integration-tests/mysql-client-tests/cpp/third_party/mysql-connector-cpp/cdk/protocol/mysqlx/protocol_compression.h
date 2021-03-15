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


#ifndef PROTOCOL_MYSQLX_PROTOCOL_COMPRESSION_H
#define PROTOCOL_MYSQLX_PROTOCOL_COMPRESSION_H

#include <mysql/cdk/protocol/mysqlx.h>
#include <zlib.h>
#include <lz4.h>
#include <lz4frame.h>
#include <zstd.h>


namespace cdk {
namespace protocol {
namespace mysqlx {

#define COMPRESSION_ERROR           0xFFFFFFFF

typedef cdk::protocol::mysqlx::api::Compression_type Compression_type;

class Protocol_compression;

class Compression_algorithm
{
  protected:

  Protocol_compression &m_protocol_compression;

  public:

  Compression_algorithm(Protocol_compression& c) :
    m_protocol_compression(c)
  {}

  virtual size_t compress(byte *src, size_t len) = 0;

  virtual size_t uncompress(byte *dst, size_t dest_size,
                            size_t compressed_size,
                            size_t &bytes_consumed) = 0;

  virtual ~Compression_algorithm() {}
};


class Compression_zlib : public Compression_algorithm
{
  z_stream m_u_zstream;       // Uncompression ZLib stream
  z_stream m_c_zstream;       // Compression ZLib stream
  bool m_zlib_inited = false;

  void init();

  public:

  Compression_zlib(Protocol_compression& c) :
    Compression_algorithm(c)
  { init(); }

  size_t compress(byte *src, size_t len) override;
  size_t uncompress(byte *dst, size_t dest_size, size_t compressed_size,
                    size_t &bytes_consumed) override;
  ~Compression_zlib();
};


class Compression_lz4 : public Compression_algorithm
{
  LZ4F_dctx_s *m_dctx = nullptr;
  LZ4F_cctx_s *m_cctx = nullptr;
  LZ4F_preferences_t m_lz4f_pref{};

  void init();

  public:

  Compression_lz4(Protocol_compression& c) :
    Compression_algorithm(c)
  { init(); }

  size_t compress(byte *src, size_t len) override;
  size_t uncompress(byte *dst, size_t dest_size, size_t compressed_size,
                    size_t &bytes_consumed) override;
  ~Compression_lz4();
};


class Compression_zstd : public Compression_algorithm
{
  ZSTD_DStream *m_u_zstd = nullptr; // Uncompression ZSTD stream
  ZSTD_CStream *m_c_zstd = nullptr; // Compression ZSTD stream

  void init();

  public:

  Compression_zstd(Protocol_compression& c) :
    Compression_algorithm(c)
  { init(); }

  size_t compress(byte *src, size_t len) override;
  size_t uncompress(byte *dst, size_t dest_size, size_t compressed_size,
                    size_t &bytes_consumed) override;
  ~Compression_zstd();
};


class Protocol_compression
{
  private:

    scoped_ptr<Compression_algorithm> m_algorithm;

    size_t   m_c_inp_size = 0; // Size of the buffer for compressed input data
    byte*    m_c_inp_buf = nullptr;  // Buffer for compressed input data (allocated in protobuf)
    size_t   m_c_inp_offset = 0;         // Offset inside compressed input buf

    /*
      Amount of uncompressed bytes left in the current compression
      frame (as set with set_rd_status).
    */

    size_t   m_u_total_size = 0;

    byte    *m_c_out_buf = nullptr; // Compression OUT buffer
    size_t   m_c_out_size = 0;      // Compression OUT buffer size

  public:

  Compression_type::value m_compression_type = Compression_type::NONE;

  Protocol_compression();

  ~Protocol_compression();

  /*
    Returns pointer to internal buffer for compressed input
    data that was requested by do_uncompress().
    The buffer is guaranteed to be big enough to hold
    requested amount of input.
  */

  inline byte *get_inp_buf()
  {
    if (m_c_inp_buf == nullptr)
      throw_error("Compression input buffer is not set");

    return m_c_inp_buf + m_c_inp_offset;
  }

  /*
    Set internal decompression frame to the given memory buffer
    (which contains compressed data). Only after that uncompress_request()
    calls can be made. Parameter uncompressed_size is the size of data
    after decompression.
  */

  inline void set_compressed_buf(byte *data, size_t compressed_size,
                                 size_t uncompressed_size)
  {
    reset();
    m_c_inp_buf = data;
    m_u_total_size = uncompressed_size;
    m_c_inp_size = compressed_size;
  }

  /*
    For compression we need the buffer size enough to hold
    the whole compressed message.

    Parameter size can be specified for resizing the buffer
    to the specific size. Otherwise the function returns
    the current buffer without resizing.
  */

  byte *get_out_buf(size_t size = 0);

  inline size_t get_out_buf_len()
  { return m_c_out_size; }


  inline void reset()
  {
    m_c_inp_offset = 0;
    m_u_total_size = 0;
    m_c_inp_size = 0;
  }

  /*
    Returns true if the current compression frame has been
    processed and there is no more data available in it.
  */
  inline bool uncompression_finished()
  {
    return (0 == m_u_total_size) &&
           (0 == m_c_inp_size);
  }

  /*
    Uncompresses data set by set_compressed_buf() function into the
    buffer of a given size.
    Returns true if requested amount of data was uncompressed.
    Otherwise returns false
  */
  bool uncompress(byte *buf, size_t size);

  /*
    Compresses the data located in src of the length len
    and returns the length of the compressed data
  */
  size_t do_compress(byte *src, size_t len);

  void set_compression_type(Compression_type::value compression_type);

  private:

  /*
    Attempts to uncompress dest_size bytes into dst buffer.
    Returns number of uncompressed bytes. If not all requested
    bytes have been uncompressed, prepares internal buffer for
    reading more compressed input, setting m_c_inp_chunk_size to
    the amount of input that should be read.
  */
  size_t do_uncompress(byte *dst, size_t dest_size);

};


}}}

#endif
