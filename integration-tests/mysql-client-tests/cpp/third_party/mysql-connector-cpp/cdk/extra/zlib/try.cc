#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "zlib.h"

#define CHUNK 255

int main(int, char **)
{
  int ret;
  unsigned len_uncompressed = 0, len_compressed = 0, i;
  z_stream strm;
  char in[CHUNK] = "ZIP compression Test example string";
  char out[CHUNK];
  unsigned char tbuff[CHUNK] = { 120,218,227,226,12,118,245,113,117,14,81,48,52,146,98,46,46,204,1,0,28,19,3,196 };

  /* allocate deflate state */
  strm.zalloc = Z_NULL;
  strm.zfree = Z_NULL;
  strm.opaque = Z_NULL;

  ret = deflateInit(&strm, 9);
  if (ret != Z_OK)
    return ret;

  printf("Uncompressed buffer boundary: %ld",
         deflateBound(&strm, 1000000));

  len_uncompressed = (unsigned)strlen(in) + 1; // Compress \0 byte as well
  strm.avail_in = len_uncompressed;
  strm.next_in = (Bytef*)in;
  strm.avail_out = CHUNK;
  strm.next_out = (Bytef*)out;
  ret = deflate(&strm, Z_FINISH);
  assert(ret != Z_STREAM_ERROR);

  (void)deflateEnd(&strm);
  printf("Compressing.....\n");
  printf("Input string: %s\n", in);
  printf("Compressed data HEX: ");
  len_compressed = CHUNK - strm.avail_out;
  for (i = 0; i < len_compressed; ++i)
  {
    printf("%02X ", (unsigned char)out[i]);
  }
  printf("\n");
  memset(in, 0, sizeof(in));

  printf("Uncompressing.....\n");

  ret = inflateInit(&strm);
  if (ret != Z_OK)
    return ret;

  //strm.avail_in = CHUNK - strm.avail_out;
  //strm.next_in = (Bytef*)out;

  strm.avail_in = 24;
  strm.next_in = (Bytef*)tbuff;

  strm.avail_out = CHUNK;
  memset(in, 0, 255);
  strm.next_out = (Bytef*)in;
  ret = inflate(&strm, Z_FINISH);
  assert(ret != Z_STREAM_ERROR);

  len_uncompressed = CHUNK - strm.avail_out;
  (void)inflateEnd(&strm);
  //printf("Uncompressed data TEXT: %s", in);

  printf("\n\n");
  for (int z = 0; z < 24; ++z)
  {
    printf("%02X ", tbuff[z]);
  }
  return Z_OK;
}