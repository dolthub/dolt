// +build !cgo
// g++ -Wall `pkg-config fuse --cflags --libs` statfs.cc -o statfs

#include <unordered_map>
#include <string>

using std::string;
using std::unordered_map;

#define FUSE_USE_VERSION 26

extern "C" {
#include <fuse.h>
}

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

useconds_t delay_usec;

class StatFs {
public:
  void readFrom(const string& fn);
  unordered_map<string, bool> is_dir_;

  int GetAttr(const char *name, struct stat *statbuf) {
    if (strcmp(name, "/") == 0) {
      statbuf->st_mode = S_IFDIR | 0777;
      return 0;
    }
    unordered_map<string, bool>::const_iterator it(is_dir_.find(name));
    if (it == is_dir_.end()) {
      return -ENOENT;
    }

    if (it->second) {
      statbuf->st_mode = S_IFDIR | 0777; 
   } else {
      statbuf->st_nlink = 1;
      statbuf->st_mode = S_IFREG | 0666;
    }

    if (delay_usec > 0) {
      usleep(delay_usec);
    }
                         
    return 0;
  }
  
};

StatFs *global;
int global_getattr(const char *name, struct stat *statbuf) {
  return global->GetAttr(name, statbuf);
}

void StatFs::readFrom(const string& fn) {
  FILE *f = fopen(fn.c_str(), "r");

  char line[1024];
  while (char *s = fgets(line, sizeof(line), f)) {
    int l = strlen(s);
    if (line[l-1] == '\n') {
      line[l-1] = '\0';
      l--;
    }
    bool is_dir = line[l-1] == '/';
    if (is_dir) {
      line[l-1] = '\0';
    }
    is_dir_[line] = is_dir;
  }
  fclose(f);
}

int main(int argc, char *argv[])
{
  global = new StatFs;
  // don't want to know about fuselib's option handling
  char *in = getenv("STATFS_INPUT");
  if (!in || !*in) {
    fprintf(stderr, "pass file in $STATFS_INPUT\n");
    exit(2);
  }
  
  global->readFrom(in);
  in = getenv("STATFS_DELAY_USEC");
  if (in != NULL) {
    delay_usec = atoi(in);
  }
  
  struct fuse_operations statfs_oper  = {0};
  statfs_oper.getattr = &global_getattr;
  return fuse_main(argc, argv, &statfs_oper, NULL);
}
