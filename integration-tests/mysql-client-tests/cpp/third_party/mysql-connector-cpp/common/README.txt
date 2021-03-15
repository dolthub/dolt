Copyright (c) 2017, 2019, Oracle and/or its affiliates. All rights reserved.

This folder contains common code used by X DevAPI and XAPI implementations.
The code is not meant to be used separately.

Most of the common code is put in headers and some of it is in the common
library. The headers in this folder are meant to be used only by
the implementation side - they should not be included from public headers.
Common definitions that are needed in public API definitions are included with
<include/mysqlx_common.h>.
