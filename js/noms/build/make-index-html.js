// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

'use strict';

const fs = require('fs-extra');
const path = require('path');

const outDir = path.join(__dirname, '..', 'generated-docs', 'js');

fs.mkdirsSync(outDir);
const version = require('../package.json').version;
const out = path.join(outDir, 'index.html');
fs.writeFileSync(out, `<script>
location.replace('./${version}/' + location.hash);
</script>`);
