// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// Tell Flow about these globals.
declare var TextDecoder: ?Function;
declare var TextEncoder: ?Function;

import {
  TextDecoder as TextDecoderPolyfill,
  TextEncoder as TextEncoderPolyfill,
} from 'text-encoding-utf-8';

const TextDecoderImpl = typeof TextDecoder === 'function' ? TextDecoder : TextDecoderPolyfill;
const TextEncoderImpl = typeof TextEncoder === 'function' ? TextEncoder : TextEncoderPolyfill;

export {TextDecoderImpl as TextDecoder, TextEncoderImpl as TextEncoder};
