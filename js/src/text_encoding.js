/* @flow */

// Tell Flow about these globals.
declare var TextDecoder: ?Function;
declare var TextEncoder: ?Function;

import {
  TextDecoder as TextDecoderPolyfill,
  TextEncoder as TextEncoderPolyfill
} from 'text-encoding-utf-8';

const TextDecoderImpl = typeof TextDecoder === 'function' ? TextDecoder : TextDecoderPolyfill;
const TextEncoderImpl = typeof TextEncoder === 'function' ? TextEncoder : TextEncoderPolyfill;

export {TextDecoderImpl as TextDecoder, TextEncoderImpl as TextEncoder};
