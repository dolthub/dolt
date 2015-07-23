var argv = require( 'argv' );
var args = argv.option([
  {
    name: 'url',
    type: 'string',
    description: 'URL to start fetching from'
  },
  {
    name: 'print',
    type: 'string',
    description: 'print urls found in files which match this regexp'
  },
  {
    name: 'reject',
    type: 'string',
    description: 'dont follow urls which match this regexp'
  },
  {
    name: 'debug',
    type: 'bool',
    description: 'print the urls which are being fetched and note which urls would be printed'
  }
]).run();


var rootURL = args.options.url;
var printRegexp = args.options.print && new RegExp(args.options.print);
var rejectRegexp = new RegExp(args.options.reject);
var debug = args.options.debug;

var crawler = require("simplecrawler").crawl(rootURL);
crawler.maxDepth = 100;
crawler.addFetchCondition(function(url) {
  var url = url.protocol + '://' + url.host + url.uriPath;
  var print = printRegexp && !!url.match(printRegexp);
  var reject = url.indexOf(rootURL) < 0 || !!url.match(rejectRegexp);

  if (debug) {
    if (!reject) {
      console.log(url);
    }
    if (print) {
      console.log('> ' + url);
    }
  } else if (print) {
    console.log(url);
  }

  return !reject;
});
