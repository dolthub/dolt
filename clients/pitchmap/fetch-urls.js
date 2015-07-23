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
  }
]).run();

var rootURL = args.options.url
var printRegexp = new RegExp(args.options.print);
var rejectRegexp = new RegExp(args.options.reject);

var crawler = require("simplecrawler").crawl(rootURL);
crawler.maxDepth = 3;
crawler.addFetchCondition(function(url) {
  var url = url.protocol + '://' + url.host + url.uriPath;
  var print = !!url.match(printRegexp);
  var reject = url.indexOf(rootURL) < 0 || !!url.match(rejectRegexp);

  if (print) {
    console.log(url);
  }

  return !reject;
});
