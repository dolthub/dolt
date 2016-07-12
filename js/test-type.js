
'use strict';
const noms = require('.');

function testList(n) {
  const a = new Array(n);
  for (let i = 0; i < a.length; i++) {
    a[i] = i;
  }
  a.push('a');

  let l = new noms.List(a);
  console.log(l.length, l.type.describe());

  l.remove(l.length - 1, l.length).then(ll => {
    l = ll;
    console.log(l.length, l.type.describe());

    l.remove(l.length - 1, l.length).then(ll => {
      l = ll;
      console.log(l.length, l.type.describe());
    });

  });
}

function testSet(n) {
  const a = new Array(n);
  for (let i = 0; i < a.length; i++) {
    a[i] = i;
  }
  a.push('a');

  let s = new noms.Set(a);
  console.log(s.size, s.type.describe());

  s.remove('a').then(ss => {
    s = ss;
    console.log(s.size, s.type.describe());

    s.remove(0).then(ss => {
      s = ss;
      console.log(s.size, s.type.describe());
    });

  });
}

function testMap(n) {
  const a = new Array(n);
  for (let i = 0; i < a.length; i++) {
    a[i] = [i, i];
  }
  a.push(['a', 'a']);

  let m = new noms.Map(a);
  // console.log(m.sequence);
  console.log(m.size, m.type.describe());

  m.remove('a').then(ss => {
    m = ss;
    console.log(m.size, m.type.describe());

    m.remove(0).then(ss => {
      m = ss;
      console.log(m.size, m.type.describe());
    });

  });
}


// testList(10);
// testSet(10);
testMap(10);
// testList(10000);
// testSet(10000);
testMap(100);
