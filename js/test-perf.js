'use strict';

const noms = require('.');

function test(n) {
  const values = [];
  for (let i = 0; i < n; i++) {
    const s = noms.newStruct('', {
      b: i % 2 === 0,
      n: i,
      s: String(i),
    });
    values.push(s);
  }

  const db = noms.DatabaseSpec.parse('mem').database();

  const d1 = Date.now();
  const l = new noms.List(values);
  console.log('Create list', Date.now() - d1);

  const d2 = Date.now();
  const r = db.writeValue(l);
  console.log('Write list', Date.now() - d2);
  //
  // const d3 = Date.now();
  // return db.readValue(r.hash).then(l => {
  //   console.log(l);
  //   console.log('Read list', Date.now() - d3);
  // });
}

test(50000);//.catch(err => console.error(err));
