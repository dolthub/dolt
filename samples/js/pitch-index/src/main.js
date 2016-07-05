// @flow

// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import argv from 'yargs';
import {
  DatasetSpec,
  invariant,
  List,
  Map as NomsMap,
  Ref,
  Struct,
  newStruct,
} from '@attic/noms';
import type {Value} from '@attic/noms';

const args = argv
  .usage('Usage: $0 <input-dataset> <output-dataset>')
  .demand(2, 'You must provide both a dataset to read from, and one to write to.')
  .argv;

main().catch(ex => {
  console.error(ex.stack);
  process.exit(1);
});

type XMLElement = NomsMap<string, NomsMap<string, any>>;

async function main(): Promise<void> {
  const inSpec = DatasetSpec.parse(args._[0]);
  invariant(inSpec, quit('invalid input dataset spec'));
  const outSpec = DatasetSpec.parse(args._[1]);
  invariant(outSpec, quit('invalid input dataset spec'));

  const input = inSpec.dataset();
  const hv = await input.headValue();
  invariant(hv, quit(`${args._[0]} does not exist}`));

  const pitchers = new Map();
  const inningPs = [];
  const playerPs = [];
  invariant(hv instanceof NomsMap);
  await hv.forEach((ref: Ref<XMLElement>) => {
    // We force elemP to be 'any' here because the 'inning' entry and the 'Player' entry have
    // different types that involve multiple levels of nested maps OR strings.
    const elemP = ref.targetValue(input.database);
    inningPs.push(maybeProcessInning(elemP));
    playerPs.push(maybeProcessPitcher(elemP, pitchers));
  });

  await Promise.all(playerPs);
  const pitcherPitches = new Map();
  for (const m of await Promise.all(inningPs)) {
    if (m) {
      for (const [pitcherID, pitches] of m) {
        const pitcher = pitchers.get(pitcherID);
        invariant(pitcher);
        pitcherPitches.set(pitcher, extendArray(pitches, pitcherPitches.get(pitcher)));
      }
    }
  }
  const mapData = [];
  for (const [pitcher, pitches] of pitcherPitches) {
    mapData.push([pitcher, new List(pitches)]);
  }
  await outSpec.dataset().commit(new NomsMap(mapData));
}

async function maybeProcessPitcher(ep: Promise<XMLElement>, pitchers: Map<string, string>):
  Promise<void> {
  const player = await (await ep).get('Player');
  if (player) {
    const [id, first, last] = await Promise.all([
      player.get('-id'), player.get('-first_name'), player.get('-last_name')]);
    pitchers.set(id, last + ', ' + first);
  }
}

type PitcherPitches = Map<string, Array<Struct>>;

function mergeInto(a: PitcherPitches, b: ?PitcherPitches) {
  if (!b) {
    return a;
  }
  for (const [pitcher, pitches] of b) {
    a.set(pitcher, extendArray(pitches, a.get(pitcher)));
  }
}

function maybeProcessInning(ep: Promise<XMLElement>): Promise<?Map<string, Array<Struct>>> {
  return ep.then(elem => elem.get('inning')).then(inn => inn && processInning(inn));
}

function processInning(inning: NomsMap<string, NomsMap<*, *>>):
    Promise<Map<string, Array<Struct>>> {
  return Promise.all([inning.get('top'), inning.get('bottom')])
    .then(halves => {
      const halfPs = [];
      for (const half of halves) {
        if (half) {
          halfPs.push(half.get('atbat'));
        }
      }
      return Promise.all(halfPs);
    })
    .then(abData => {
      const abPs = [];
      for (const abs of abData) {
        abPs.push(processAbs(normalize(abs)));
      }
      return Promise.all(abPs);
    })
    .then(pitcherPitchList => {
      // any because of Flow.
      const ret: any = new Map();
      for (const pitcherPitches of pitcherPitchList) {
        mergeInto(ret, pitcherPitches);
      }
      return ret;
    });
}

function processAbs(abs: List): Promise<PitcherPitches> {
  const ps = [];
  return abs.forEach(ab => {
    ps.push(
      Promise.all([ab.get('-pitcher'), ab.get('pitch')])
      .then(([pitcher, d]) => Promise.all([pitcher, processPitches(normalize(d))]))
    );
  })
  .then(() => Promise.all(ps))
  .then(abdata => {
    const pitchCounts = new Map();
    for (const [pitcher, pitches] of abdata) {
      if (pitches.length > 0) {
        pitchCounts.set(pitcher, extendArray(pitchCounts.get(pitcher), pitches));
      }
    }
    return pitchCounts;
  });
}

function extendArray(a = [], b = []) {
  b.forEach(e => a.push(e));
  return a;
}

function normalize<T: Value>(d: ?T | List<T>): List<T> {
  if (!d) {
    return new List();
  }
  if (d instanceof List) {
    return d;
  }
  return new List([d]);
}

type PitchData = NomsMap<string, string>;

function processPitches(d: List<PitchData>): Promise<Array<Struct>> {
  const pitchPs = [];
  return d.forEach((p: PitchData) => {
    pitchPs.push(getPitch(p));
  })
  .then(() => pitchPs)
  .then(pitchPs => Promise.all(pitchPs))
  .then(pitches => pitches.filter((e: ?Struct): boolean => !!e));
}

function getPitch(p: PitchData): Promise<?Struct> {
  return Promise.all([p.get('-px'), p.get('-pz')]).then(([xStr, zStr]) => {
    if (!xStr || !zStr) {
      return;
    }
    const [x, z] = [Number(xStr), Number(zStr)];
    invariant(!isNaN(x), x + ' should be a number');
    invariant(!isNaN(z), z + ' should be a number');
    return newStruct('Pitch', {x, z});
  });
}

function quit(err: string): () => void {
  return () => {
    process.stderr.write(err + '\n');
    process.exit(1);
  };
}
