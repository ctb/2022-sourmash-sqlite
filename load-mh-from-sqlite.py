#! /usr/bin/env python
import sys
import os
import sourmash
import argparse
import sqlite3


MAX_SQLITE_INT = 2 ** 63 - 1
sqlite3.register_adapter(
    int, lambda x: hex(x) if x > MAX_SQLITE_INT else x)
sqlite3.register_converter(
    'integer', lambda b: int(b, 16 if b[:2] == b'0x' else 10))


def main():
    p = argparse.ArgumentParser()
    p.add_argument('sqlite_db')
    p.add_argument('-o', '--output', required=True)
    args = p.parse_args()

    with sourmash.sourmash_args.SaveSignaturesToLocation(args.output) as sigout:
        db = sqlite3.connect(args.sqlite_db)
        c = db.cursor()
        c2 = db.cursor()
        c.execute("SELECT id, name, num, scaled, ksize, filename, is_dna, is_protein, is_dayhoff, is_hp, track_abundance, seed FROM sketches")
        for (id, name, num, scaled, ksize, filename, is_dna, is_protein, is_dayhoff, is_hp, track_abundance, seed) in c:
            mh = sourmash.MinHash(n=num, ksize=ksize, scaled=scaled, seed=seed, is_protein=is_protein, dayhoff=is_dayhoff, hp=is_hp, track_abundance=track_abundance)
            c2.execute("SELECT hashval FROM hashes WHERE sketch_id=?", (id,))

            for hashval, in c2:
                mh.add_hash(hashval)

            print('XXX', name)
            ss = sourmash.SourmashSignature(mh, name=name, filename=filename)
            sigout.add(ss)

    print(f"Saved {len(sigout)} signatures to location '{args.output}'")


if __name__ == '__main__':
    sys.exit(main())
