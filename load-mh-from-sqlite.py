#! /usr/bin/env python
import sys
import os
import sourmash
import argparse
import sqlite3


def main():
    p = argparse.ArgumentParser()
    p.add_argument('sqlite_db')
    p.add_argument('-o', '--output', required=True)
    args = p.parse_args()

    with sourmash.sourmash_args.SaveSignaturesToLocation(args.output) as sigout:
        db = sqlite3.connect(args.sqlite_db)
        c = db.cursor()
        c2 = db.cursor()
        c.execute("SELECT id, name, scaled, ksize, filename FROM sketches")
        for (id, name, scaled, ksize, filename) in c:
            mh = sourmash.MinHash(n=0, ksize=ksize, scaled=scaled)
            c2.execute("SELECT hashval FROM hashes WHERE sketch_id=?", (id,))

            for hashval, in c2:
                mh.add_hash(hashval)

            print('XXX', name)
            ss = sourmash.SourmashSignature(mh, name=name, filename=filename)
            sigout.add(ss)

    print(f"Saved {len(sigout)} signatures to location '{args.output}'")


if __name__ == '__main__':
    sys.exit(main())
