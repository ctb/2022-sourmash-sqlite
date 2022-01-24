#! /usr/bin/env python
import sys
import os
import sourmash
import argparse
import sqlite3
import screed

from sourmash.sqlite_index import SqliteIndex

MAX_SQLITE_INT = 2 ** 63 - 1
sqlite3.register_adapter(
    int, lambda x: hex(x) if x > MAX_SQLITE_INT else x)
sqlite3.register_converter(
    'integer', lambda b: int(b, 16 if b[:2] == b'0x' else 10))

def main():
    p = argparse.ArgumentParser()
    p.add_argument('seqfiles', nargs='+')
    p.add_argument('-o', '--output', required=True, help='SQLite database')
    p.add_argument('--protein', action='store_true', default=True)
    p.add_argument('-k', '--ksize', type=int, default=10)
    p.add_argument('--scaled', type=int, default=200)
    p.add_argument('--combine-seqs', action='store_true', default=False)
    args = p.parse_args()

    assert args.protein         # for now

    template_mh = sourmash.MinHash(n=0, ksize=args.ksize,
                                   is_protein=args.protein,
                                   scaled=args.scaled)

    db = SqliteIndex(args.output)

    n = 0
    for seqfile in args.seqfiles:
        print(f"Loading sequences from {seqfile}")

        if args.combine_seqs:
            mh = template_mh.copy_and_clear()
            for record in screed.open(seqfile):
                mh.add_protein(record.sequence)

            ss = sourmash.SourmashSignature(mh, name=os.path.basename(seqfile),
                                            filename=seqfile)
            db.insert(ss)
            n += 1
        else:
            siglist = []
            for record in screed.open(seqfile):
                mh = template_mh.copy_and_clear()
                mh.add_protein(record.sequence)
                ss = sourmash.SourmashSignature(mh,
                                                name=record.name,
                                                filename=seqfile)
                db.insert(ss)
                n += 1

        db.conn.commit()
    print(f"Created {n} signatures and saved them to '{args.output}'")


if __name__ == '__main__':
    sys.exit(main())


