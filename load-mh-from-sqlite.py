#! /usr/bin/env python
import sys
import os
import sourmash
import argparse
import sqlite3


from sourmash.sqlite_index import SqliteIndex


def main():
    p = argparse.ArgumentParser()
    p.add_argument('sqlite_db')
    p.add_argument('-o', '--output', required=True)
    args = p.parse_args()

    with sourmash.sourmash_args.SaveSignaturesToLocation(args.output) as sigout:
        idx = SqliteIndex(args.sqlite_db)
        for ss in idx.signatures():
            sigout.add(ss)

    print(f"Saved {len(sigout)} signatures to location '{args.output}'")


if __name__ == '__main__':
    sys.exit(main())
