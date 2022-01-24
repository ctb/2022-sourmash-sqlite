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
    p.add_argument('sigfiles', nargs='+')
    p.add_argument('-o', '--output', required=True, help='SQLite database')
    args = p.parse_args()

    db = sqlite3.connect(args.output)
    c = db.cursor()

    c.execute("CREATE TABLE IF NOT EXISTS sketches (id INTEGER PRIMARY KEY, name TEXT, num INTEGER NOT NULL, scaled INTEGER NOT NULL, ksize INTEGER NOT NULL, filename TEXT, is_dna BOOLEAN, is_protein BOOLEAN, is_dayhoff BOOLEAN, is_hp BOOLEAN, track_abundance BOOLEAN, seed INTEGER NOT NULL)")
    c.execute("CREATE TABLE IF NOT EXISTS hashes (hashval INTEGER NOT NULL, sketch_id INTEGER NOT NULL, FOREIGN KEY (sketch_id) REFERENCES sketches (id))")

    n = 0
    for sigfile in args.sigfiles:
        siglist = sourmash.load_file_as_signatures(sigfile)

        for ss in siglist:
            c.execute("INSERT INTO sketches (name, num, scaled, ksize, filename, is_dna, is_protein, is_dayhoff, is_hp, track_abundance, seed) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (ss.name, ss.minhash.num, ss.minhash.scaled, ss.minhash.ksize, ss.filename, ss.minhash.is_dna, ss.minhash.is_protein, ss.minhash.dayhoff, ss.minhash.hp, ss.minhash.track_abundance, ss.minhash.seed))
            c.execute("SELECT last_insert_rowid()")
            id, = c.fetchone()
            for h in ss.minhash.hashes:
                c.execute("INSERT INTO hashes (hashval, sketch_id) VALUES (?, ?)", (h, id))
            n += 1
        db.commit()

    print(f"Loaded {n} signatures and saved them to '{args.output}'")


if __name__ == '__main__':
    sys.exit(main())
pp

