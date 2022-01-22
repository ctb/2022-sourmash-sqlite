#! /usr/bin/env python
import sys
import os
import sourmash
import argparse
import sqlite3


def main():
    p = argparse.ArgumentParser()
    p.add_argument('sigfile')
    p.add_argument('sqlite_db')
    args = p.parse_args()

    assert not os.path.exists(args.sqlite_db)

    siglist = sourmash.load_file_as_signatures(args.sigfile)

    db = sqlite3.connect(args.sqlite_db)
    c = db.cursor()

    c.execute("CREATE TABLE IF NOT EXISTS sketches (id INTEGER PRIMARY KEY, name TEXT, scaled INTEGER NOT NULL, ksize INTEGER NOT NULL, filename TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS hashes (hashval INTEGER NOT NULL, sketch_id INTEGER NOT NULL, FOREIGN KEY (sketch_id) REFERENCES sketches (id))")

    n = 0
    for ss in siglist:
        c.execute("INSERT INTO sketches (name, scaled, ksize, filename) VALUES (?, ?, ?, ?)", (ss.name, ss.minhash.scaled, ss.minhash.ksize, ss.filename))
        c.execute("SELECT last_insert_rowid()")
        id, = c.fetchone()
        for h in ss.minhash.hashes:
            c.execute("INSERT INTO hashes (hashval, sketch_id) VALUES (?, ?)", (h, id))
        n += 1
    db.commit()

    print(f"Loaded {n + 1} signatures and saved them to '{args.sqlite_db}'")


if __name__ == '__main__':
    sys.exit(main())


