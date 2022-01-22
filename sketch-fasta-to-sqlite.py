#! /usr/bin/env python
import sys
import os
import sourmash
import argparse
import sqlite3
import screed

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

    db = sqlite3.connect(args.output)
    c = db.cursor()

    c.execute("PRAGMA cache_size=1000000")
    c.execute("PRAGMA synchronous = OFF")
    c.execute("PRAGMA journal_mode = MEMORY")

    c.execute("CREATE TABLE IF NOT EXISTS sketches (id INTEGER PRIMARY KEY, name TEXT, num INTEGER NOT NULL, scaled INTEGER NOT NULL, ksize INTEGER NOT NULL, filename TEXT, is_dna BOOLEAN, is_protein BOOLEAN, is_dayhoff BOOLEAN, is_hp BOOLEAN, track_abundance BOOLEAN, seed INTEGER NOT NULL)")
    c.execute("CREATE TABLE IF NOT EXISTS hashes (hashval INTEGER NOT NULL, sketch_id INTEGER NOT NULL, FOREIGN KEY (sketch_id) REFERENCES sketches (id))")

    for seqfile in args.seqfiles:
        print(f"Loading sequences from {seqfile}")

        if args.combine_seqs:
            mh = template_mh.copy_and_clear()
            for record in screed.open(seqfile):
                mh.add_protein(record.sequence)

            ss = sourmash.SourmashSignature(mh, name=os.path.basename(seqfile),
                                            filename=seqfile)
            siglist = [ss]
        else:
            siglist = []
            for record in screed.open(seqfile):
                mh = template_mh.copy_and_clear()
                mh.add_protein(record.sequence)
                ss = sourmash.SourmashSignature(mh,
                                                name=record.name,
                                                filename=seqfile)
                siglist.append(ss)

        n = 0
        for ss in siglist:
            c.execute("INSERT INTO sketches (name, num, scaled, ksize, filename, is_dna, is_protein, is_dayhoff, is_hp, track_abundance, seed) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (ss.name, ss.minhash.num, ss.minhash.scaled, ss.minhash.ksize, ss.filename, ss.minhash.is_dna, ss.minhash.is_protein, ss.minhash.dayhoff, ss.minhash.hp, ss.minhash.track_abundance, ss.minhash.seed))
            c.execute("SELECT last_insert_rowid()")
            id, = c.fetchone()
            for h in ss.minhash.hashes:
                c.execute("INSERT INTO hashes (hashval, sketch_id) VALUES (?, ?)", (h, id))
            n += 1
    db.commit()
    print(f"Created {n} signatures and saved them to '{args.output}'")


if __name__ == '__main__':
    sys.exit(main())


