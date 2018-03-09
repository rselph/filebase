package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	dbFile = ".filebase.sqlite3"
	filesPerBatch = 512
)

const schema = `
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS file (
        fileid integer primary key,
        path text
);
CREATE UNIQUE INDEX IF NOT EXISTS filepath on file(path);

CREATE TABLE IF NOT EXISTS sample (
        fileid integer,
        sampletime integer,
        mode integer,
        size integer,
        mtime integer,
        primary key (fileid, sampletime),
        foreign key (fileid) references file(fileid) ON UPDATE RESTRICT ON DELETE CASCADE
);

CREATE TEMPORARY TABLE found (fileid integer PRIMARY KEY );
`

var (
	cache         *fileDB
	defaultDBPath string
	dbPath        string
)

func main() {
	usr, err := user.Current()
	if err != nil {
		log.Fatal(err)
	}
	defaultDBPath = filepath.Join(usr.HomeDir, dbFile)

	flag.StringVar(&dbPath, "db", defaultDBPath, "Path to database file.")
	flag.Parse()

	cache, err = newFileDB(dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer cache.close()

	for _, dir := range flag.Args() {
		cache.scanDir(dir)
	}
}

func (fdb *fileDB) scanDir(dir string) {
	fdb.getFiles(dir)
	fdb.wg.Wait()
	_, err := fdb.flush.Exec()
	if err != nil {
		log.Fatal(err)
	}
}

func (fdb *fileDB) getFiles(dir string) {
	canonicalPath, err := filepath.Abs(dir)
	if err != nil {
		log.Print(err)
		return
	}
	canonicalPath, err = filepath.EvalSymlinks(canonicalPath)
	if err != nil {
		log.Print(err)
		return
	}

	type insertJob struct {
		now time.Time
		i   os.FileInfo
		p   string
	}
	infos := make(chan *insertJob)
	defer close(infos)

	fdb.wg.Add(1)
	go func() {
		defer fdb.wg.Done()

		var i int

		tx, err := fdb.db.Begin()
		if err != nil {
			log.Fatal(err)
		}

		for info := range infos {
			//fmt.Println(info)
			fmt.Print(".")

			err = fdb.insertOneSample(tx, info.p, info.i, info.now)
			i++
			if err == nil {
				if i % filesPerBatch == 0 {
					fmt.Println()
					err = tx.Commit()
					if err != nil {
						log.Fatal(err)
					}
					tx, err = fdb.db.Begin()
					if err != nil {
						log.Fatal(err)
					}
				}
			} else {
				tx.Rollback()
				log.Fatal(err)
			}
		}

		err = tx.Commit()
		if err != nil {
			log.Fatal(err)
		}
	}()

	filepath.Walk(canonicalPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Print(err)
			return nil
		}

		if info.Mode().IsRegular() {
			infos <- &insertJob{now: time.Now(), i: info, p: path}
		}

		return nil
	})
}

func (fdb *fileDB) insertOneSample(tx *sql.Tx, path string, info os.FileInfo, now time.Time) (err error) {
	var fileid int64

	err = tx.Stmt(fdb.getFileID).QueryRow(path).Scan(&fileid)
	if err == sql.ErrNoRows {
		res, err := tx.Stmt(fdb.insertFile).Exec(path)
		if err != nil {
			return err
		}
		fileid, err = res.LastInsertId()
		if err != nil {
			return err
		}
	} else if err != nil {
		return
	}

	_, err = tx.Stmt(fdb.insertSample).Exec(fileid, now.Unix(), info.Mode(), info.Size(), info.ModTime().Unix())
	if err != nil {
		return
	}

	_, err = tx.Stmt(fdb.markFound).Exec(fileid)
	return
}

type fileDB struct {
	db *sql.DB
	wg sync.WaitGroup

	getFileID    *sql.Stmt
	insertFile   *sql.Stmt
	insertSample *sql.Stmt
	markFound    *sql.Stmt
	flush        *sql.Stmt
}

func newFileDB(path string) (fdb *fileDB, err error) {
	fdb = &fileDB{}

	fdb.db, err = sql.Open("sqlite3", path)
	if err != nil {
		return
	}

	_, err = fdb.db.Exec(schema)
	if err != nil {
		return
	}

	fdb.getFileID, err = fdb.db.Prepare("SELECT fileid FROM file WHERE path = ?")
	if err != nil {
		return
	}

	fdb.insertFile, err = fdb.db.Prepare("INSERT INTO file (path) VALUES (?)")
	if err != nil {
		return
	}

	fdb.insertSample, err = fdb.db.Prepare(
		"INSERT INTO sample (fileid, sampletime, mode, size, mtime) VALUES (?,?,?,?,?)")
	if err != nil {
		return
	}

	fdb.markFound, err = fdb.db.Prepare("INSERT INTO found VALUES (?)")
	if err != nil {
		return
	}

	fdb.flush, err = fdb.db.Prepare("DELETE FROM file WHERE fileid NOT IN (SELECT fileid FROM found)")
	if err != nil {
		return
	}

	return
}

func (fdb *fileDB) close() {
	fdb.wg.Wait()
	fdb.db.Close()
}
