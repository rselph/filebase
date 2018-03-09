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
	filesPerBatch = 1024
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
	fatal(err)
	defaultDBPath = filepath.Join(usr.HomeDir, dbFile)

	flag.StringVar(&dbPath, "db", defaultDBPath, "Path to database file.")
	flag.Parse()

	cache= newFileDB(dbPath)
	defer cache.close()

	for _, dir := range flag.Args() {
		cache.scanDir(dir)
	}
}

func (fdb *fileDB) scanDir(dir string) {
	fdb.getFiles(dir)
	fdb.wg.Wait()
	_, err := fdb.flush.Exec()
	fatal(err)
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
		fatal(err)

		for info := range infos {

			fdb.insertOneSample(tx, info.p, info.i, info.now)
			i++
			if i % filesPerBatch == 0 {
				fmt.Print(".")
				err = tx.Commit()
				fatal(err)
				tx, err = fdb.db.Begin()
				fatal(err)
			}
		}

		err = tx.Commit()
		fatal(err)
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

func (fdb *fileDB) insertOneSample(tx *sql.Tx, path string, info os.FileInfo, now time.Time) {
	var err error
	var fileid int64

	err = tx.Stmt(fdb.getFileID).QueryRow(path).Scan(&fileid)
	if err == sql.ErrNoRows {
		res, err := tx.Stmt(fdb.insertFile).Exec(path)
		fatal(err)

		fileid, err = res.LastInsertId()
		fatal(err)

	}
	fatal(err)

	_, err = tx.Stmt(fdb.insertSample).Exec(fileid, now.Unix(), info.Mode(), info.Size(), info.ModTime().Unix())
	fatal(err)

	_, err = tx.Stmt(fdb.markFound).Exec(fileid)
	fatal(err)

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

func newFileDB(path string) (fdb *fileDB) {
	var err error

	fdb = &fileDB{}
	fdb.db, err = sql.Open("sqlite3", path)
	fatal(err)

	_, err = fdb.db.Exec(schema)
	fatal(err)

	fdb.getFileID, err = fdb.db.Prepare("SELECT fileid FROM file WHERE path = ?")
	fatal(err)

	fdb.insertFile, err = fdb.db.Prepare("INSERT INTO file (path) VALUES (?)")
	fatal(err)

	fdb.insertSample, err = fdb.db.Prepare(
		"INSERT INTO sample (fileid, sampletime, mode, size, mtime) VALUES (?,?,?,?,?)")
	fatal(err)

	fdb.markFound, err = fdb.db.Prepare("INSERT INTO found VALUES (?)")
	fatal(err)

	fdb.flush, err = fdb.db.Prepare("DELETE FROM file WHERE fileid NOT IN (SELECT fileid FROM found)")
	fatal(err)

	return
}

func (fdb *fileDB) close() {
	fdb.wg.Wait()
	fdb.db.Close()
}

func fatal(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
