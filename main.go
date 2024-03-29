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

	"math"

	_ "github.com/mattn/go-sqlite3"
)

const (
	dbFile        = ".filebase.sqlite3"
	filesPerBatch = 1024
)

const schema = `
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS dir (
		dirid integer PRIMARY KEY,
		dirpath text
);

CREATE TABLE IF NOT EXISTS file (
        fileid integer PRIMARY KEY,
        dirid integer,
        path text,
        FOREIGN KEY (dirid) REFERENCES dir(dirid) ON UPDATE RESTRICT ON DELETE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS filediridpath ON file(dirid, path);

CREATE TABLE IF NOT EXISTS sample (
        fileid integer,
        sampletime integer,
        mode integer,
        size integer,
        mtime integer,
        PRIMARY KEY (fileid, sampletime),
        FOREIGN KEY (fileid) REFERENCES file(fileid) ON UPDATE RESTRICT ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS samplesize ON sample(size);
CREATE INDEX IF NOT EXISTS samplemtime ON sample(mtime);

create view IF NOT EXISTS times as
    SELECT file.dirid, sample.fileid, sampletime, mode, size, mtime, max(sampletime) as maxtime, min(sampletime) as mintime
    from sample, file, dir
    where file.fileid == sample.fileid and file.dirid = dir.dirid
    group by sample.fileid;

create view IF NOT EXISTS rates AS
    SELECT *,
      ((select size from sample WHERE sampletime = maxtime and sample.fileid = times.fileid)-
       (select size from sample WHERE sampletime = mintime and sample.fileid = times.fileid)) / 
      cast(maxtime-mintime AS real) as rate
    from times;

CREATE TEMPORARY TABLE found (fileid integer PRIMARY KEY );
`

var (
	cache         *fileDB
	defaultDBPath string
	dbPath        string

	noScan    bool
	doBiggest bool
	doOldest  bool
	doNewest  bool
	doFastest bool
	listSize  int
)

func main() {
	usr, err := user.Current()
	fatal(err)
	defaultDBPath = filepath.Join(usr.HomeDir, dbFile)

	flag.StringVar(&dbPath, "db", defaultDBPath, "Path to database file.")
	flag.BoolVar(&doBiggest, "biggest", false, "Search for biggest files.")
	flag.BoolVar(&doFastest, "fastest", false, "Search for fastest growing files.")
	flag.BoolVar(&doOldest, "oldest", false, "Search for oldest files.")
	flag.BoolVar(&doNewest, "newest", false, "Search for newest files.")
	flag.BoolVar(&noScan, "noscan", false, "Don't rescan.  Just use the existing database.")
	flag.IntVar(&listSize, "list", 25, "How many files to list.")
	flag.Parse()

	cache = newFileDB(dbPath)
	defer cache.close()

	for _, dir := range flag.Args() {
		dirid := cache.getDirID(dir)

		if !noScan {
			cache.scanDir(dirid)
		}

		if doBiggest {
			fmt.Println("*** BIGGEST FILES ***")
			bigFiles := cache.getBiggest(dirid, listSize)
			for _, bigFile := range bigFiles {
				fmt.Println(bigFile.String())
			}
			fmt.Println()
		}

		if doOldest {
			fmt.Println("*** OLDEST FILES ***")
			oldFiles := cache.getOldest(dirid, listSize)
			for _, oldFile := range oldFiles {
				fmt.Println(oldFile.String())
			}
			fmt.Println()
		}

		if doNewest {
			fmt.Println("*** NEWEST FILES ***")
			newFiles := cache.getNewest(dirid, listSize)
			for _, newFile := range newFiles {
				fmt.Println(newFile.String())
			}
			fmt.Println()
		}

		if doFastest {
			fmt.Println("*** FASTEST GROWING FILES ***")
			newFiles := cache.getFastest(dirid, listSize)
			for _, fastFile := range newFiles {
				fmt.Println(fastFile.String())
			}
			fmt.Println()
		}
	}

}

func (fdb *fileDB) scanDir(dirid int64) {
	err := fdb.getFiles(dirid)
	if err != nil {
		return
	}

	fdb.wg.Wait()
	_, err = fdb.db.Exec("DELETE FROM file WHERE dirid = ? AND fileid NOT IN (SELECT fileid FROM found)", dirid)
	fatal(err)
}

func canonical(dir string) (canonicalPath string) {
	canonicalPath, err := filepath.Abs(dir)
	if err != nil {
		log.Fatal(err)
	}
	canonicalPath, err = filepath.EvalSymlinks(canonicalPath)
	if err != nil {
		log.Fatal(err)
	}
	return
}

func (fdb *fileDB) getDirID(dir string) (dirid int64) {
	canonicalPath := canonical(dir)
	err := fdb.db.QueryRow("SELECT dirid FROM dir WHERE dirpath = ?", canonicalPath).Scan(&dirid)
	if err == sql.ErrNoRows {
		res, err := fdb.db.Exec("INSERT INTO dir (dirpath) VALUES (?)", canonicalPath)
		fatal(err)
		dirid, err = res.LastInsertId()
		fatal(err)
	} else {
		fatal(err)
	}
	return
}

func (fdb *fileDB) getDirPath(dirid int64) (canonicalPath string) {
	err := fdb.db.QueryRow("SELECT dirpath FROM dir WHERE dirid = ?", dirid).Scan(&canonicalPath)
	fatal(err)
	return
}

func (fdb *fileDB) getFiles(dirid int64) (err error) {
	canonicalPath := fdb.getDirPath(dirid)

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

			fdb.insertOneSample(dirid, tx, info.p, info.i, info.now)
			i++
			if i%filesPerBatch == 0 {
				fmt.Print(".")
				err = tx.Commit()
				fatal(err)
				tx, err = fdb.db.Begin()
				fatal(err)
			}
		}
		fmt.Println()

		err = tx.Commit()
		fatal(err)
	}()

	filepath.Walk(canonicalPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Println()
			log.Print(err)
			return nil
		}

		if info.Mode().IsRegular() {
			infos <- &insertJob{now: time.Now(), i: info, p: path}
		}

		return nil
	})

	return
}

func (fdb *fileDB) insertOneSample(dirid int64, tx *sql.Tx, path string, info os.FileInfo, now time.Time) {
	var err error
	var fileid int64

	err = tx.Stmt(fdb.getFileID).QueryRow(dirid, path).Scan(&fileid)
	if err == sql.ErrNoRows {
		res, err := tx.Stmt(fdb.insertFile).Exec(dirid, path)
		fatal(err)

		fileid, err = res.LastInsertId()
		fatal(err)

	} else {
		fatal(err)
	}

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
}

func newFileDB(path string) (fdb *fileDB) {
	var err error

	fdb = &fileDB{}
	fdb.db, err = sql.Open("sqlite3", path)
	if err != nil {
		log.Fatal(err)
	}

	_, err = fdb.db.Exec(schema)
	fatal(err)

	fdb.getFileID, err = fdb.db.Prepare("SELECT fileid FROM file WHERE dirid = ? AND path = ?")
	fatal(err)

	fdb.insertFile, err = fdb.db.Prepare("INSERT INTO file (dirid, path) VALUES (?,?)")
	fatal(err)

	fdb.insertSample, err = fdb.db.Prepare(
		"INSERT INTO sample (fileid, sampletime, mode, size, mtime) VALUES (?,?,?,?,?)")
	fatal(err)

	fdb.markFound, err = fdb.db.Prepare("INSERT INTO found VALUES (?)")
	fatal(err)

	return
}

type fileEnt struct {
	path  string
	when  time.Time
	mode  int
	size  int64
	mtime time.Time
	rate  float64
}

const secondsPerDay = 3600 * 24

func (f *fileEnt) String() string {
	rateString := ""
	if f.rate != 0.0 {
		rateString = fmt.Sprintf("%vB/day\t", niceSizef(f.rate*secondsPerDay))
	}
	return fmt.Sprintf("%v\t%o\t%v\t%s%v", f.mtime, f.mode, niceSize(f.size), rateString, f.path)
}

func (f *fileEnt) Scan(r *sql.Rows) {
	var when, mtime int64
	r.Scan(&f.path, &when, &f.mode, &f.size, &mtime)
	f.when = time.Unix(when, 0)
	f.mtime = time.Unix(mtime, 0)
}

func rowsToResults(r *sql.Rows, n int) []fileEnt {
	defer r.Close()

	result := make([]fileEnt, n)
	i := 0
	for r.Next() && i < n {
		result[i].Scan(r)
		i++
	}

	return result[0:i]
}

func (fdb *fileDB) getBiggest(dirid int64, n int) []fileEnt {
	rows, err := fdb.db.Query(
		`select path, sampletime, mode, size, mtime from file, sample 
		where file.fileid=sample.fileid and 
			file.dirid = ? and
			sample.sampletime =	(
				select max(sampletime) from sample where file.fileid=sample.fileid
				)
		order by sample.size DESC LIMIT ?`, dirid, n)
	fatal(err)

	return rowsToResults(rows, n)
}

func (fdb *fileDB) getOldest(dirid int64, n int) []fileEnt {
	rows, err := fdb.db.Query(
		`select path, sampletime, mode, size, mtime from file, sample 
		where file.fileid=sample.fileid and 
			file.dirid = ? and
			sample.sampletime =	(
				select max(sampletime) from sample where file.fileid=sample.fileid
				)
		order by sample.mtime ASC LIMIT ?`, dirid, n)
	fatal(err)

	return rowsToResults(rows, n)
}

func (fdb *fileDB) getNewest(dirid int64, n int) []fileEnt {
	rows, err := fdb.db.Query(
		`select path, sampletime, mode, size, mtime from file, sample 
		where file.fileid=sample.fileid and 
			file.dirid = ? and
			sample.sampletime =	(
				select max(sampletime) from sample where file.fileid=sample.fileid
				)
		order by sample.mtime DESC LIMIT ?`, dirid, n)
	fatal(err)

	return rowsToResults(rows, n)
}

func (fdb *fileDB) getFastest(dirid int64, n int) []fileEnt {
	rows, err := fdb.db.Query(
		`select path, sampletime, mode, size, mtime, rate
  				from rates, file
  				where rates.fileid = file.fileid and file.dirid = ? order by rate DESC limit ?;`, dirid, n)
	fatal(err)
	defer rows.Close()

	result := make([]fileEnt, n)
	i := 0
	for rows.Next() && i < n {
		var when, mtime int64
		rows.Scan(&result[i].path, &when, &result[i].mode, &result[i].size, &mtime, &result[i].rate)
		result[i].when = time.Unix(when, 0)
		result[i].mtime = time.Unix(mtime, 0)
		i++
	}

	return result[0:i]
}

func (fdb *fileDB) close() {
	fdb.wg.Wait()
	fdb.db.Close()
}

func fatal(err error) {
	if err != nil {
		panic(err)
	}
}

const suffixes = " kMGTP"

func niceSizef(n float64) string {
	if n == 0.0 {
		return "0"
	}
	p := int(math.Floor(math.Log10(n) / 3.0))
	if p >= len(suffixes) {
		return fmt.Sprintf("%d", n)
	}
	return fmt.Sprintf("%3.2f%c", n/math.Pow10(3*p), suffixes[p])
}

func niceSize(n int64) string {
	return niceSizef(float64(n))
}
