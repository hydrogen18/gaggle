package gaggle

import _ "github.com/lib/pq"
import "crypto/rand"
import "fmt"
import "os"
import "path"
import "bitbucket.org/liamstask/goose/lib/goose"
import "database/sql"
import "errors"
import "io/ioutil"

type Gaggle struct {
	Dbname string
	Open   string
	Driver string

	MostRecentDbVersion int64

	closer func(*Gaggle) error
}

func (this *Gaggle) Close() error {
	return this.closer(this)
}

func postgresCloser(this *Gaggle) error {
	pg, err := sql.Open(this.Driver, "dbname=postgres")
	if err != nil {
		return err
	}

	defer pg.Close()
	err = pg.Ping()
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "Dropping database %q\n", this.Dbname)
	_, err = pg.Exec(fmt.Sprintf("Drop database if exists %s", this.Dbname))

	return err

}

func Fly(driver string, dbname string, migrationsRel []string) (*Gaggle, error) {
	this := &Gaggle{}

	gopath := os.Getenv("GOPATH")
	if len(gopath) == 0 {
		this.Close()
		return nil, errors.New("GOPATH is not set")
	}
	goPathSrc := []string{gopath, "src"}
	migrationsPath := append(goPathSrc, migrationsRel...)
	migrationsDir := path.Join(migrationsPath...)

	var err error
	this.MostRecentDbVersion, err = goose.GetMostRecentDBVersion(migrationsDir)
	if err != nil {
		return nil, err
	}

	this.Dbname = dbname
	this.Driver = driver

	if len(this.Dbname) == 0 {
		entropy := make([]byte, 8)
		_, err := rand.Read(entropy)
		if err != nil {
			return nil, err
		}

		this.Dbname = fmt.Sprintf("gaggle_%x", entropy)
	}

	conf := &goose.DBConf{
		MigrationsDir: migrationsDir,
		Env:           "gaggle",
		Driver: goose.DBDriver{
			Name: this.Driver,
		},
	}

	switch this.Driver {
	case "sqlite3":
		return this, this.flySqlite3(conf)
	case "postgres":
		return this, this.flyPostgres(conf)
	default:
		return nil, fmt.Errorf("Unknown driver %q", this.Driver)
	}
}

type sqlite3Closer struct {
	tempFile string
}

func (this sqlite3Closer) Close(g *Gaggle) error {
	return os.Remove(this.tempFile)
}

func (this *Gaggle) flySqlite3(conf *goose.DBConf) error {

	var tempFileName string
	{
		tempFile, err := ioutil.TempFile("", "gaggle_sqlite3_")
		if err != nil {
			return err
		}
		tempFileName = tempFile.Name()
		fmt.Fprintf(os.Stderr, "Created tempfile %q\n", tempFileName)
		err = os.Remove(tempFile.Name())
		if err != nil {
			return err
		}
	}
	this.closer = sqlite3Closer{tempFileName}.Close
	this.Open = fmt.Sprintf("file://%s", tempFileName)
	conf.Driver.OpenStr = this.Open
	conf.Driver.Import = "github.com/mattn/go-sqlite3"
	conf.Driver.Dialect = goose.Sqlite3Dialect{}

	db, err := sql.Open(this.Driver, this.Open)
	if err != nil {
		return err
	}

	err = db.Ping()
	db.Close()

	if err != nil {
		return this.Close()
	}

	return this.migrations(conf)

}

func (this *Gaggle) migrations(conf *goose.DBConf) error {
	fmt.Fprintf(os.Stderr, "Using database %q with version %d from %q\n", this.Dbname, this.MostRecentDbVersion, conf.MigrationsDir)
	var err error
	if !conf.Driver.IsValid() {
		this.Close()
		return errors.New("invalid driver")
	}

	err = goose.RunMigrations(conf, conf.MigrationsDir, this.MostRecentDbVersion)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed running migrations:%v", err)
		this.Close()
		return err
	}

	return nil

}

func (this *Gaggle) flyPostgres(conf *goose.DBConf) error {
	this.closer = postgresCloser

	this.Open = fmt.Sprintf("sslmode=disable dbname=%s", this.Dbname)
	conf.Driver.OpenStr = this.Open

	pg, err := sql.Open(this.Driver, "dbname=postgres")
	if err != nil {
		return err
	}

	defer pg.Close()

	err = pg.Ping()
	if err != nil {
		return err
	}
	_, err = pg.Exec(fmt.Sprintf("Drop database if exists %s", this.Dbname))
	if err != nil {
		fmt.Fprint(os.Stderr, "Failed dropping database:%v", err)
		return err
	}
	_, err = pg.Exec(fmt.Sprintf("Create database %s", this.Dbname))
	if err != nil {
		fmt.Fprint(os.Stderr, "Failed creating database:%v", err)
		return err
	}

	conf.Driver.Import = "github.com/lib/pq"
	conf.Driver.Dialect = goose.PostgresDialect{}
	conf.PgSchema = ""
	return this.migrations(conf)
}
