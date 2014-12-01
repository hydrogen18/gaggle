package gaggle

import _ "github.com/lib/pq"
import "crypto/rand"
import "fmt"
import "os"
import "path"
import "bitbucket.org/liamstask/goose/lib/goose"
import "database/sql"
import "errors"

type Gaggle struct {
	Dbname string
	Open   string
	Driver string
}

func (this *Gaggle) Close() error {
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

	mostRecentDbVersion, err := goose.GetMostRecentDBVersion(migrationsDir)
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
	this.Open = fmt.Sprintf("sslmode=disable dbname=%s", this.Dbname)

	pg, err := sql.Open(this.Driver, "dbname=postgres")
	if err != nil {
		return nil, err
	}

	defer pg.Close()

	err = pg.Ping()
	if err != nil {
		return nil, err
	}
	_, err = pg.Exec(fmt.Sprintf("Drop database if exists %s", this.Dbname))
	if err != nil {
		fmt.Fprint(os.Stderr, "Failed dropping database:%v", err)
		return nil, err
	}
	_, err = pg.Exec(fmt.Sprintf("Create database %s", this.Dbname))
	if err != nil {
		fmt.Fprint(os.Stderr, "Failed creating database:%v", err)
		return nil, err
	}

	conf := &goose.DBConf{
		MigrationsDir: migrationsDir,
		Env:           "gaggle",
		Driver: goose.DBDriver{
			Name:    this.Driver,
			OpenStr: this.Open,
			Import:  "github.com/lib/pq",
			Dialect: goose.PostgresDialect{},
		},
		PgSchema: "",
	}
	if !conf.Driver.IsValid() {
		this.Close()
		return nil, errors.New("invalid driver")
	}

	err = goose.RunMigrations(conf, migrationsDir, mostRecentDbVersion)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed running migrations:%v", err)
		this.Close()
		return nil, err
	}
	fmt.Fprintf(os.Stderr, "Using database %q with version %d from %q", this.Dbname, mostRecentDbVersion, migrationsDir)
	return this, nil
}
