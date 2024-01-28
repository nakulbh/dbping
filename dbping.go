package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres" // embedded postgres server.

	_ "github.com/jackc/pgx/v5"
)

func main() {
	timeout := flag.Duration("timeout", 5*time.Second, "timeout for connection to postgres")
	flag.Parse()
	cfg, err := pgConfigFromEnv()
	if err != nil {
		log.Fatalf("postgres configration error: %v ", err)
	}

	// setup of embeddedpostgres server
	portN, err := strconv.Atoi(cfg.port)
	if err != nil {
		panic(err)
	}

	embeddedCfg := embeddedpostgres.DefaultConfig().
		Username(cfg.user).
		Password(cfg.password).
		DataPath(cfg.database).
		Port(uint32(portN)).
		Logger(io.Discard)

	embeddedDB := embeddedpostgres.NewDatabase(embeddedCfg)

	if err := embeddedDB.Start(); err != nil {
		panic(err)
	}
	log.Printf("postgres is running  on: %s \n", embeddedCfg.GetConnectionURL())
	defer embeddedDB.Stop()

	db, err := sql.Open("postgres", cfg.String())
	if err != nil {
		panic(err)
	}

	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		panic(err)
	}
	log.Println("ping successful")
}

type pgconfig struct {
	user     string
	database string
	host     string
	password string
	port     string
	sslMode  string
}

func (pg pgconfig) String() string {
	s := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", pg.user, pg.password, pg.host, pg.port, pg.database)
	if pg.sslMode != "" {
		s += "?sslmode=" + pg.sslMode
	}
	return s
}

func pgConfigFromEnv() (pgconfig, error) {
	var missing []string

	get := func(key string) string {
		val := os.Getenv(key)
		if val == "" {
			missing = append(missing, key)
		}
		return val
	}

	cfg := pgconfig{
		user:     get("PG_USER"),
		database: get("PG_DATABASE"),
		host:     get("PG_HOST"),
		password: get("PG_PASSWORD"),
		port:     get("PG_PORT"),
		sslMode:  os.Getenv("PG_SSLMODE"),
	}

	switch cfg.sslMode {
	case "", "disable", "allow", "require", "verify-ca", "verify-full":

	default:
		return cfg, fmt.Errorf(`invalid ssl mode "%s": expected one of "", "disable", "allow", "require", "verify-ca", or "verify-full" `, cfg.sslMode)

	}

	if len(missing) > 0 {
		sort.Strings(missing)
		return cfg, fmt.Errorf("missing required environnment variable: %v", missing)
	}

	return cfg, nil

}
