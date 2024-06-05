package radius

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"log/slog"
)

const (
	createZipTablePgx = `CREATE TABLE IF NOT EXISTS zipcodes(
    	zip varchar(16) primary key,
    	location geometry(Point, 4326)
	);`

	createLocationIndex = `CREATE INDEX IF NOT EXISTS idx_zipcodes_location ON zipcodes USING gist(location);`
)

type RepositoryPgx struct {
	conn *pgxpool.Pool
}

func NewRepositoryPgx(conn *pgxpool.Pool) (Repository, error) {
	_, err := conn.Exec(context.Background(), createZipTablePgx)
	if err != nil {
		return nil, err
	}

	_, err = conn.Exec(context.Background(), createLocationIndex)
	if err != nil {
		return nil, err
	}

	//_, err = conn.Exec(context.Background(), "CREATE EXTENSION postgis;")
	//if err != nil {
	//	slog.Error("error creating extension postgis", "err", err)
	//	os.Exit(2)
	//}

	return &RepositoryPgx{conn: conn}, nil
}

func (r *RepositoryPgx) Store(zipCode, point string) error {
	_, err := r.conn.Exec(context.Background(), "INSERT INTO zipcodes(zip, location) VALUES($1, $2);", zipCode, point)
	return err
}

func (r *RepositoryPgx) FindZipCode(zipCode string) (ZipCode, error) {
	row := r.conn.QueryRow(context.Background(), "SELECT zip, ST_X(location), ST_Y(location) FROM zipcodes WHERE zip = $1;", zipCode)
	var z ZipCode
	err := row.Scan(&z.ZipCode, &z.Longitude, &z.Latitude)
	return z, err
}

func (r *RepositoryPgx) FindAdjacent(longitude, latitude, meters float64) ([]ZipCode, error) {
	// 1 Mile = 1609.34 Meters
	/*
		SRID: tells postgis what measurement system we're using
		location::geography ensures we're using the dwithin query on a geography
		st_setsrid(st_point())::geography again using a geography
	*/
	rows, err := r.conn.Query(context.Background(), "SELECT zip, ST_X(location), ST_Y(location), ST_Distance(location::geography, ST_SetSRID(ST_Point($1, $2), 4326)::geography) AS distance FROM zipcodes WHERE ST_DWithin(location::geography, ST_SetSRID(ST_Point($1, $2), 4326)::geography, $3) ORDER BY distance", longitude, latitude, meters)
	if err != nil {
		return nil, err
	}

	var results []ZipCode
	for rows.Next() {
		var z ZipCode
		if err = rows.Scan(&z.ZipCode, &z.Longitude, &z.Latitude, &z.Distance); err == nil {
			results = append(results, z)
		} else {
			slog.Error("error scanning zipcode for adjacent query", "err", err)
		}
	}

	return results, nil
}
