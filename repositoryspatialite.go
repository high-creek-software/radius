package radius

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
)

const (
	createZipTableSpatialite = `CREATE TABLE IF NOT EXISTS zipcodes(
    	zip varchar(16) primary key
	);`

	addGeoColumn = `SELECT AddGeometryColumn('zipcodes', 'location', 4326, 'POINT', 'XY');`

	createLocationIndexSpatialite = `SELECT CreateSpatialIndex('zipcodes', 'location');`
)

type RepositorySpatialite struct {
	db *sql.DB
}

func NewRepositorySpatialite(db *sql.DB) (Repository, error) {

	_, err := db.ExecContext(context.TODO(), "SELECT InitSpatialMetaData();")
	if err != nil {
		return nil, err
	}

	_, err = db.ExecContext(context.TODO(), createZipTableSpatialite)
	if err != nil {
		return nil, err
	}

	_, err = db.ExecContext(context.TODO(), addGeoColumn)
	if err != nil {
		return nil, err
	}

	_, err = db.ExecContext(context.TODO(), createLocationIndexSpatialite)
	if err != nil {
		return nil, err
	}

	return &RepositorySpatialite{db: db}, nil
}

func (r *RepositorySpatialite) Store(zipCode, point string) error {
	_, err := r.db.ExecContext(context.Background(), "INSERT INTO zipcodes(zip, location) VALUES(?, GeomFromText(?, 4326));", zipCode, point)
	return err
}

func (r *RepositorySpatialite) FindZipCode(zipCode string) (ZipCode, error) {

	row := r.db.QueryRowContext(context.Background(), "SELECT zip, X(location), Y(location) FROM zipcodes WHERE zip = ?;", zipCode)
	var z ZipCode
	err := row.Scan(&z.ZipCode, &z.Longitude, &z.Latitude)
	return z, err
}

func (r *RepositorySpatialite) FindAdjacent(longitude, latitude, meters float64) ([]ZipCode, error) {
	// 1 Mile = 1609.34 Meters
	/*
		SRID: tells postgis what measurement system we're using
		location::geography ensures we're using the dwithin query on a geography
		st_setsrid(st_point())::geography again using a geography
	*/
	point := fmt.Sprintf("POINT(%f %f)", longitude, latitude)
	rows, err := r.db.QueryContext(context.Background(), "SELECT zip, X(location), Y(location), Distance(location, GeomFromText(?, 4326), true) AS distance FROM zipcodes WHERE PTDistWithin(location, GeomFromText(?, 4326), ?) ORDER BY distance ASC", point, point, meters)
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
