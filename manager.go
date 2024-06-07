package radius

import (
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/Valentin-Kaiser/go-dbase/dbase"
	"io"
	"log/slog"
)

// Headers as per the file obtained here: https://www.census.gov/cgi-bin/geo/shapefiles/index.php
const (
	headerZCTA = "ZCTA5CE20"
	headerLat  = "INTPTLAT20"
	headerLon  = "INTPTLON20"
)

type Manager interface {
	ImportZCTAs(r io.Reader) error
	ImportZCTAsFromDBF(fileName string) error
}

type ManagerImpl struct {
	repository Repository
}

func NewManagerImpl(repository Repository) Manager {
	m := &ManagerImpl{
		repository: repository,
	}

	return m
}

func (m *ManagerImpl) ImportZCTAs(r io.Reader) error {
	cr := csv.NewReader(r)
	// One read to read the header
	cr.Read()

	for {
		fields, err := cr.Read()
		if err != nil && errors.Is(io.EOF, err) {
			break
		} else if err != nil {
			slog.Error("error reading line", "err", err)
			continue
		}

		zip := fields[0]
		point := fields[len(fields)-1]

		slog.Info("zip code at", "zip", zip, "point", point)

		//point = "SRID=4326;" + point
		err = m.repository.Store(zip, point)
		if err != nil {
			slog.Error("error inserting point", "err", err)
		}
	}

	return nil
}

func (m *ManagerImpl) ImportZCTAsFromDBF(fileName string) error {

	/*table, err := godbf.NewFromFile(fileName, "UTF8")
	if err != nil {
		return fmt.Errorf("error opening file: %w", err)
	}

	for i := 0; i < table.NumberOfRecords(); i++ {
		zip, zipErr := table.FieldValueByName(i, headerZCTA)
		lat, latErr := table.FieldValueByName(i, headerLat)
		lon, lonErr := table.FieldValueByName(i, headerLon)

		if zipErr == nil && latErr == nil && lonErr == nil {
			// Formatting the point as WKT (well known text), this matches what is available via the above csv
			point := fmt.Sprintf("POINT(%s %s)", lon, lat)
			slog.Info("Record", "zip", zip, "POINT", point)
			if err := m.repository.Store(zip, point); err != nil {
				slog.Error("error importing zip", "error", err)
			}
		} else {
			slog.Error("Error reading record", "row", i, "zipErr", zipErr, "latErr", latErr, "lonErr", lonErr)
		}
	}*/

	table, err := dbase.OpenTable(&dbase.Config{
		Filename: fileName,
		Untested: true,
	})
	if err != nil {
		return fmt.Errorf("error opening file: %w", err)
	}

	for !table.EOF() {
		row, err := table.Next()
		if err != nil {
			return err
		}

		zip := row.FieldByName(headerZCTA).GetValue().(string)
		lat := row.FieldByName(headerLat).GetValue().(string)
		lon := row.FieldByName(headerLon).GetValue().(string)

		// Formatting the point as WKT (well known text), this matches what is available via the above csv
		point := fmt.Sprintf("POINT(%s %s)", lon, lat)
		slog.Info("Record", "zip", zip, "POINT", point)
		if err := m.repository.Store(zip, point); err != nil {
			slog.Error("error importing zip", "error", err)
		}

	}

	return nil
}
