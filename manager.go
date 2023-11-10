package radius

import (
	"encoding/csv"
	"errors"
	"io"
	"log/slog"
)

type Manager interface {
	ImportZCTAs(r io.Reader) error
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
