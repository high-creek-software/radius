package radius

type Repository interface {
	Store(zipCode, point string) error
	FindZipCode(zipCode string) (ZipCode, error)
	FindAdjacent(longitude, latitude, meters float64) ([]ZipCode, error)
}
