package radius

type ZipCode struct {
	ZipCode   string  `json:"zipCode"`
	Longitude float64 `json:"longitude"`
	Latitude  float64 `json:"latitude"`
	Distance  float64 `json:"distance"`
}
