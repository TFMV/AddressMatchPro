package standardizer

import (
	"strings"

	"github.com/Boostport/address"
)

// StandardizeAddress takes raw address components and returns a standardized address string.
func StandardizeAddress(name, organization, street, locality, state, postalCode string) (string, error) {
	addr, err := address.NewValid(
		address.WithCountry("US"),
		address.WithName(name),
		address.WithOrganization(organization),
		address.WithStreetAddress([]string{street}),
		address.WithLocality(locality),
		address.WithAdministrativeArea(state),
		address.WithPostCode(postalCode),
	)

	if err != nil {
		return "", err
	}

	formatter := address.DefaultFormatter{
		Output: address.StringOutputter{},
	}

	lang := "en" // Use the English names of the administrative areas, localities and dependent localities where possible
	standardizedAddress := formatter.Format(addr, lang)

	// Remove any unwanted characters and trim spaces
	standardizedAddress = strings.ReplaceAll(standardizedAddress, "\n", " ")
	standardizedAddress = strings.TrimSpace(standardizedAddress)

	return standardizedAddress, nil
}
