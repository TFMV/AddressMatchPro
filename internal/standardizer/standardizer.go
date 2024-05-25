package standardizer

import (
	"regexp"
	"strings"
)

// StandardizeAddress takes raw address components and returns a standardized address string.
func StandardizeAddress(street string) (string, error) {
	// Define common abbreviations
	abbreviations := map[string]string{
		"avenue":    "ave",
		"boulevard": "blvd",
		"circle":    "cir",
		"court":     "ct",
		"drive":     "dr",
		"highway":   "hwy",
		"lane":      "ln",
		"place":     "pl",
		"road":      "rd",
		"street":    "st",
		"terrace":   "ter",
		"northwest": "nw",
		"southeast": "se",
		"southwest": "sw",
		"northeast": "ne",
		"unit":      "unit",
		"ste":       "ste",
		"apt":       "apt",
		"floor":     "fl",
		"po box":    "pobox", // Keep consistent with reference entities
	}

	// Normalize the street address by converting to lower case and trimming spaces
	street = strings.ToLower(street)
	street = strings.TrimSpace(street)

	// Remove extra spaces within the street address
	space := regexp.MustCompile(`\s+`)
	street = space.ReplaceAllString(street, " ")

	// Apply abbreviations
	for longForm, shortForm := range abbreviations {
		street = strings.ReplaceAll(street, longForm, shortForm)
	}

	return street, nil
}
