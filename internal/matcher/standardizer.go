package matcher

import (
	"strings"
	"unicode"
)

var (
	// Define common abbreviations
	abbreviations = map[string]string{
		"avenue":    "ave",
		"boulevard": "blvd",
		"parkway":   "pkwy",
		"circle":    "cir",
		"court":     "ct",
		"center":    "ctr",
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
		"suite":     "ste",
		"apartment": "apt",
		"floor":     "fl",
		"north":     "n",
		"south":     "s",
		"east":      "e",
		"west":      "w",
	}
)

// StandardizeAddress takes a raw address string and returns a standardized address string.
func StandardizeAddress(street string) (string, error) {
	// Normalize the street address by converting to lower case and trimming spaces
	street = strings.ToLower(strings.TrimSpace(street))

	// Remove any commas, periods, or other punctuation from the street address
	street = strings.Map(func(r rune) rune {
		if unicode.IsPunct(r) || unicode.IsSymbol(r) {
			return -1
		}
		return r
	}, street)

	// Remove extra spaces within the street address
	street = strings.Join(strings.Fields(street), " ")

	// Split the address into words
	words := strings.Fields(street)

	// Process each word in the address
	for i := 0; i < len(words); i++ {
		// Remove the '#' prefix from unit numbers
		if i > 0 && (words[i-1] == "unit" || words[i-1] == "ste" || words[i-1] == "apt" || words[i-1] == "fl") {
			words[i] = strings.TrimPrefix(words[i], "#")
		}

		// Apply abbreviations
		if abbr, ok := abbreviations[words[i]]; ok {
			words[i] = abbr
		}
	}

	return strings.Join(words, " "), nil
}

// IsNumeric checks if a string contains only numeric characters
func IsNumeric(s string) bool {
	for _, r := range s {
		if !unicode.IsDigit(r) {
			return false
		}
	}
	return true
}
