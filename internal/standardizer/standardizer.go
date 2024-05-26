// --------------------------------------------------------------------------------
// Author: Thomas F McGeehan V
//
// This file is part of a software project developed by Thomas F McGeehan V.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//
// For more information about the MIT License, please visit:
// https://opensource.org/licenses/MIT
//
// Acknowledgment appreciated but not required.
// --------------------------------------------------------------------------------

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
