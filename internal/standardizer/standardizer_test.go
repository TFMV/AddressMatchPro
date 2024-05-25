package standardizer

import (
	"testing"
)

func TestStandardizeAddress(t *testing.T) {
	tests := []struct {
		name         string
		organization string
		street       string
		locality     string
		state        string
		postalCode   string
		expected     string
	}{
		{
			name:         "John Doe",
			organization: "Company Inc.",
			street:       "123 Main Street",
			locality:     "Springfield",
			state:        "IL",
			postalCode:   "62704",
			expected:     "John Doe Company Inc. 123 Main Street Springfield, Illinois 62704 United States",
		},
	}

	for _, test := range tests {
		result, err := StandardizeAddress(test.name, test.organization, test.street, test.locality, test.state, test.postalCode)
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}
		if result != test.expected {
			t.Errorf("Expected '%s', but got '%s'", test.expected, result)
		}
	}
}
