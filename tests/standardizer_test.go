package matcher_test

import (
	"testing"

	"github.com/TFMV/AddressMatchPro/internal/matcher"
)

func TestStandardizeAddress(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Basic street address",
			input:    "123 Main Street",
			expected: "123 main st",
		},
		{
			name:     "Address with directional",
			input:    "456 North Elm Avenue",
			expected: "456 n elm ave",
		},
		{
			name:     "Address with unit number",
			input:    "789 Oak Drive Apt #301",
			expected: "789 oak dr apt 301",
		},
		{
			name:     "Address with multiple spaces",
			input:    "1010   Maple    Lane",
			expected: "1010 maple ln",
		},
		{
			name:     "Address with mixed case",
			input:    "2020 SuNsEt BoUlEvArD",
			expected: "2020 sunset blvd",
		},
		{
			name:     "Address with PO Box",
			input:    "PO Box 12345",
			expected: "po box 12345",
		},
		{
			name:     "Address with suite",
			input:    "3030 Business Center Drive, Suite 200",
			expected: "3030 business ctr dr ste 200",
		},
		{
			name:     "Complex address",
			input:    "4040 Southwest Highland TERRACE, Unit #B-12, Floor 3",
			expected: "4040 sw highland ter unit b-12 fl 3",
		},
		{
			name:     "Address with all lowercase",
			input:    "5050 eastern parkway circle",
			expected: "5050 eastern pkwy cir",
		},
		{
			name:     "Address with all uppercase",
			input:    "6060 WESTERN HEIGHTS COURT NORTHWEST",
			expected: "6060 western heights ct nw",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := matcher.StandardizeAddress(tt.input)
			if err != nil {
				t.Errorf("StandardizeAddress() error = %v", err)
				return
			}
			if result != tt.expected {
				t.Errorf("StandardizeAddress() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestIsNumeric(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{"Only digits", "12345", true},
		{"Digits and letters", "123abc", false},
		{"Empty string", "", true},
		{"Special characters", "123-456", false},
		{"Decimal number", "123.45", false},
		{"Large number", "9876543210", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := matcher.IsNumeric(tt.input)
			if result != tt.expected {
				t.Errorf("IsNumeric(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}
