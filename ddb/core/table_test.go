package core

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateTableName(t *testing.T) {
	tests := []struct {
		name        string
		tableName   string
		expectedErr error
	}{
		{
			name:        "Valid table name",
			tableName:   "ValidTableName",
			expectedErr: nil,
		},
		{
			name:        "Empty table name",
			tableName:   "",
			expectedErr: errors.New("length must be between 3 and 255 characters"),
		},
		{
			name:        "Table name too short (1 char)",
			tableName:   "A",
			expectedErr: errors.New("length must be between 3 and 255 characters"),
		},
		{
			name:        "Table name too short (2 chars)",
			tableName:   "AB",
			expectedErr: errors.New("length must be between 3 and 255 characters"),
		},
		{
			name:        "Table name too long (256 chars)",
			tableName:   strings.Repeat("a", 256),
			expectedErr: errors.New("length must be between 3 and 255 characters"),
		},
		{
			name:        "Table name with invalid character (space)",
			tableName:   "Invalid Table",
			expectedErr: errors.New("contains invalid characters"),
		},
		{
			name:        "Table name with invalid character (@)",
			tableName:   "Invalid@Table",
			expectedErr: errors.New("contains invalid characters"),
		},
		{
			name:        "Table name with underscore",
			tableName:   "Valid_Table_Name",
			expectedErr: nil,
		},
		{
			name:        "Table name with dash",
			tableName:   "Valid-Table-Name",
			expectedErr: nil,
		},
		{
			name:        "Table name with dot",
			tableName:   "Valid.Table.Name",
			expectedErr: nil,
		},
		{
			name:        "Table name with numbers",
			tableName:   "Table123",
			expectedErr: nil,
		},
		{
			name:        "Table name with only numbers",
			tableName:   "123456",
			expectedErr: nil,
		},
		{
			name:        "Table name at min length (3 chars)",
			tableName:   "abc",
			expectedErr: nil,
		},
		{
			name:        "Table name at max length (255 chars)",
			tableName:   strings.Repeat("a", 255),
			expectedErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateTableName(tt.tableName)
			if err != nil && tt.expectedErr == nil {
				t.Fatalf("ValidateTableName(%v) returned unexpected error: %v", tt.tableName, err)
			} else if err == nil && tt.expectedErr != nil {
				t.Fatalf("ValidateTableName(%v) expected error: %v, got nil", tt.tableName, tt.expectedErr)
			} else if err != nil && tt.expectedErr != nil && err.Error() != tt.expectedErr.Error() {
				t.Fatalf("ValidateTableName(%v) expected error: %v, got: %v", tt.tableName, tt.expectedErr, err)

			}
		})
	}
}
