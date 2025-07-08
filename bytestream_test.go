package bazelazblob

import "testing"

func TestParseByteStreamResourceName(t *testing.T) {
	tests := []struct {
		name         string
		resourceName string
		expectedHash string
		expectedSize int64
		expectedErr  bool
	}{
		{
			name:         "valid blob resource",
			resourceName: "blobs/abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890/1024",
			expectedHash: "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
			expectedSize: 1024,
			expectedErr:  false,
		},
		{
			name:         "valid upload resource",
			resourceName: "uploads/uuid/blobs/abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890/1024",
			expectedHash: "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
			expectedSize: 1024,
			expectedErr:  false,
		},
		{
			name:         "invalid resource format",
			resourceName: "invalid/resource/name",
			expectedErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			hash, size, err := parseByteStreamResourceName(tc.resourceName)

			if tc.expectedErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if hash != tc.expectedHash {
				t.Errorf("expected hash %s, got %s", tc.expectedHash, hash)
			}

			if size != tc.expectedSize {
				t.Errorf("expected size %d, got %d", tc.expectedSize, size)
			}
		})
	}
}
