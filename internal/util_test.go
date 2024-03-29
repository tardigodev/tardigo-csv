package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConvertRecordToStrSlice(t *testing.T) {
	// with mixed records
	convRecords, err := ConvertRecordToStrSlice([]any{1, 2.0, "test"})
	assert.NoError(t, err)
	assert.Equal(t, []string{"1", "2", "test"}, convRecords)

	// with invalid any records
	_, err = ConvertRecordToStrSlice([]int{1, 2, 3})
	assert.ErrorContains(t, err, "record type []int not supported")

	_, err = ConvertRecordToStrSlice([]any{[]int{1}, 2, 3})
	assert.ErrorContains(t, err, "failed to convert '[]int' to string")
}
