package internal

import (
	"fmt"

	"github.com/tardigodev/tardigo-core/pkg/dtypes"
)

func ConvertRecordToStrSlice(record any) ([]string, error) {
	recordAny, ok := record.([]any)
	if !ok {
		return nil, fmt.Errorf("record type %T not supported", record)
	}
	recordStr := make([]string, len(recordAny))
	for i := 0; i < len(recordAny); i++ {
		recStr, err := dtypes.StringType{}.Convert(recordAny[i])
		if err != nil {
			return nil, err
		}
		recordStr[i] = recStr.(string)
	}
	return recordStr, nil
}
