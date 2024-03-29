package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"

	"github.com/tardigodev/tardigo-core/pkg"
	"github.com/tardigodev/tardigo-core/pkg/constants"
	"github.com/tardigodev/tardigo-core/pkg/dtypes"
	appErrors "github.com/tardigodev/tardigo-core/pkg/errors"
	"github.com/tardigodev/tardigo-core/pkg/objects"
	"github.com/tardigodev/tardigo-core/pkg/utils"
	"github.com/tardigodev/tardigo-csv/internal"
)

type targetParserPlugin struct {
	Delimiter string
}

func (tp targetParserPlugin) PutRecord(writer io.Writer, writerDetail objects.WriterDetail, record any, recordDetail objects.RecordDetail, schema dtypes.Schema, addErrorRecord pkg.AddRecord) error {
	csvWriter := csv.NewWriter(writer)
	csvWriter.Comma = []rune(tp.Delimiter)[0]
	defer csvWriter.Flush()

	schema, _ = tp.ConvertSchema(schema)

	record, err := utils.ConvertToRecord(record, schema)
	if err != nil {
		recordDetail.RecordType = constants.RecordTypeFailed
		recordDetail.RecordErrors = append(recordDetail.RecordErrors, err)
	} else {
		recordStr, err := internal.ConvertRecordToStrSlice(record)

		if err != nil {
			recordDetail.RecordType = constants.RecordTypeFailed
			recordDetail.RecordErrors = append(recordDetail.RecordErrors, err)
		} else {
			err = csvWriter.Write(recordStr)
			if err != nil {
				recordDetail.RecordType = constants.RecordTypeFailed
				recordDetail.RecordErrors = append(recordDetail.RecordErrors, fmt.Errorf("failed to write to buffer %w", err))
			}
		}
	}
	if len(recordDetail.RecordErrors) > 0 {
		if err := addErrorRecord(record, recordDetail); err != nil {
			if errors.Is(err, appErrors.StopExecution{}) {
				return err
			}
		}
	}
	return nil
}

func (tp targetParserPlugin) ConvertSchema(schema dtypes.Schema) (dtypes.Schema, error) {
	for i := 0; i < len(schema); i++ {
		schema[i].Type = dtypes.StringType{}
	}
	return schema, nil
}

func (tp targetParserPlugin) GetPluginDetail() objects.PluginDetail {
	return objects.PluginDetail{
		PluginName: "csv_target_parser",
		PluginType: constants.PluginTypeTargetParser,
	}
}

// exported
var TargetParserPlugin = targetParserPlugin{
	Delimiter: ",",
}
