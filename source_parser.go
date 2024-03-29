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
)

type sourceParserPlugin struct {
	Delimiter string
	HasHeader bool

	InferSchema bool
	Schema      dtypes.Schema
}

func (pp sourceParserPlugin) GetRecord(reader io.Reader, readerDetail objects.ReaderDetail, addRecord pkg.AddRecord) error {
	csvReader := csv.NewReader(reader)
	csvReader.Comma = []rune(pp.Delimiter)[0]
	csvReader.FieldsPerRecord = -1

	haveParsedHeader := false
	for {
		recordDetail := objects.RecordDetail{
			ReaderDetail: readerDetail,
			WriterDetail: objects.WriterDetail{},
			RecordType:   constants.RecordTypeOk,
			RecordErrors: []error{},
		}
		record, err := csvReader.Read()

		if err != nil {
			if err == io.EOF {
				recordDetail.RecordType = constants.RecordTypeEnd
				if err := addRecord(nil, recordDetail); err != nil {
					if errors.Is(err, appErrors.StopExecution{}) {
						return err
					}
				}
				break
			} else {
				recordDetail.RecordType = constants.RecordTypeFailed
				recordDetail.RecordErrors = append(recordDetail.RecordErrors, err)
				if err := addRecord(record, recordDetail); err != nil {
					if errors.Is(err, appErrors.StopExecution{}) {
						return err
					}
				}
			}
		}
		if !haveParsedHeader && pp.HasHeader {
			recordDetail.RecordType = constants.RecordTypeSchema
			haveParsedHeader = true
		}
		if err := addRecord(record, recordDetail); err != nil {
			if errors.Is(err, appErrors.StopExecution{}) {
				return err
			}
		}
	}
	return nil
}

func (pp sourceParserPlugin) GetSchema(reader io.Reader, readerDetail objects.ReaderDetail) (dtypes.Schema, error) {
	if pp.Schema != nil {
		return pp.Schema, nil
	}
	if pp.InferSchema {
		var header []string
		pp.GetRecord(reader, readerDetail,
			func(record any, recordDetail objects.RecordDetail) error {
				if recordDetail.RecordType == constants.RecordTypeSchema {
					header = record.([]string)
				} else {
					for i := range record.([]string) {
						header = append(header, fmt.Sprintf("col_%d", i))
					}
				}
				return appErrors.StopExecution{}
			})
		for _, colName := range header {
			pp.Schema = append(pp.Schema, dtypes.Column{Name: colName, Type: dtypes.StringType{}})
		}
		return pp.Schema, nil
	}
	return nil, fmt.Errorf("schema cannot be inferred for plugin %s", pp.GetPluginDetail().PluginName)
}

func (pp sourceParserPlugin) GetPluginDetail() objects.PluginDetail {
	return objects.PluginDetail{
		PluginName: "csv_source_parser",
		PluginType: constants.PluginTypeSourceParser,
	}
}

// exported
var SourceParserPlugin = sourceParserPlugin{
	Delimiter:   ",",
	HasHeader:   false,
	InferSchema: false,
	Schema:      nil,
}
