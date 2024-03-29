package main

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tardigodev/tardigo-core/pkg"
	"github.com/tardigodev/tardigo-core/pkg/constants"
	"github.com/tardigodev/tardigo-core/pkg/dtypes"
	"github.com/tardigodev/tardigo-core/pkg/objects"
)

func TestSourceParserPlugin(t *testing.T) {
	var _ pkg.SourceParserPlugin = SourceParserPlugin

	assert.Equal(t, 4, reflect.TypeOf(SourceParserPlugin).NumField())
	assert.Equal(t, ",", SourceParserPlugin.Delimiter)
	assert.Equal(t, false, SourceParserPlugin.HasHeader)
	assert.Equal(t, false, SourceParserPlugin.InferSchema)
	assert.Nil(t, SourceParserPlugin.Schema)
}

func TestSourceParserPlugin_GetSchema(t *testing.T) {
	// test for data with header
	reader := strings.NewReader("a,b,c\n1,2,3")
	spp := sourceParserPlugin{
		Delimiter:   ",",
		HasHeader:   true,
		InferSchema: true,
	}
	schema, err := spp.GetSchema(reader, objects.ReaderDetail{ReaderSource: "memory"})

	assert.NoError(t, err)
	assert.Equal(t, 3, len(schema))
	assert.Equal(t, "a", schema[0].Name)
	assert.Equal(t, "b", schema[1].Name)
	assert.Equal(t, "c", schema[2].Name)

	assert.IsType(t, dtypes.StringType{}, schema[0].Type)
	assert.IsType(t, dtypes.StringType{}, schema[1].Type)
	assert.IsType(t, dtypes.StringType{}, schema[2].Type)

	// test for data without header
	reader = strings.NewReader("1,2,3")
	spp = sourceParserPlugin{
		Delimiter:   ",",
		HasHeader:   false,
		InferSchema: true,
	}
	schema, err = spp.GetSchema(reader, objects.ReaderDetail{ReaderSource: "memory"})

	assert.NoError(t, err)
	assert.Equal(t, 3, len(schema))
	assert.Equal(t, "col_0", schema[0].Name)
	assert.Equal(t, "col_1", schema[1].Name)
	assert.Equal(t, "col_2", schema[2].Name)

	assert.IsType(t, dtypes.StringType{}, schema[0].Type)
	assert.IsType(t, dtypes.StringType{}, schema[1].Type)
	assert.IsType(t, dtypes.StringType{}, schema[2].Type)

	// test for invalid schema
	reader = strings.NewReader("a,b,c\n1,2,3")
	spp = sourceParserPlugin{
		Delimiter:   ",",
		HasHeader:   false,
		InferSchema: false,
	}
	_, err = spp.GetSchema(reader, objects.ReaderDetail{ReaderSource: "memory"})

	assert.ErrorContains(t, err, "schema cannot be inferred")
}

func TestSourceParserPlugin_GetRecord(t *testing.T) {
	// test for data with header
	reader := strings.NewReader("a,b,c\n1,2,3")
	spp := sourceParserPlugin{
		Delimiter:   ",",
		HasHeader:   true,
		InferSchema: true,
	}
	records := []any{}
	recordDetails := []objects.RecordDetail{}

	addRecord := func(record any, recordDetail objects.RecordDetail) error {
		records = append(records, record)
		recordDetails = append(recordDetails, recordDetail)
		return nil
	}
	spp.GetRecord(reader, objects.ReaderDetail{ReaderSource: "memory"}, addRecord)

	assert.Equal(t, 3, len(records))
	assert.Equal(t, 3, len(recordDetails))
	assert.Equal(t, constants.RecordTypeSchema, recordDetails[0].RecordType)
	assert.Equal(t, constants.RecordTypeOk, recordDetails[1].RecordType)
	assert.Equal(t, constants.RecordTypeEnd, recordDetails[2].RecordType)

	// test for data without header
	reader = strings.NewReader("1,2,3")
	spp = sourceParserPlugin{
		Delimiter:   ",",
		HasHeader:   false,
		InferSchema: true,
	}
	records = []any{}
	recordDetails = []objects.RecordDetail{}

	addRecord = func(record any, recordDetail objects.RecordDetail) error {
		records = append(records, record)
		recordDetails = append(recordDetails, recordDetail)
		return nil
	}
	spp.GetRecord(reader, objects.ReaderDetail{ReaderSource: "memory"}, addRecord)

	assert.Equal(t, 2, len(records))
	assert.Equal(t, 2, len(recordDetails))
	assert.Equal(t, constants.RecordTypeOk, recordDetails[0].RecordType)
	assert.Equal(t, constants.RecordTypeEnd, recordDetails[1].RecordType)
}

func TestSourceParserPlugin_GetPluginDetail(t *testing.T) {
	spp := sourceParserPlugin{}
	detail := spp.GetPluginDetail()

	assert.Equal(t, "csv_source_parser", detail.PluginName)
	assert.Equal(t, constants.PluginTypeSourceParser, detail.PluginType)
}
