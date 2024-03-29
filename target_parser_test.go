package main

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tardigodev/tardigo-core/pkg"
	"github.com/tardigodev/tardigo-core/pkg/constants"
	"github.com/tardigodev/tardigo-core/pkg/dtypes"
	"github.com/tardigodev/tardigo-core/pkg/objects"
)

func TestTargetParserPlugin(t *testing.T) {
	var _ pkg.TargetParserPlugin = TargetParserPlugin

	assert.Equal(t, 1, reflect.TypeOf(TargetParserPlugin).NumField())
	assert.Equal(t, ",", TargetParserPlugin.Delimiter)
}

func TestTargetParserPlugin_ConvertSchema(t *testing.T) {
	schema := dtypes.Schema{
		{Name: "a", Type: dtypes.IntegerType{}},
		{Name: "b", Type: dtypes.FloatType{}},
		{Name: "c", Type: dtypes.StringType{}},
	}

	schema, err := targetParserPlugin{}.ConvertSchema(schema)
	assert.NoError(t, err)

	assert.Equal(t, 3, len(schema))

	assert.Equal(t, "a", schema[0].Name)
	assert.Equal(t, "b", schema[1].Name)
	assert.Equal(t, "c", schema[2].Name)

	assert.IsType(t, dtypes.StringType{}, schema[0].Type)
	assert.IsType(t, dtypes.StringType{}, schema[1].Type)
	assert.IsType(t, dtypes.StringType{}, schema[2].Type)
}

func TestTargetParserPlugin_GetPluginDetail(t *testing.T) {
	detail := targetParserPlugin{}.GetPluginDetail()
	assert.Equal(t, "csv_target_parser", detail.PluginName)
	assert.Equal(t, constants.PluginTypeTargetParser, detail.PluginType)
}

func TestTargetParserPlugin_PutRecord(t *testing.T) {
	var buff bytes.Buffer
	tpp := targetParserPlugin{
		Delimiter: ",",
	}
	schema := dtypes.Schema{
		{Name: "a", Type: dtypes.IntegerType{}},
		{Name: "b", Type: dtypes.FloatType{}},
		{Name: "c", Type: dtypes.StringType{}},
	}
	recordErrors := make([]objects.RecordDetail, 0)
	recordError := func(record any, recordDetail objects.RecordDetail) error {
		recordErrors = append(recordErrors, recordDetail)
		return nil
	}

	tpp.PutRecord(&buff, objects.WriterDetail{}, []any{1, 2.0, "alice"}, objects.RecordDetail{RecordType: constants.RecordTypeOk}, schema, recordError)
	assert.Equal(t, "1,2,alice", strings.Trim(buff.String(), "\n"))
	assert.Equal(t, 0, len(recordErrors))

	// test for invalid record
	buff.Reset()
	tpp.PutRecord(&buff, objects.WriterDetail{}, []any{[]any{}, 2.0, "alice"}, objects.RecordDetail{RecordType: constants.RecordTypeFailed}, schema, recordError)

	assert.Equal(t, "", buff.String())
	assert.Equal(t, 1, len(recordErrors))
	assert.Equal(t, constants.RecordTypeFailed, recordErrors[0].RecordType)
	assert.ErrorContains(t, recordErrors[0].RecordErrors[0], "failed to convert column")
}
