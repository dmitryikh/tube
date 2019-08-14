package client

import (
	"fmt"
	"reflect"

	"github.com/dmitryikh/tube/api"
)

func checkResponseError(response interface{}) error {
	v := reflect.Indirect(reflect.ValueOf(response))
	erro, isOk := v.FieldByName("Error").Interface().(*api.Error)
	if !isOk {
		return fmt.Errorf("bad error cast")
	}
	if erro != nil {
		return api.ErrorFromProtoError(erro)
	}
	return nil
}
