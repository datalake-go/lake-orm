package structs

import (
	"errors"
	"strings"
	"testing"

	"github.com/go-playground/validator/v10"
)

type fromJSONBook struct {
	ID     string `json:"id"     lake:"id,pk"    validate:"required"`
	Title  string `json:"title"  lake:"title"    validate:"required"`
	Author string `json:"author" lake:"author"`
}

// The json tag is what encoding/json reads for key matching.
// lake + validate tags coexist — they carry persistence / validation
// intent independently of wire-format naming.

func TestFromJSON_DecodesValidatedModel(t *testing.T) {
	payload := []byte(`{"id":"b1","title":"Ulysses","author":"Joyce"}`)
	got, err := FromJSON[fromJSONBook](payload)
	if err != nil {
		t.Fatalf("FromJSON: %v", err)
	}
	if got.ID != "b1" || got.Title != "Ulysses" || got.Author != "Joyce" {
		t.Errorf("decoded = %+v, want {b1, Ulysses, Joyce}", got)
	}
}

func TestFromJSON_RejectsExtraTopLevelFields(t *testing.T) {
	// "publisher" is not declared on fromJSONBook — must error,
	// not silently drop.
	payload := []byte(`{"id":"b1","title":"x","publisher":"Shakespeare & Co"}`)
	_, err := FromJSON[fromJSONBook](payload)
	if err == nil {
		t.Fatal("FromJSON should reject an extra top-level JSON key")
	}
	if !strings.Contains(err.Error(), "publisher") {
		t.Errorf("error should name the offending key, got: %v", err)
	}
}

func TestFromJSON_RejectsNestedObjectForScalarField(t *testing.T) {
	// title is declared as a string; giving it a nested object
	// is a type mismatch encoding/json catches.
	payload := []byte(`{"id":"b1","title":{"en":"Ulysses","fr":"Ulysse"}}`)
	_, err := FromJSON[fromJSONBook](payload)
	if err == nil {
		t.Fatal("FromJSON should reject a nested object where the struct expects a scalar")
	}
}

func TestFromJSON_RejectsMissingRequired(t *testing.T) {
	// "title" is required per validate:"required"; omit it and
	// Validate should fail.
	payload := []byte(`{"id":"b1"}`)
	_, err := FromJSON[fromJSONBook](payload)
	if err == nil {
		t.Fatal("FromJSON should reject a payload missing a required field")
	}
	var vErr validator.ValidationErrors
	if !errors.As(err, &vErr) {
		t.Errorf("err = %T, want validator.ValidationErrors", err)
	}
}

func TestFromJSON_RejectsTrailingData(t *testing.T) {
	// Two JSON documents concatenated — catch the malformed shape.
	payload := []byte(`{"id":"b1","title":"Ulysses"}{"id":"b2","title":"Dubliners"}`)
	_, err := FromJSON[fromJSONBook](payload)
	if err == nil {
		t.Fatal("FromJSON should reject trailing content after the first JSON value")
	}
}

// fromJSONNested shows that a nested struct the model DOES declare
// is walked normally — "no nesting" applies to undeclared keys, not
// to explicit struct-typed fields.
type fromJSONPublisher struct {
	Name    string `json:"name"     validate:"required"`
	Country string `json:"country"`
}

type fromJSONBookWithPublisher struct {
	ID        string            `json:"id"        lake:"id,pk"       validate:"required"`
	Title     string            `json:"title"     lake:"title"       validate:"required"`
	Publisher fromJSONPublisher `json:"publisher" lake:"publisher,json"`
}

func TestFromJSON_AllowsDeclaredNestedStruct(t *testing.T) {
	payload := []byte(`{
		"id":"b1",
		"title":"Ulysses",
		"publisher":{"name":"Shakespeare & Co","country":"FR"}
	}`)
	got, err := FromJSON[fromJSONBookWithPublisher](payload)
	if err != nil {
		t.Fatalf("FromJSON on declared nested struct: %v", err)
	}
	if got.Publisher.Name != "Shakespeare & Co" || got.Publisher.Country != "FR" {
		t.Errorf("publisher = %+v", got.Publisher)
	}
}

func TestFromJSON_RejectsUnknownKeyInsideDeclaredNested(t *testing.T) {
	// "year" isn't on fromJSONPublisher — strictness should
	// propagate down into declared nested structs.
	payload := []byte(`{
		"id":"b1",
		"title":"Ulysses",
		"publisher":{"name":"Shakespeare & Co","year":1922}
	}`)
	_, err := FromJSON[fromJSONBookWithPublisher](payload)
	if err == nil {
		t.Fatal("FromJSON should reject unknown keys inside a declared nested struct")
	}
	if !strings.Contains(err.Error(), "year") {
		t.Errorf("error should name the offending nested key, got: %v", err)
	}
}
