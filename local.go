package lakeorm

import "fmt"

// Local is an intentional no-op in the root package — it exists only
// so godoc surfaces the "how do I run this locally?" question here.
// The real implementation lives in the `local` subpackage:
//
//	import lakeormlocal "github.com/datalake-go/lake-orm/local"
//	db, err := lakeormlocal.Open()
//
// The split exists because the real Local() must import the spark,
// iceberg, and backend driver packages, each of which already imports
// this package for the Driver/Dialect/Backend interfaces — composing
// them at the top-level would introduce an import cycle. Making
// Local() a subpackage avoids the cycle and matches database/sql's
// pattern of sub-package factory imports.
func Local() (Client, error) {
	return nil, fmt.Errorf("lakeorm: use the `local` subpackage — `import lakeormlocal \"github.com/datalake-go/lake-orm/local\"` then `lakeormlocal.Open()`")
}
