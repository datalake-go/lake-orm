package structs

import "errors"

// ErrInvalidTag is the sentinel wrapped by every ParseSchema error
// that rejects malformed or disallowed tag grammar. Callers check
// with errors.Is(err, structs.ErrInvalidTag).
var ErrInvalidTag = errors.New("lakeorm/structs: invalid struct tag")
