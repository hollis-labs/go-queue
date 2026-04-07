package queue

import "errors"

// ErrNoJob is returned by Pop when no job is available. Drivers return
// nil, nil instead of this error — this sentinel exists for internal
// worker logic.
var ErrNoJob = errors.New("no job available")

// ErrHandlerNotFound is used when a job is popped but no handler is
// registered for its type.
var ErrHandlerNotFound = errors.New("handler not found for job type")
