package uc

import "context"

// Publisher TODO
type Publisher interface {
	PublishSync(ctx context.Context, event any) error
}
