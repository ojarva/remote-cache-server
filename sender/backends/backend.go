package backends

import "github.com/ojarva/remote-cache-server/sender/types"

// CacheBackend is used to persist overflow when sending is either lagging behind or failing (for example, because of any connectivity issues).
type CacheBackend interface {
	Init() error
	GetOldestIdentifier() (string, error)
	GetNewestIdentifier() (string, error)
	GetCachedContent(identifier string) ([]string, error)
	DeleteCacheItem(identifier string) error
	Save(ob types.OutgoingBatch) error
}
