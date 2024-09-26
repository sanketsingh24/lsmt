package lru

type LruList[T comparable] struct {
	items []T
}

func NewLruList[T comparable]() *LruList[T] {
	return &LruList[T]{
		items: make([]T, 0),
	}
}

func NewLruListWithCapacity[T comparable](n int) *LruList[T] {
	return &LruList[T]{
		items: make([]T, 0, n),
	}
}

func (l *LruList[T]) removeBy(f func(T) bool) {
	newItems := make([]T, 0, len(l.items))
	for _, item := range l.items {
		if f(item) {
			newItems = append(newItems, item)
		}
	}
	l.items = newItems
}

func (l *LruList[T]) Remove(item T) {
	l.removeBy(func(x T) bool {
		return x != item
	})
}

func (l *LruList[T]) Refresh(item T) {
	l.Remove(item)
	l.items = append(l.items, item)
}

func (l *LruList[T]) GetLeastRecentlyUsed() (T, bool) {
	if len(l.items) == 0 {
		var zero T
		return zero, false
	}
	front := l.items[0]
	l.items = l.items[1:]
	l.items = append(l.items, front)
	return front, true
}
