package log

// Loggable describes objects that can be marshalled into Metadata for logging
type Loggable interface {
	Loggable() map[string]interface{}
}

type LoggableMap map[string]interface{}

func (l LoggableMap) Loggable() map[string]interface{} {
	return l
}

// LoggableF converts a func into a Loggable
type LoggableF func() map[string]interface{}

func (l LoggableF) Loggable() map[string]interface{} {
	return l()
}

func Deferred(key string, f func() string) Loggable {
	function := func() map[string]interface{} {
		return map[string]interface{}{
			key: f(),
		}
	}
	return LoggableF(function)
}

func Pair(key string, l Loggable) Loggable {
	return LoggableMap{
		key: l,
	}
}
