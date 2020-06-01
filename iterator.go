package sqlmaper

// Iterator defines an interator over the Queries map to fetch every single Query
// in a spefic order
type Iterator struct {
	queries      Queries
	orderedNames []string
	idNames      int
	query        *Query
}

// NewFileOrderIterator returns an Iterator that allows to fetch every individual Query
// in the same orden as they are in the sql file
func (q Queries) NewFileOrderIterator() *Iterator {
	return &Iterator{queries: q,
		orderedNames: initFileOrderIterator(q),
		idNames:      -1,
	}
}

// NewConcurrentIterators provides two iterators, one to iterates over DML & DDL statements
// in a sequential an ordered way (sequentialIter) and another one (concurrentIter) to iterates
// over the DQL sentences
// The idea behind this is to allow the posibility to execute concurrently all the sentences
// with no dependencies (DQL) to speed up the process
func (q Queries) NewConcurrentIterators() (sequentialIter *Iterator, concurrentIter *Iterator) {
	sequentialIter = &Iterator{queries: q,
		orderedNames: initSequentialIterator(q),
		idNames:      -1,
	}

	concurrentIter = &Iterator{queries: q,
		orderedNames: initConcurrentIterator(q),
		idNames:      -1,
	}
	return sequentialIter, concurrentIter
}

// Iterate iterates over the Queries map
func (i *Iterator) Iterate() bool {
	i.idNames++
	if i.idNames > len(i.orderedNames)-1 {
		return false
	}
	i.query = i.queries.Query(i.orderedNames[i.idNames])
	if i.query == nil {
		return false
	}
	return true
}

// Statement returns the query fetched in the last iteration
func (i *Iterator) Statement() string {
	return i.query.Statement()
}

// QueryType returns the type of the query fetched in the last iteration
func (i *Iterator) QueryType() int {
	return i.query.QueryType()
}

// TagValue returns the tag value of the tags asociated with the query fetched in the last iteration
func (i *Iterator) TagValue(tag string) string {
	return i.query.TagValue(tag)
}

func initFileOrderIterator(q Queries) []string {
	foi := make([]string, len(q))
	for k, v := range q {
		foi[v.idx] = k
	}
	return foi
}

func initSequentialIterator(q Queries) []string {
	// In this case where are interested in all type of statements except selects
	return initIterator(q, false)
}

// initConcurrentIterator returns a slice of query names in the order to be procesed
// in this particular case the order souldn't be necessary but it is to easy the testing
func initConcurrentIterator(q Queries) []string {
	// In this case we are only interested in select statements because these are
	// the only type of statements that could be executed in concurrent way
	return initIterator(q, true)
}

func initIterator(q Queries, dql bool) []string {
	foi := make([]string, len(q))
	cant := 0
	for k, v := range q {
		if dql {
			if v.Type != DQL {
				continue
			}
		} else {
			if v.Type == DQL {
				continue
			}
		}
		foi[v.idx] = k
		cant++
	}

	if cant == 0 {
		return nil
	}

	si := make([]string, cant)
	i := 0
	for _, f := range foi {
		if f != "" {
			si[i] = f
			i++
		}
	}
	return si
}
