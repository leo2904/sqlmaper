// Package sqlmaper provides a way to parse and handle the sql statements found in a sql file
// This sql file could not be any sql file, it needs some kind of metadata information to
// handle everything properly, or not!!!
package sqlmaper

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
)

// Query is a parsed query along with its associated information
type Query struct {
	Query string            // SQL statement
	Type  int               // query tipe (DML, DQL o DDL)
	Tags  map[string]string // additional information in the form of: -- tag_name: tag_value
	idx   int
}

// String satisfy stringer interface
func (q Query) String() string {
	var str strings.Builder
	str.WriteString(fmt.Sprintf("Query: %s", q.Query))
	str.WriteString(fmt.Sprintf("Type: %d", q.Type))
	for tag, value := range q.Tags {
		str.WriteString(fmt.Sprintf("Tag: %s - Value: %s", tag, value))
	}
	return str.String()
}

// Statement is a helper function to get the query statement ready to be executed
func (q Query) Statement() string {
	return q.Query
}

// QueryType is a helper function to get the type of the query
func (q Query) QueryType() int {
	return q.Type
}

// TagValue is a helper function to get the value of the given query tag identifier (label)
func (q Query) TagValue(tag string) string {
	v, ok := q.Tags[strings.ToLower(tag)]
	if !ok {
		return ""
	}
	return v
}

// Queries is a query container
// The key value is implemented as a consecutive number of the valid queries (the executable ones) retrieved from the file
// Accessing the map by an ordered key ensures that the sentences are processed in the order in which they are in the parsed sql file
type Queries map[string]*Query // the will be the value of the special tag "-- name:"

const (
	// UKN - Unknow
	UKN = iota
	// DML - Data Manipulation Languaje (insert, update, etc)
	DML
	// DQL - Data Query Languaje (select)
	DQL
	// DDL - Data Definition Languaje (create, drop, etc)
	DDL

	// TagRegularRegExp is a regular expression to get the regular tags (-- tag=*:)
	TagRegularRegExp = "(?i)^\\s*--\\s*tag\\s*=\\s*[a-z0-9_-]+\\s*:\\s*"

	// TagPrefixRegExp is a regular expression to get the prefix of the regular tags (-- tag=)
	TagPrefixRegExp = "(?i)^\\s*--\\s*tag\\s*=\\s*"
)

const (
	lineName = iota
	lineQuery
	lastLineQuery
	lineTag
	lineToSkip
	lineComment
)

var (
	reTagRegular = regexp.MustCompile(TagRegularRegExp)
	reTagPrefix  = regexp.MustCompile(TagPrefixRegExp)
)

type parsedLine struct {
	Type  int
	Tag   string
	Value string
}

// Query is a helper function to get the Query of the given label (tag=name value)
func (q Queries) Query(label string) *Query {
	query, ok := q[label]
	if !ok {
		return nil
	}
	return query
}

// Statement is a helper to obtain the query statement of a given query
func (q Queries) Statement(label string) string {
	label = strings.ToLower(label)
	v, ok := q[label]
	if !ok {
		return ""
	}
	return v.Statement()
}

// TagValue is a helper to obtain the tag value of a given query tag
func (q Queries) TagValue(label, tag string) string {
	label = strings.ToLower(label)
	qry, ok := q[label]
	if !ok {
		return ""
	}
	return qry.TagValue(tag)
}

// QueryType is a helper to obtain the type of a given query
func (q Queries) QueryType(label string) int {
	label = strings.ToLower(label)
	v, ok := q[label]
	if !ok {
		return UKN
	}
	return v.QueryType()
}

// ParseFile reads a file and returns Queries or an error
func ParseFile(path string) (Queries, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	return ParseReader(file)
}

// OrderedNames contains a list of query names sorted by the order
// in wich they appear in the sql file.
// This list is useful when the original sequence of events should
// be preserved
type OrderedNames []string

// ParseReader process the stream and returns Queries or an error
func ParseReader(r io.Reader) (Queries, error) {
	var (
		queries = make(Queries)
		q       *Query
		qName   string
		scn     = bufio.NewScanner(r)
		idx     int
	)

	FF := true // Fast Forward
	for scn.Scan() {
		pl := parseLine(scn.Text())
		switch pl.Type {
		case lineToSkip:
			continue

		case lineName:
			FF = false
			qName = strings.ToLower(pl.Value)
			if _, ok := queries[qName]; ok {
				return nil, fmt.Errorf("duplicated query name: %q", qName)
			}
			q = &Query{}
			q.Tags = make(map[string]string)
			q.Tags[strings.ToLower(pl.Tag)] = pl.Value

		case lineTag:
			if FF {
				continue
			}
			q.Tags[strings.ToLower(pl.Tag)] = pl.Value

		case lineQuery, lastLineQuery:
			if FF {
				continue
			}
			if len(q.Query) > 0 {
				q.Query += " "
			}
			q.Query += pl.Value
			if pl.Type == lastLineQuery {
				FF = true
				q.Type = sqlType(q.Query)
				if q.Type != DDL {
					q.Query = scapeColon(q.Query)
				}
				q.idx = idx
				queries[qName] = q
				idx++
			}
		}
	}
	return queries, nil
}

func sqlType(q string) int {
	if len(q) < 10 {
		return UKN
	}

	s := strings.ToUpper(q[:10])

	switch s[:6] {
	case "INSERT", "UPDATE", "DELETE", "MERGE":
		return DML
	case "SELECT":
		return DQL
	case "CREATE":
		return DDL
	}

	switch s[:10] {
	case "DROP TABLE", "DROP INDEX":
		return DDL
	}
	return UKN
}

// parseLine classify a single line
func parseLine(line string) parsedLine {
	line = strings.TrimSpace(line)
	if line == "" {
		return parsedLine{Type: lineToSkip}
	}
	if len(line) >= 6 && strings.ToUpper(line[:6]) == "COMMIT" {
		return parsedLine{Type: lineToSkip}
	}
	if len(line) >= 8 && strings.ToUpper(line[:8]) == "ROLLBACK" {
		return parsedLine{Type: lineToSkip}
	}

	if matches := reTagRegular.FindStringSubmatch(line); len(matches) > 0 {
		tagName := tagName(matches[0])
		tagType := lineTag
		if tagName == "name" {
			tagType = lineName
		}
		return parsedLine{Type: tagType, Tag: tagName, Value: strings.TrimPrefix(line, matches[0])}
	}

	if strings.HasPrefix(line, "--") {
		return parsedLine{Type: lineComment}
	}

	// the sql sentences could be multilineal and the final result is a single line with the whole sentences
	// therefor is necessary to strip out any single comment that could be at the end of each sql line
	if pos := strings.Index(line, "--"); pos > 0 {
		line = strings.TrimSpace(line[:pos])
	}

	if isQueryLastLine(line) {
		return parsedLine{Type: lastLineQuery, Value: line}
	}

	return parsedLine{Type: lineQuery, Value: line}
}

// tagName returns the name of the tag in lowercase (eg: --tag=FileName returns filename)
func tagName(tag string) string {
	match := reTagPrefix.FindStringSubmatch(tag)
	if len(match) > 0 {
		tag = strings.TrimPrefix(tag, match[0])
	}

	i := strings.Index(tag, ":")
	if i > 0 {
		tag = tag[:i]
	}

	return strings.TrimSpace(strings.ToLower(tag))
}

// isQueryLastLine returns true if the line ends in ; or if there is a ; before the first comment
func isQueryLastLine(line string) bool {
	// sc	-> SemiColon (;)
	// slc	-> SingleLine Comment (--)
	// mlco	-> MultiLine Comment Open (/*)
	// mlcc	-> MultiLine Comment Close (*/)

	sc := strings.Index(line, ";")
	if sc == -1 {
		return false
	}

	slc := strings.Index(line, "--")
	mlco := strings.Index(line, "/*")
	mlcc := strings.Index(line, "*/")
	if slc > 0 && mlco == -1 && mlcc == -1 && slc < sc {
		return false
	}

	if mlco > 0 {
		if mlcc > 0 {
			if sc > mlco && sc < mlcc {
				return false
			}
		}
		if sc > mlco && mlcc == -1 {
			return false
		}
	}
	return true
}

// scapeColon scapes every single colon for a safety use of bind variables
// using sqlx package
func scapeColon(s string) string {
	if !strings.Contains(s, ":") {
		return s
	}

	if !strings.Contains(s, "=") {
		return s
	}

	var str strings.Builder
	str.Grow(len(s) + 10) // at this point I know for sure that at least one colon wil be duplicate so I make some room for 9 more

	colon := false
	equal := false
	bindVarFound := false

	for i := 0; i < len(s); i++ {
		if equal == true && s[i] != ' ' {
			equal = false
			if s[i] == ':' {
				str.WriteByte(s[i])
				bindVarFound = true
				continue
			}
		}
		if colon == true {
			if s[i] != ':' {
				str.WriteByte(':')
				colon = false
			}
		}
		if s[i] == ':' {
			colon = !colon
		}
		str.WriteByte(s[i])
		if s[i] == '=' {
			equal = true
		}
	}
	if colon == true {
		str.WriteByte(':')
	}
	if bindVarFound == false {
		return s
	}
	return str.String()
}

//TODO: new idea... parse a regular sql file (the one that everybody writes without
// the format imposed by this package).
// The map ID could be a query's hash and the querys name I don't know yet, maybe nothing)
// ParseFreeFileReader parse an unstructured query file
func ParseFreeFileReader(r io.Reader) (*Query, error) {
	return nil, nil
}
