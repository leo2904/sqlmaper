package sqlmaper

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseLine(t *testing.T) {
	var tests = []struct {
		feed     string
		expected parsedLine
	}{
		{"", parsedLine{Type: lineToSkip, Tag: "", Value: ""}},
		{"commit;", parsedLine{Type: lineToSkip, Tag: "", Value: ""}},
		{" commit", parsedLine{Type: lineToSkip, Tag: "", Value: ""}},
		{"  commit -- commit de los cambios", parsedLine{Type: lineToSkip, Tag: "", Value: ""}},
		{"  rollback -- rollback de los cambios", parsedLine{Type: lineToSkip, Tag: "", Value: ""}},
		{"roll", parsedLine{Type: lineQuery, Tag: "", Value: "roll"}}, // this test is to validate that the string slicing are protected, doesn't matter if the sql sentences is not valid
		{"select count(*) from peoples;", parsedLine{Type: lastLineQuery, Tag: "", Value: "select count(*) from peoples"}},
		{"select count(*)\nfrom pets   -- quantity of pets", parsedLine{Type: lineQuery, Tag: "", Value: "select count(*)\nfrom pets"}},
		{"-- tag:name= Quantity of peoples", parsedLine{Type: lineName, Tag: "name", Value: "Quantity of peoples"}},
		{"--tag : NAME= Quantity of pets", parsedLine{Type: lineName, Tag: "name", Value: "Quantity of pets"}},
		{"-- tag: FileName= peoples.unl", parsedLine{Type: lineTag, Tag: "filename", Value: "peoples.unl"}},
		{"--tag:FileName_2-KK= peoples.unl", parsedLine{Type: lineTag, Tag: "filename_2-kk", Value: "peoples.unl"}},
		{"-- tag:= unknown 1", parsedLine{Type: lineComment, Tag: "", Value: ""}},
		{"-- notas varias= 1) los commit son ignorados", parsedLine{Type: lineComment, Tag: "", Value: ""}},
		{"-- kk= unknown 3", parsedLine{Type: lineComment, Tag: "", Value: ""}},
		{"-- commit", parsedLine{Type: lineComment, Tag: "", Value: ""}},
		{"-- coMMit", parsedLine{Type: lineComment, Tag: "", Value: ""}},
		{"insert /*+ append */ into peoples select * from aux_peoples;", parsedLine{Type: lastLineQuery, Tag: "", Value: "insert /*+ append */ into peoples select * from aux_peoples"}},
		// multiline comments are not supported yet
		// {"select id, name from peoples /*  i will put the ; at the end of this comment", parsedLine{Type: lineQuery, Tag: "", Value: "select id, name from peoples"}},
		// {"now the end of the comment and the ;*/;", parsedLine{Type: lineQuery, Tag: "", Value: ";"}},
	}

	for i, tt := range tests {
		assert.Equal(t, tt.expected, parseLine(tt.feed), tt.feed, "Case: %d", i)
	}
}

func TestParseQueryFile(t *testing.T) {
	fileRecord := `
select count(*) as Cant from Employees;
create table TempSales as select * from Sales where CompanyID = :IDCompany;
select * from TempSales;
`
	q, err := ParseFreeFileReader(strings.NewReader(fileRecord))
	assert.Nil(t, err, "error: %v", err)
	_ = q
}

func TestQueryTerminator(t *testing.T) {
	var tests = []struct {
		feed     string
		expected bool
	}{
		{"", false},
		{";", true},
		{";--comment at the end", true},
		{"select count(*) from peoples", false},
		{"select count(*) from peoples;", true},
		{"select count(*) from peoples; -- people count", true},
		{"select count(*) from peoples -- people count;", false},
		{"/*", false},
		{"*/", false},
		{"/* comment */", false},
		{"now the end of the comment and the ;/*;", true},
		{"now the end of the comment and the ;*/;", true},
		{"now the end of the comment and the ;/**/;", true},
		{"insert /*+ append */ into peoples select * from aux_peoples;", true},
		{"select id, name from peoples; /* comment", true},
		{"select id, name from peoples /* comment */;", true},
		{"select id, name from peoples; /* comment */", true},
		{"select id, name from peoples /* comment; */", false},
		{"select id, name from peoples /* comment ;", false},
	}

	for i, tt := range tests {
		assert.Equal(t, tt.expected, isQueryLastLine(tt.feed), tt.feed, "Case: %d", i)
	}
}

var resultIQLL bool

func BenchmarkIsQueryLastLine(b *testing.B) {
	var r bool
	for i := 0; i < b.N; i++ {
		r = isQueryLastLine("insert /*+ append */ into peoples select * from aux_peoples;")
	}
	resultIQLL = r
}

func TestScapeColons(t *testing.T) {
	var tests = []struct {
		feed     string
		expected string
	}{
		{"", ""},
		{":", ":"},
		{"a", "a"},
		{"to_char(sysdate,'HH24:MM:SS')", "to_char(sysdate,'HH24::MM::SS')"},
		{"to_char(sysdate,'HH24:MM:SS') and DoctorID = :DoctorID and Status = :Status order by PatientID", "to_char(sysdate,'HH24::MM::SS') and DoctorID = :DoctorID and Status = :Status order by PatientID"},
		{"a:", "a:"},
		{"a:b", "a:b"},
		{":a=:b:", ":a=:b:"},
		{":=:", ":=:"},
		{"f=t(':", "f=t('::"},
		{"con.Fecha = to_date(:FechaIni, 'YYYYMMDD')", "con.Fecha = to_date(:FechaIni, 'YYYYMMDD')"},
		{"con.FechaHora = to_date(:FechaHoraIni, 'YYYYMMDD HH:MI::SS')", "con.FechaHora = to_date(:FechaHoraIni, 'YYYYMMDD HH::MI::SS')"},
		{"con.Cen_ID = :CenterID and con.Fecha between to_date(:FechaIni, 'YYYYMMDD') and to_date(:FechaFin, 'YYYYMMDD')", "con.Cen_ID = :CenterID and con.Fecha between to_date(:FechaIni, 'YYYYMMDD') and to_date(:FechaFin, 'YYYYMMDD')"},
		{"con.Cen_ID = :CenterID and con.Fecha between to_date(:FechaIni, 'YYYYMMDD HH:MI:SS') and to_date(:FechaFin, 'YYYYMMDD HH24:MI:SS')", "con.Cen_ID = :CenterID and con.Fecha between to_date(:FechaIni, 'YYYYMMDD HH::MI::SS') and to_date(:FechaFin, 'YYYYMMDD HH24::MI::SS')"},
	}

	for i, tt := range tests {
		assert.Equal(t, tt.expected, scapeColons(tt.feed), tt.feed, "Case: %d", i)
	}
}

var resultSC string

func BenchmarkScapeColon(b *testing.B) {
	var r string
	for i := 0; i < b.N; i++ {
		scapeColons("select to_char(sysdate,'HH24:MM:SS'), to_date('2020-05-23 17:18:19','YYYY-MM-DD HH24:MM:SS') from peoples where peopleID = :IDPeople and peopleName=peopleName")
	}
	resultSC = r
}

func TestSqlType(t *testing.T) {
	var tests = []struct {
		feed     string
		expected int
	}{
		{"", UKN},
		{"select * from dual;", DQL},
		{"create index KKX1 on KK(ID);", DDL},
		{"delete from KK where ID = 1;", DML},
		{"drop table", DDL},
		{"-- select 1 from dual;", UKN},
		{"with aux_agendas as (select test_id from dual) select * from aux_agendas;", DQL},
	}

	for i, tt := range tests {
		assert.Equal(t, tt.expected, sqlType(tt.feed), tt.feed, "Case:", i)
	}
}

var resultST int

func BenchmarkSqlType(b *testing.B) {
	var r int
	for i := 0; i < b.N; i++ {
		r = sqlType("drop table KK;")
	}
	resultST = r
}

type Feed struct {
	name  string
	query string
}

func (f Feed) String() string {
	return fmt.Sprintf("-- tag:name= %s\n%s\n", f.name, f.query)
}

func TestParseReader(t *testing.T) {

	var tests = []struct {
		feed        Feed
		expected    Query
		shouldError bool
	}{
		{feed: Feed{name: "test1", query: "select count(*), 'HH:MM:SS' from peoples where idPeople = :idPeople;"},
			expected:    Query{Query: "select count(*), 'HH::MM::SS' from peoples where idPeople = :idPeople", Type: DQL},
			shouldError: false},
		{feed: Feed{name: "test2", query: "insert into peoples select peopleID, name from auxPeople where peopleID = :idPeople;"},
			expected:    Query{Query: "insert into peoples select peopleID, name from auxPeople where peopleID = :idPeople", Type: DML},
			shouldError: false},
		{feed: Feed{name: "test3", query: "create index KKX1 on KK(ID);"},
			expected:    Query{Query: "create index KKX1 on KK(ID)", Type: DDL},
			shouldError: false},
	}
	for i, tt := range tests {
		got, err := ParseReader(strings.NewReader(tt.feed.String()))
		if err != nil {
			assert.Equal(t, tt.shouldError, true, "not expected error: %v - Case: %d", err, i)
			continue
		}
		q, ok := got[tt.feed.name]
		if !assert.Equal(t, true, ok, "query not found: %q - Case: %d", tt.feed.name, i) {
			continue
		}
		if !assert.Equal(t, tt.expected.Type, q.Type, "Case: %d", i) {
			continue
		}
		if !assert.Equal(t, tt.expected.Query, q.Query, "Case: %d", i) {
			continue
		}
	}
}

func TestParseReaderMultiQueries(t *testing.T) {
	queries := make(Queries)

	tags := make(map[string]string)
	tags["name"] = "Peoples"
	tags["hasher"] = "1,1,0,0,0"
	tags["filename"] = "peoples.psv"

	queries["peoples"] = &Query{
		Query: "select PeopleID from Peoples",
		Type:  DQL,
		Tags:  tags,
		idx:   0,
	}

	tags = make(map[string]string)
	tags["name"] = "Cities"
	tags["hasher"] = "1,0,0"
	tags["filename"] = "cities.psv"
	tags["repo"] = "/shared/test"

	queries["cities"] = &Query{
		Query: "select CityID from cities where CountryID = :CountryID",
		Type:  DQL,
		Tags:  tags,
		idx:   1,
	}

	var tests = []struct {
		fileRecords string
		expected    Queries
		shouldError bool
		errorText   string
	}{
		{`
-- tag:name= Test1
select 1 from dual;
-- tag: name= Test1
select 2 from dual;`, nil, true, `duplicated query name: "test1"`},

		{`
-- tag:name= Peoples
--master table
-- tag:hasher=1,1,0,0,0
--tag:fileName=peoples.psv
select PeopleID from Peoples;
commit;

--tag:name=Cities
--tag:hasher=1,0,0
--tag:fileName=cities.psv
-- Basic city information
-- tag : repo= /shared/test
-- testing multilines query with an inline comment
select CityID
from cities -- city table
where CountryID = :CountryID; -- comment at the end
select * from Provinces;
rollback;
`, queries, false, ""},
	}

	for i, tt := range tests {
		got, err := ParseReader(strings.NewReader(tt.fileRecords))
		if err != nil {
			if tt.shouldError {
				assert.NotNil(t, err, "error expected")
				continue
			}
			if !assert.Nil(t, err, "error not expected: %v", err) {
				continue
			}
		}

		if !assert.Equal(t, len(tt.expected), len(got), "the maps length are different - Case: %d", i) {
			continue
		}

		if !assert.Equal(t, tt.expected, got, "queries gotted is not what was expected - Case: %d", i) {
			continue
		}
	}
}

func TestHelpers(t *testing.T) {
	fileRecords := `
--tag:name=Cities
-- tag:hasher=1,0,0
-- tag:fileName=cities.psv
-- tag : repo= /shared/test
select CityID from cities where CountryID = :CountryID;

-- tag:name= Peoples
-- tag:hasher=1,1,0,0,0
-- tag:fileName=peoples.psv
select PeopleID from Peoples;
`
	queries, err := ParseReader(strings.NewReader(fileRecords))
	assert.Nil(t, err, "got error when it wasn't expected")

	assert.Equal(t, `select PeopleID from Peoples`, queries.Statement("Peoples"), "Peoples")

	assert.Equal(t, "", queries.Statement("KK"), "KK")

	assert.Equal(t, "cities.psv", queries.TagValue("Cities", "fileName"), "Cities & fileName")

	assert.Equal(t, "peoples.psv", queries.TagValue("PEOPLES", "FILENAME"), "PEOPLES & FILENAME")

	assert.Equal(t, "", queries.TagValue("PEOPLES", "KK"), "PEOPLES & KK")

	assert.Equal(t, "", queries.TagValue("KK", "filename"), "KK & filename")

	assert.Equal(t, DQL, queries.QueryType("cities"), "cities")

	assert.Equal(t, UKN, queries.QueryType("KK"), "KK")
}
