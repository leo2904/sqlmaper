package sqlmaper

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFileOrderIterator(t *testing.T) {
	sqlFile := `
-- tag=name: Select1
-- tag=FileName: peoples.psv
select * from peoples;
-- tag=name: Select2
select * from cities;
-- tag=name: Update1
update peoples set Name = 'Leo' where ID = 1;
-- tag=name: Insert1
insert into Cities (ID, Name) values (1, 'Barcelone');
-- tag=name: CreateTable
create table countries (ID number, Name varchar2(50));
-- tag=name: FAKE1
-- select * from Fake;
-- tag=name: CreateIndex
create index counX1 on countries (ID);
`

	queries, err := ParseReader(strings.NewReader(sqlFile))
	if err != nil {
		t.Errorf("error en ParseReader: %v", err)
	}
	iter := queries.NewFileOrderIterator()
	i := 0
	for iter.Iterate() {
		i++
		switch i {
		case 1:
			assert.Equal(t, "select * from peoples;", iter.Statement(), "")
			assert.Equal(t, DQL, iter.QueryType())
			assert.Equal(t, "Select1", iter.TagValue("name"))
			assert.Equal(t, "peoples.psv", iter.TagValue("fileName"))
		case 2:
			assert.Equal(t, "select * from cities;", iter.Statement(), "")
			assert.Equal(t, DQL, iter.QueryType())
		case 3:
			assert.Equal(t, "update peoples set Name = 'Leo' where ID = 1;", iter.Statement(), "")
			assert.Equal(t, DML, iter.QueryType())
		case 4:
			assert.Equal(t, "insert into Cities (ID, Name) values (1, 'Barcelone');", iter.Statement(), "")
			assert.Equal(t, DML, iter.QueryType())
		case 5:
			assert.Equal(t, "create table countries (ID number, Name varchar2(50));", iter.Statement(), "")
			assert.Equal(t, DDL, iter.QueryType())
		case 6:
			assert.Equal(t, "create index counX1 on countries (ID);", iter.Statement(), "")
			assert.Equal(t, DDL, iter.QueryType())
		default:
			t.Errorf("Should be not here, ever - i: %d", i)
		}
	}

}

func TestConcurrentIterator(t *testing.T) {
	sqlFile := `
-- tag=name: Select1
-- tag=FileName: peoples.psv
select * from peoples;
-- tag=name: Select2
select * from cities;
-- tag=name: Update1
update peoples set Name = 'Leo' where ID = 1;
-- tag=name: Insert1
insert into Cities (ID, Name) values (1, 'Barcelone');
-- tag=name: CreateTable
create table countries (ID number, Name varchar2(50));
-- tag=name: FAKE1
-- select * from Fake;
-- tag=name: CreateIndex
create index counX1 on countries (ID);
-- tag=name: Select3
select * from KK3;
`

	queries, err := ParseReader(strings.NewReader(sqlFile))
	if err != nil {
		t.Errorf("error en ParseReader: %v", err)
	}
	seqIter, concIter := queries.NewConcurrentIterators()
	i := 0
	for seqIter.Iterate() {
		i++
		switch i {
		case 1:
			assert.Equal(t, "update peoples set Name = 'Leo' where ID = 1;", seqIter.Statement())
			assert.Equal(t, DML, seqIter.QueryType())
		case 2:
			assert.Equal(t, "insert into Cities (ID, Name) values (1, 'Barcelone');", seqIter.Statement())
			assert.Equal(t, DML, seqIter.QueryType())
		case 3:
			assert.Equal(t, "create table countries (ID number, Name varchar2(50));", seqIter.Statement())
			assert.Equal(t, DDL, seqIter.QueryType())
		case 4:
			assert.Equal(t, "create index counX1 on countries (ID);", seqIter.Statement())
			assert.Equal(t, DDL, seqIter.QueryType())
		default:
			t.Errorf("Should be not here, ever - i: %d", i)
		}
	}

	assert.Greater(t, i, 0, "not found sequencial queries")

	i = 0
	for concIter.Iterate() {
		i++
		switch i {
		case 1:
			assert.Equal(t, "select * from peoples;", concIter.Statement())
			assert.Equal(t, DQL, concIter.QueryType())
			assert.Equal(t, "Select1", concIter.TagValue("name"))
			assert.Equal(t, "peoples.psv", concIter.TagValue("fileName"))
		case 2:
			assert.Equal(t, "select * from cities;", concIter.Statement())
			assert.Equal(t, DQL, concIter.QueryType())
		case 3:
			assert.Equal(t, "select * from KK3;", concIter.Statement())
		default:
			t.Errorf("Should be not here, ever - i: %d", i)
		}
	}

	assert.Greater(t, i, 0, "not found concurrent queries")
}

func TestConcurrentIteratorWithoutDDLnorDML(t *testing.T) {
	sqlFile := `
-- tag=name: Select1
-- tag=FileName: peoples.psv
select * from peoples;
-- tag=name: Select2
select * from cities;
`

	queries, err := ParseReader(strings.NewReader(sqlFile))
	assert.Nil(t, err, "error in ParseReader")
	seqIter, concIter := queries.NewConcurrentIterators()

	for seqIter.Iterate() {
		assert.Nil(t, seqIter.queries, "should be not queries in sequential iterator")
	}

	i := 0
	for concIter.Iterate() {
		i++
		switch i {
		case 1:
			assert.Equal(t, "select * from peoples;", concIter.Statement(), "")
			assert.Equal(t, DQL, concIter.QueryType())
			assert.Equal(t, "Select1", concIter.TagValue("name"))
			assert.Equal(t, "peoples.psv", concIter.TagValue("fileName"))
		case 2:
			assert.Equal(t, "select * from cities;", concIter.Statement(), "")
			assert.Equal(t, DQL, concIter.QueryType())
		default:
			t.Errorf("Should be not here, ever - i: %d", i)
		}
	}
}

func TestConcurrentIteratorWithoutDQL(t *testing.T) {
	sqlFile := `
-- tag=name: Update1
update peoples set Name = 'Leo' where ID = 1;
-- tag=name: Insert1
insert into Cities (ID, Name) values (1, 'Barcelone');
-- tag=name: CreateTable
create table countries (ID number, Name varchar2(50));
`

	queries, err := ParseReader(strings.NewReader(sqlFile))
	assert.Nil(t, err, "error in ParseReader")
	seqIter, concIter := queries.NewConcurrentIterators()

	i := 0
	for seqIter.Iterate() {
		i++
		switch i {
		case 1:
			assert.Equal(t, "update peoples set Name = 'Leo' where ID = 1;", seqIter.Statement())
			assert.Equal(t, DML, seqIter.QueryType())
			assert.Equal(t, "Update1", seqIter.TagValue("name"))
		case 2:
			assert.Equal(t, "insert into Cities (ID, Name) values (1, 'Barcelone');", seqIter.Statement())
			assert.Equal(t, DML, seqIter.QueryType())
		case 3:
			assert.Equal(t, "create table countries (ID number, Name varchar2(50));", seqIter.Statement())
			assert.Equal(t, DDL, seqIter.QueryType())
			assert.Equal(t, "CreateTable", seqIter.TagValue("name"))
		default:
			t.Errorf("Should be not here, ever - i: %d", i)
		}
	}

	for concIter.Iterate() {
		assert.Nil(t, seqIter.queries, "should be not queries in concurrent iterator")
	}
}
