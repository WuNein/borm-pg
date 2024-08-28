package borm

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v4/stdlib"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	uri = "host=localhost user=postgres password=just4fun dbname=postgres port=5432 sslmode=disable TimeZone=Asia/Shanghai"
)

var (
	db *sql.DB
)

func init() {
	var err error
	db, err = sql.Open("pgx", uri)
	if err != nil {
		log.Fatal(err)
	}
}

type x struct {
	X  string    `borm:"name"`
	Y  int64     `borm:"age"`
	Z1 int64     `borm:"ctime" type:"time"`
	Z2 time.Time `borm:"ctime2"`
	Z3 time.Time `borm:"ctime3"`
	Z  int64     `borm:"ctime4"`
}

type xx struct {
	BormLastId int64
	X          string `borm:"name"`
	Y          int64  `borm:"age"`
}

type x1 struct {
	X     string `borm:"name"`
	ctime int64
}

func (x *x1) CTime() int64 {
	return x.ctime
}

type c struct {
	C int64 `borm:"count(1)"`
}

/*
goos: darwin
goarch: amd64
pkg: borm
BenchmarkBormSelect-12    	    1228	   1012782 ns/op	    4304 B/op	      74 allocs/op
PASS
ok  	borm	1.851s
*/
func BenchmarkBormSelectPG(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var o []x
		tbl := Table(db, "test").Reuse().UsePG()
		tbl.Select(&o, Where("`id` >= 1"))
	}
}

/*
goos: darwin
goarch: amd64
pkg: borm
BenchmarkNormalSelect-12    	    1162	    981764 ns/op	    6405 B/op	     107 allocs/op
PASS
ok  	borm	1.572s
*/
func BenchmarkNormalSelectPG(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var o []x
		rows, _ := db.QueryContext(context.TODO(), "select name,age,ctime4,ctime,ctime2,ctime3 from test where id >= 1")
		for rows.Next() {
			var t x
			rows.Scan(&t.X, &t.Y, &t.Z, &t.Z1, &t.Z2, &t.Z3)
			o = append(o, t)
		}

		rows.Close()
	}
}

//pass
func TestSelectPG(t *testing.T) {
	Convey("normal", t, func() {
		Convey("single select", func() {

			var o x
			tbl := Table(db, "test").UsePG()

			for i := 0; i < 10; i++ {
				//orderby before limit
				var ids []int64
				//clear all ` expersion
				_, err := tbl.Debug().Select(&ids, Fields("`id`"), Where(Cond("id >= ?", 1)), GroupBy("id"), Limit(1))
				So(err, ShouldBeNil)
				// So(n, ShouldEqual, 1)
				fmt.Printf("%+v\n", o)
			}
		})

		Convey("multiple select", func() {
			var o []x
			tbl := Table(db, "test").Debug().UsePG()
			//another tempTable with config
			//and replace the first half with select clause
			//Cond("id=?", id)
			n, err := tbl.Select(&o, Where(Gte("id", 0), Lte("id", 1000), Between("id", 0, 1000)), OrderBy("id", "name"), Limit(0, 100))

			So(err, ShouldBeNil)
			So(n, ShouldBeGreaterThanOrEqualTo, 1)
			fmt.Printf("%+v\n", o)
		})
		Convey("multiple select with pointer", func() {
			var o []*x
			tbl := Table(db, "test").Debug().UsePG()

			n, err := tbl.Select(&o, Where(In("id", []interface{}{1, 2, 3, 4}...)))

			So(err, ShouldBeNil)
			So(n, ShouldBeGreaterThanOrEqualTo, 1)

			for _, v := range o {
				fmt.Printf("%+v\n", v)
			}
		})

		Convey("counter", func() {
			var o c
			tbl := Table(db, "test").Debug().UsePG()

			n, err := tbl.Select(&o, GroupBy("id", `name`), Having(Gt("id", 0), Neq("name", "")), Limit(100))

			So(err, ShouldBeNil)
			So(n, ShouldEqual, 1)

			fmt.Printf("%+v\n", o)
		})
		//problem here

		Convey("user-defined fields", func() {
			var o x
			tbl := Table(db, "test").Debug().UsePG()

			n, err := tbl.Select(&o, Fields("name", "ctime", "age"), Where("id >= ?", 1), Limit(100))

			So(err, ShouldBeNil)
			So(n, ShouldEqual, 1)
			fmt.Printf("%+v\n", o)
		})

		Convey("user-defined fields with simple type", func() {
			var cnt int64
			tbl := Table(db, "test").Debug().UsePG()

			n, err := tbl.Select(&cnt, Fields("count(1)"), Where(Eq("id", 1)), Limit(100))

			So(err, ShouldBeNil)
			So(n, ShouldEqual, 1)
			So(n, ShouldBeGreaterThan, 0)
		})

		Convey("user-defined fields with simple slice type", func() {
			var ids []int64
			tbl := Table(db, "test").Debug().UsePG()

			n, err := tbl.Select(&ids, Fields("id"), Limit(100))

			So(err, ShouldBeNil)
			So(n, ShouldBeGreaterThan, 1)
			So(len(ids), ShouldBeGreaterThan, 1)
		})
	})
}

func TestSelectTime(t *testing.T) {
	var o []x
	tbl := Table(db, "test").Debug().UsePG()

	n, err := tbl.Select(&o, Where(Gte("id", 0), Lte("id", 1000), Between("id", 0, 1000)), OrderBy("id", "name"), Limit(0, 100))
	fmt.Println(n, err)

	fmt.Printf("%+v\n", o)
}

func TestInsertPG(t *testing.T) {

	Convey("normal", t, func() {

		Convey("single insert", func() {
			o := x{
				X:  "Orca1",
				Y:  20,
				Z1: 1551405784,
			}
			tbl := Table(db, "test").Debug().UsePG()

			n, err := tbl.Insert(&o)

			So(err, ShouldBeNil)
			So(n, ShouldEqual, 1)
		})
		Convey("single insert", func() {
			o := x{
				X:  "Orca1have?",
				Y:  20,
				Z1: 1551405784,
			}
			tbl := Table(db, "test").Debug().UsePG()

			n, err := tbl.Insert(&o)

			So(err, ShouldBeNil)
			So(n, ShouldEqual, 1)
		})
		Convey("multiple insert", func() {
			o := []*x{
				{
					X:  "Orca4",
					Y:  23,
					Z1: 1551405784,
				},
				{
					X:  "Orca5",
					Y:  24,
					Z1: 1551405784,
				},
			}
			tbl := Table(db, "test").Debug().UsePG()

			n, err := tbl.Insert(&o)

			So(err, ShouldBeNil)
			So(n, ShouldEqual, 2)
		})
		//'ERROR: null value in column "ctime2" of relation "test" violates not-null constraint (SQLSTATE 23502)'
		Convey("user-defined fields", func() {
			o := x{
				X:  "Orca1",
				Y:  20,
				Z1: 1551405784,
			}
			tbl := Table(db, "test").Debug().UsePG()

			n, err := tbl.Insert(&o, Fields("name", "ctime", "ctime2", "ctime3", "ctime4", "age"))

			So(err, ShouldBeNil)
			So(n, ShouldEqual, 1)
		})
	})
}

func TestInsertOne(t *testing.T) {
	o := x{
		X:  "Orca1",
		Y:  20,
		Z1: 1551405784, //add type:"time" to struct
	}
	tbl := Table(db, "test").Debug().UsePG()

	n, err := tbl.Insert(&o)

	fmt.Println(n, err)
}

//pass
func TestUpdatePG(t *testing.T) {

	Convey("normal", t, func() {

		Convey("update", func() {
			o := x{
				X:  "Orca1",
				Y:  20,
				Z1: 1551405784,
			}
			tbl := Table(db, "test").Debug().UsePG()

			n, err := tbl.Update(&o, Where("id = ?", 0))
			fmt.Println(n, err)

			So(err, ShouldBeNil)
			// So(n, ShouldBeGreaterThan, 0)
		})
		Convey("update with map", func() {
			tbl := Table(db, "test").Debug().UsePG()

			n, err := tbl.Update(V{
				"name": "OrcaUpdated",
				"age":  88,
			}, Where("id = ?", 1))
			fmt.Println(n, err)

			So(err, ShouldBeNil)
			// So(n, ShouldBeGreaterThan, 0)
		})
		Convey("update with U", func() {
			tbl := Table(db, "test").Debug().UsePG()

			n, err := tbl.Update(V{
				"age": U("age+1"),
			}, Where("id = ?", 1))
			fmt.Println(n, err)

			So(err, ShouldBeNil)
			// So(n, ShouldBeGreaterThan, 0)
		})

	})
}

func TestSto(t *testing.T) {
	temp := V{
		"age": U("age+1"),
	}
	temp["test"] = "test"
	fmt.Println(temp)
}

func TestUpdateOne(t *testing.T) {
	tbl := Table(db, "test").Debug().UsePG()

	n, err := tbl.Update(V{
		"name": "OrcaUpdated",
		"age":  88,
	}, Where("id = ?", 2))
	fmt.Println(n, err)
}

func TestDelete(t *testing.T) {

	Convey("normal", t, func() {
		//提供一个拼嵌套查询的接口
		//not exist in pg
		//in (select * limit 1)
		// Convey("single delete", func() {
		// 	tbl := Table(db, "test").Debug()

		// 	n, err := tbl.Delete(Where("id=1"), Limit(1))

		// 	So(err, ShouldBeNil)
		// 	So(n, ShouldBeGreaterThanOrEqualTo, 0)
		// })

		Convey("bulk delete", func() {
			tbl := Table(db, "test").Debug().UsePG()

			n, err := tbl.Delete(Where("id=1"))

			So(err, ShouldBeNil)
			So(n, ShouldBeGreaterThanOrEqualTo, 0)
		})
	})
}

func TestForceIndexPG(t *testing.T) {
	//not support
	Convey("normal", t, func() {
		So(ForceIndex("idx_ctime").Type(), ShouldEqual, _forceIndex)

		var ids []int64
		tbl := Table(db, "test").Debug().UsePG()

		n, err := tbl.Select(&ids, Fields("id"), ForceIndex("idx_ctime"), Limit(100))

		So(err, ShouldBeNil)
		So(n, ShouldBeGreaterThan, 1)
		So(len(ids), ShouldBeGreaterThan, 1)
	})
}

type xTemp struct {
	X  string    `borm:"test.name"`
	Y  int64     `borm:"test.age"`
	Z1 int64     `borm:"test.ctime" type:"time"`
	Z2 time.Time `borm:"test.ctime2"`
	Z3 time.Time `borm:"test.ctime3"`
	Z  int64     `borm:"test.ctime4"`
}

func (tmp *xTemp) TableName() string {
	return "test"
}

func TestJoin(t *testing.T) {
	tbl := Table(db, "test inner join test2 on test.age = test2.age").Debug() // 表名用join语句
	// tag 上面需要表名
	// 不太实用
	var o []xTemp
	n, err := tbl.Select(&o, Where("1 =1 ")) // 条件加上表名
	fmt.Println(n, err)
	fmt.Println(o)
}
