package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	// 连接数据库1
	db1, err := sql.Open("mysql", "root:password@tcp(host:port)/test1")
	if err != nil {
		log.Fatal(err)
	}
	defer db1.Close()

	// 连接数据库2
	db2, err := sql.Open("mysql", "root:password@tcp(host:port)/test2")
	if err != nil {
		log.Fatal(err)
	}
	defer db2.Close()

	// 获取数据库1中的表
	tablesDB1, err := getTables(db1)
	if err != nil {
		log.Fatal(err)
	}

	// 获取数据库2中的表
	tablesDB2, err := getTables(db2)
	if err != nil {
		log.Fatal(err)
	}

	// 比较表差异
	tableDiff := compareTables(tablesDB1, tablesDB2)

	// 打印表差异
	fmt.Println("表差异:")
	for _, diff := range tableDiff {
		if diff.TableOnlyInDB1 != "" {
			fmt.Printf("在数据库1中存在但在数据库2中不存在的表: %s\n", diff.TableOnlyInDB1)
		}
		if diff.TableOnlyInDB2 != "" {
			fmt.Printf("在数据库2中存在但在数据库1中不存在的表: %s\n", diff.TableOnlyInDB2)
		}
		fmt.Println("------------")
	}

	// 比较字段差异
	fieldDiff := compareFields(db1, db2, tablesDB1, tablesDB2)

	// 打印字段差异
	fmt.Println("字段差异:")
	for _, diff := range fieldDiff {
		fmt.Printf("表名：%s\n", diff.TableName)
		if len(diff.FieldsOnlyInDB1) > 0 {
			fmt.Printf("在数据库1中存在但在数据库2中不存在的字段：%v\n", diff.FieldsOnlyInDB1)
		}
		if len(diff.FieldsOnlyInDB2) > 0 {
			fmt.Printf("在数据库2中存在但在数据库1中不存在的字段：%v\n", diff.FieldsOnlyInDB2)
		}
		fmt.Println("------------")
	}
}

// 获取数据库中的表
func getTables(db *sql.DB) ([]string, error) {
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			log.Fatal(err)
		}
		tables = append(tables, tableName)
	}

	return tables, nil
}

// 比较表差异
func compareTables(tablesDB1, tablesDB2 []string) []TableDiff {
	var diff []TableDiff

	for _, table := range tablesDB1 {
		if !contains(tablesDB2, table) {
			diff = append(diff, TableDiff{
				TableOnlyInDB1: table,
			})
		}
	}

	for _, table := range tablesDB2 {
		if !contains(tablesDB1, table) {
			diff = append(diff, TableDiff{
				TableOnlyInDB2: table,
			})
		}
	}

	return diff
}

// 比较字段差异
func compareFields(db1, db2 *sql.DB, tablesDB1, tablesDB2 []string) []FieldDiff {
	var diff []FieldDiff

	for _, table := range tablesDB1 {
		fieldsDB1 := getTableFields(db1, table)
		fieldsDB2 := getTableFields(db2, table)
		fieldDiff := compareFieldSlices(fieldsDB1, fieldsDB2)
		if len(fieldDiff.FieldsOnlyInDB1) > 0 || len(fieldDiff.FieldsOnlyInDB2) > 0 {
			fieldDiff.TableName = table
			diff = append(diff, fieldDiff)
		}
	}

	for _, table := range tablesDB2 {
		fieldsDB1 := getTableFields(db1, table)
		fieldsDB2 := getTableFields(db2, table)
		fieldDiff := compareFieldSlices(fieldsDB1, fieldsDB2)
		if len(fieldDiff.FieldsOnlyInDB1) > 0 || len(fieldDiff.FieldsOnlyInDB2) > 0 {
			fieldDiff.TableName = table
			diff = append(diff, fieldDiff)
		}
	}

	return diff
}

// 获取表中的字段
func getTableFields(db *sql.DB, tableName string) []string {
	rows, err := db.Query(fmt.Sprintf("SHOW COLUMNS FROM `%s`", tableName))
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	var fields []string
	for rows.Next() {
		var field, dummy, dummy2, dummy3, dummy4, dummy5 any
		if err := rows.Scan(&field, &dummy, &dummy2, &dummy3, &dummy4, &dummy5); err != nil {
			log.Fatal(err)
		}

		fields = append(fields, fmt.Sprintf("%s", field))
		//fields = append(fields, field)
	}

	return fields
}

// 比较字段切片差异
func compareFieldSlices(fieldsDB1, fieldsDB2 []string) FieldDiff {
	var diff FieldDiff

	for _, field := range fieldsDB1 {
		if !contains(fieldsDB2, field) {
			diff.FieldsOnlyInDB1 = append(diff.FieldsOnlyInDB1, field)
		}
	}

	for _, field := range fieldsDB2 {
		if !contains(fieldsDB1, field) {
			diff.FieldsOnlyInDB2 = append(diff.FieldsOnlyInDB2, field)
		}
	}

	return diff
}

// 判断字符串切片中是否包含某个字符串
func contains(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}

// 表差异
type TableDiff struct {
	TableOnlyInDB1 string
	TableOnlyInDB2 string
}

// 字段差异
type FieldDiff struct {
	TableName       string
	FieldsOnlyInDB1 []string
	FieldsOnlyInDB2 []string
}
