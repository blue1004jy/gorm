package gorm

import (
	"crypto/sha1"
	"database/sql"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

type oracle struct {
	commonDialect
}

func init() {
	RegisterDialect("oracle", &oracle{})
}

func (oracle) GetName() string {
	return "oracle"
}

func (oracle) Quote(key string) string {
	return fmt.Sprintf("\"%s\"", strings.ToUpper(key))
}

func (oracle) SelectFromDummyTable() string {
	return "FROM dual"
}

func (oracle) BindVar(i int) string {
	return fmt.Sprintf(":%d", i)
}

func (s *oracle) DataTypeOf(field *StructField) string {
	var dataValue, sqlType, size, additionalType = ParseFieldStructForDialectOracle(field, s)

	if sqlType == "" {
		switch dataValue.Kind() {
		case reflect.Bool:
			sqlType = "CHAR(1)"
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uintptr:
			sqlType = "NUMBER(10)"
			/*
				if s.fieldCanAutoIncrement(field) {
					field.TagSettingsSet("AUTO_INCREMENT", "GENERATED ALWAYS")
					sqlType = "NUMBER GENERATED ALWAYS AS IDENTITY"
				} else {
					sqlType = "NUMBER"
				}
			*/
		case reflect.Int64, reflect.Uint64:
			/*
				if _, ok := field.TagSettings["AUTO_INCREMENT"]; ok || field.IsPrimaryKey {
					field.TagSettings["SEQUENCE"] = "SEQUENCE"
				}
			*/
			sqlType = "NUMBER(19)"
		case reflect.Float32, reflect.Float64:
			sqlType = "FLOAT"
		case reflect.String:
			// Maximum size of VARCHAR2 is 4000 bytes or characters if MAX_STRING_SIZE = STANDARD
			if size > 0 && size < 4000 {
				sqlType = fmt.Sprintf("VARCHAR2(%d)", size)
			} else {
				sqlType = "VARCHAR2(255)"
			}
		case reflect.Struct:
			if _, ok := dataValue.Interface().(time.Time); ok {
				sqlType = "TIMESTAMP"
			}
		default:
			if IsByteArrayOrSlice(dataValue) {
				sqlType = "BLOB"
			}
		}

	}

	if sqlType == "" {
		panic(fmt.Sprintf("invalid sql type %s (%s) for godror", dataValue.Type().Name(), dataValue.Kind().String()))
	}

	if strings.TrimSpace(additionalType) == "" {
		return sqlType
	}
	return fmt.Sprintf("%v %v", sqlType, additionalType)
}

func (s oracle) HasIndex(tableName string, indexName string) bool {
	var count int
	s.db.QueryRow("SELECT COUNT(*) FROM USER_INDEXES WHERE TABLE_NAME = :1 AND INDEX_NAME = :2", strings.ToUpper(tableName), strings.ToUpper(indexName)).Scan(&count)
	return count > 0
}

func (s oracle) HasForeignKey(tableName string, foreignKeyName string) bool {
	var count int
	s.db.QueryRow("SELECT COUNT(*) FROM USER_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'R' AND TABLE_NAME = :1 AND CONSTRAINT_NAME = :2", strings.ToUpper(tableName), strings.ToUpper(foreignKeyName)).Scan(&count)
	return count > 0
}

func (s oracle) HasTable(tableName string) bool {
	var count int
	s.db.QueryRow("SELECT COUNT(*) FROM USER_TABLES WHERE TABLE_NAME = :1", strings.ToUpper(tableName)).Scan(&count)
	return count > 0
}

func (s oracle) HasColumn(tableName string, columnName string) bool {
	var count int
	s.db.QueryRow("SELECT COUNT(*) FROM USER_TAB_COLUMNS WHERE TABLE_NAME = :1 AND COLUMN_NAME = :2", strings.ToUpper(tableName), strings.ToUpper(columnName)).Scan(&count)
	return count > 0
}

func (oracle) LimitAndOffsetSQL(limit, offset interface{}) (sql string, err error) {

	if limit != nil {
		if parsedLimit, err := strconv.ParseInt(fmt.Sprint(limit), 0, 0); err == nil && parsedLimit >= 0 {
			sql += fmt.Sprintf(" ROWNUM <= %d", limit)
		}
	}

	/*
		// Oracle 12c 이상 버전에서 동작
		// Todo: 테스트 후 동작에 문제가 생기면 위에 코드로 변경
		if offset == nil && limit == nil {
			sql = ""
			return
		}

		var parsedLimit, parsedOffset int64
		var errLimitParse, errOffsetParse error
		// Parsing the limit and the offset beforehand
		if limit != nil {
			parsedLimit, errLimitParse = strconv.ParseInt(fmt.Sprint(limit), 0, 0)
		}
		if offset != nil {
			parsedOffset, errOffsetParse = strconv.ParseInt(fmt.Sprint(offset), 0, 0)
		}

		// Offset clause comes first
		if errOffsetParse == nil && parsedOffset >= 0 {
			sql += fmt.Sprintf(" OFFSET %d", parsedOffset)
		} else if parsedLimit > 0 {
			// Set the offset as zero in case there is no offset > 0 specified for a limit > 0
			sql += fmt.Sprintf(" OFFSET %d", 0)
		}

		// Limit clause comes later
		if errLimitParse == nil && parsedLimit >= 0 {
			sql += fmt.Sprintf(" ROWS FETCH NEXT %d ROWS ONLY", parsedLimit)
		}
	*/
	return
}

func (s oracle) BuildKeyName(kind, tableName string, fields ...string) string {
	keyName := s.commonDialect.BuildKeyName(kind, tableName, fields...)
	if utf8.RuneCountInString(keyName) <= 30 {
		return keyName
	}
	h := sha1.New()
	h.Write([]byte(keyName))
	bs := h.Sum(nil)

	// sha1 is 40 digits, keep first 24 characters of destination
	destRunes := []rune(regexp.MustCompile("(_*[^a-zA-Z]+_*|_+)").ReplaceAllString(fields[0], "_"))
	result := fmt.Sprintf("%s%x", string(destRunes), bs)
	if len(result) <= 30 {
		return result
	}
	return result[:29]
}

// ParseFieldStructForDialectOracle get field's sql data type Oracle
var ParseFieldStructForDialectOracle = func(field *StructField, dialect Dialect) (fieldValue reflect.Value, sqlType string, size int, additionalType string) {
	// Get redirected field type
	var (
		reflectType = field.Struct.Type
		dataType, _ = field.TagSettingsGet("TYPE")
	)

	switch strings.ToLower(dataType) {
	case "bigint":
		dataType = "NUMBER(19)"
	case "integer":
		dataType = "NUMBER(10)"
	case "datetime":
		dataType = "TIMESTAMP"
	case "tinyint":
		dataType = "NUMBER(3)"
	default:
		if strings.Contains(strings.ToLower(dataType), "nvarchar") == true {
			dataType = strings.Replace(dataType, "nvarchar", "nvarchar2", 1)
		} else if strings.Contains(strings.ToLower(dataType), "binary") == true {
			dataType = strings.Replace(dataType, "binary", "raw", 1)
		}
	}

	for reflectType.Kind() == reflect.Ptr {
		reflectType = reflectType.Elem()
	}

	// Get redirected field value
	fieldValue = reflect.Indirect(reflect.New(reflectType))

	if gormDataType, ok := fieldValue.Interface().(interface {
		GormDataType(Dialect) string
	}); ok {
		dataType = gormDataType.GormDataType(dialect)
	}

	// Get scanner's real value
	if dataType == "" {
		var getScannerValue func(reflect.Value)
		getScannerValue = func(value reflect.Value) {
			fieldValue = value
			if _, isScanner := reflect.New(fieldValue.Type()).Interface().(sql.Scanner); isScanner && fieldValue.Kind() == reflect.Struct {
				getScannerValue(fieldValue.Field(0))
			}
		}
		getScannerValue(fieldValue)
	}

	// Default Size
	if num, ok := field.TagSettingsGet("SIZE"); ok {
		size, _ = strconv.Atoi(num)
	} else {
		size = 255
	}

	// Default type from tag setting
	notNull, _ := field.TagSettingsGet("NOT NULL")
	unique, _ := field.TagSettingsGet("UNIQUE")

	additionalType = notNull + " " + unique
	// Oarcle에서 Default 값이 앞에 와야 테이블이 생성됨
	if value, ok := field.TagSettingsGet("DEFAULT"); ok {
		additionalType = " DEFAULT " + value + " " + additionalType
	}

	if value, ok := field.TagSettingsGet("COMMENT"); ok {
		additionalType = additionalType + " COMMENT " + value
	}

	return fieldValue, dataType, size, strings.TrimSpace(additionalType)
}
