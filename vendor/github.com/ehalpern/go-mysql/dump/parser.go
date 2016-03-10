package dump

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"strconv"

	"github.com/juju/errors"
	"github.com/siddontang/go/log"
)

var (
	ErrSkip = errors.New("Handler error, but skipped")
)

type ParseHandler interface {
	// Parse CHANGE MASTER TO MASTER_LOG_FILE=name, MASTER_LOG_POS=pos;
	BinLog(name string, pos uint64) error

	Data(schema string, table string, values []string) error

	Complete() error
}


// Parse the dump data with Dumper generate.
// It can not parse all the data formats with mysqldump outputs
func Parse(r io.Reader, h ParseHandler) error {
	rb := bufio.NewReaderSize(r, 1024*16)

	binlogExp := regexp.MustCompile("^CHANGE MASTER TO MASTER_LOG_FILE='(.+)', MASTER_LOG_POS=(\\d+);")
	useExp := regexp.MustCompile("^USE `(.+)`;")
	insertWithValuesExp := regexp.MustCompile("^INSERT INTO `(.+)` VALUES \\((.+)\\);")
	insertExp := regexp.MustCompile("INSERT INTO `(.+)` VALUES")
	valuesExp := regexp.MustCompile("^\\((.+)\\)[;,]")

	var db string
	var binlogParsed bool
	var currentInsertTable string

	for {
		line, err := rb.ReadString('\n')
		if err != nil && err != io.EOF {
			return errors.Trace(err)
		} else if err == io.EOF {
			break
		}

		if firstChar := line[0]; firstChar == '"' || firstChar == '\'' {
			// remove quotes
			line = line[0 : len(line)-1]
		}

		if !binlogParsed {
			if m := binlogExp.FindAllStringSubmatch(line, -1); len(m) == 1 {
				log.Infof("Parse binlog: %s", line)
				name := m[0][1]
				pos, err := strconv.ParseUint(m[0][2], 10, 64)
				if err != nil {
					return errors.Errorf("parse binlog %v err, invalid number", line)
				}

				if err = h.BinLog(name, pos); err != nil && err != ErrSkip {
					return errors.Trace(err)
				}

				binlogParsed = true
			}
		}

		if m := useExp.FindStringSubmatch(line); len(m) == 2 {
			db = m[1]
		} else if m = insertWithValuesExp.FindStringSubmatch(line); len(m) == 3 {
			log.Debugf("Parse insert: %s", line)
			table := m[1]
			values, err := parseValues(m[2])
			if err != nil {
				return errors.Errorf("parse values %v err", line)
			}

			if err = h.Data(db, table, values); err != nil && err != ErrSkip {
				return errors.Trace(err)
			}
		} else if m = insertExp.FindStringSubmatch(line); len(m) == 2 {
			log.Debugf("Parse insert start: %s", line)
			currentInsertTable = m[1]
		} else if m = valuesExp.FindStringSubmatch(line); len(m) == 2 {
			log.Debugf("Parse insert value: %s", line)
			values, err := parseValues(m[1])
			if err != nil {
				return errors.Errorf("parse values %v err", line)
			}

			if err = h.Data(db, currentInsertTable, values); err != nil && err != ErrSkip {
				return errors.Trace(err)
			}
		}
	}
	return h.Complete()
}

func parseValues(str string) ([]string, error) {
	// values are seperated by comma, but we can not split using comma directly
	// string is enclosed by single quote

	// a simple implementation, may be more robust later.

	values := make([]string, 0, 8)

	i := 0
	for i < len(str) {
		if firstChar := str[i]; firstChar != '\'' && firstChar != '"' {
			// no string, read until comma
			j := i + 1
			for ; j < len(str) && str[j] != ','; j++ {
			}
			values = append(values, str[i:j])
			// skip ,
			i = j + 1
		} else {
			// read string until another single quote
			j := i + 1

			for j < len(str) {
				if str[j] == '\\' {
					// skip escaped character
					j += 2
					continue
				} else if str[j] == firstChar {
					break
				} else {
					j++
				}
			}

			if j >= len(str) {
				return nil, fmt.Errorf("parse quote values error")
			}

			values = append(values, str[i:j+1])
			// skip ' and ,
			i = j + 2
		}

		// need skip blank???
	}

	return values, nil
}
