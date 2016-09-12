package dump

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"regexp"
	"strings"

	"github.com/juju/errors"
	"github.com/siddontang/go/log"
)

// Unlick mysqldump, Dumper is designed for parsing and syning data easily.
type Dumper struct {
	// mysqldump execution path, like mysqldump or /usr/bin/mysqldump, etc...
	ExecutionPath string

	Addr     string
	User     string
	Password string

	// Will override Databases
	Tables  []string
	TableDB string

	Databases []string

	IgnoreTables map[string][]string

	ErrOut io.Writer
}

func NewDumper(executionPath string, addr string, user string, password string) (*Dumper, error) {
	if len(executionPath) == 0 {
		executionPath = "mydumper"
	}

	path, err := exec.LookPath(executionPath)
	if err != nil {
		return nil, errors.Trace(err)
	}

	d := new(Dumper)
	d.ExecutionPath = path
	d.Addr = addr
	d.User = user
	d.Password = password
	d.Tables = make([]string, 0, 16)
	d.Databases = make([]string, 0, 16)
	d.IgnoreTables = make(map[string][]string)

	d.ErrOut = os.Stderr

	return d, nil
}

func (d *Dumper) SetErrOut(o io.Writer) {
	d.ErrOut = o
}

func (d *Dumper) AddDatabases(dbs ...string) {
	d.Databases = append(d.Databases, dbs...)
}

func (d *Dumper) AddTables(db string, tables ...string) {
	if d.TableDB != db {
		d.TableDB = db
		d.Tables = d.Tables[0:0]
	}

	d.Tables = append(d.Tables, tables...)
}

func (d *Dumper) AddIgnoreTables(db string, tables ...string) {
	t, _ := d.IgnoreTables[db]
	t = append(t, tables...)
	d.IgnoreTables[db] = t
}

func (d *Dumper) Reset() {
	d.Tables = d.Tables[0:0]
	d.TableDB = ""
	d.IgnoreTables = make(map[string][]string)
	d.Databases = d.Databases[0:0]
}

func (d *Dumper) Dump(w io.Writer) error {
	if strings.HasSuffix(d.ExecutionPath, "mydumper") {
		return d.mydumper(w)
	} else {
		return d.mysqldump(w)
	}
}

func (d *Dumper) mysqldump(w io.Writer) error {
	log.Trace("mysqldump")
	args := make([]string, 0, 16)

	// Common args
	seps := strings.Split(d.Addr, ":")
	args = append(args, fmt.Sprintf("--host=%s", seps[0]))
	if len(seps) > 1 {
		args = append(args, fmt.Sprintf("--port=%s", seps[1]))
	}

	args = append(args, fmt.Sprintf("--user=%s", d.User))
	args = append(args, fmt.Sprintf("--password=%s", d.Password))

	args = append(args, "--master-data")
	args = append(args, "--single-transaction")
	args = append(args, "--skip-lock-tables")

	// Disable uncessary data
	args = append(args, "--compact")
	args = append(args, "--skip-opt")
	args = append(args, "--quick")

	// We only care about data
	args = append(args, "--no-create-info")

	// Multi row is easy for us to parse the data
	args = append(args, "--skip-extended-insert")

	for db, tables := range d.IgnoreTables {
		for _, table := range tables {
			args = append(args, fmt.Sprintf("--ignore-table=%s.%s", db, table))
		}
	}

	if len(d.Tables) == 0 && len(d.Databases) == 0 {
		args = append(args, "--all-databases")
	} else if len(d.Tables) == 0 {
		args = append(args, "--databases")
		args = append(args, d.Databases...)
	} else {
		args = append(args, d.TableDB)
		args = append(args, d.Tables...)

		// If we only dump some tables, the dump data will not have database name
		// which makes us hard to parse, so here we add it manually.

		w.Write([]byte(fmt.Sprintf("USE `%s`;\n", d.TableDB)))
	}

	cmd := exec.Command(d.ExecutionPath, args...)

	cmd.Stderr = d.ErrOut
	cmd.Stdout = w
	log.Infof("Executing dump: %+v", cmd)

	return cmd.Run()
}

func (d *Dumper) mydumper(w io.Writer) error {
	dumpDir := ""
	files, err := ioutil.ReadDir("/tmp")
	if err != nil {
		panic(err)
	}
	var prev *os.FileInfo = nil
	for _, f := range files {
		if f.IsDir() && strings.HasPrefix(f.Name(), "mydumper") {
			if prev == nil || f.ModTime().After((*prev).ModTime()) {
				if _, err := os.Stat("/tmp/" + f.Name() + "/complete"); err == nil {
					copy := f; prev = &copy
				}
			}
		}
	}
	if prev != nil {
		dumpDir = "/tmp/" + (*prev).Name()
	}
	if dumpDir != "" {
		log.Infof("Reusing existing dump at %s", dumpDir)
		return d.parseDumpOuput(dumpDir, w)
	} else if dumpDir, err := ioutil.TempDir("", "mydumper"); err != nil {
		return err
	} else {
		//defer os.RemoveAll(dumpDir)

		args := make([]string, 0, 16)
		seps := strings.Split(d.Addr, ":")
		args = append(args, fmt.Sprintf("--host=%s", seps[0]))
		if len(seps) > 1 {
			args = append(args, fmt.Sprintf("--port=%s", seps[1]))
		}
		args = append(args, fmt.Sprintf("--user=%s", d.User))
		args = append(args, fmt.Sprintf("--password=%s", d.Password))

		// Output directory for dump files
		args = append(args, fmt.Sprintf("--outputdir=%s", dumpDir))

		// Required for RDS since FLUSH DATA not allowed
		args = append(args, "--lock-all-tables")

		// We only care about data
		args = append(args, "--no-schemas")

		//args = append(args, "--compress")
		args = append(args, "--compress-protocol")
		args = append(args, fmt.Sprintf("--long-query-guard=%d", 2000))

		if len(d.IgnoreTables) != 0 {
			fmt.Errorf("ignoreTables not supported when using mydumper")
		}

		if len(d.Tables) == 0 && len(d.Databases) == 0 {
			// handled by default
		} else if len(d.Tables) == 0 {
			for i := range d.Databases {
				args = append(args, "--database")
				args = append(args, d.Databases[i])
			}
		} else {
			args = append(args, "--tables-list")
			args = append(args, d.TableDB + "." + strings.Join(d.Tables, "," + d.TableDB + "."))
		}

		cmd := exec.Command(d.ExecutionPath, args...)
		cmd.Stderr = d.ErrOut
		cmd.Stdout = os.Stdout
		log.Infof("Executing dump: %+v", cmd)
		if err := cmd.Run(); err == nil {
			f := os.NewFile(0, dumpDir + "/complete"); f.Close()
			err = d.parseDumpOuput(dumpDir, w)
		}
		return err
	}
}

func (d *Dumper) parseDumpOuput(dir string, w io.Writer) error {
	files, err := ioutil.ReadDir(dir)
	if err == nil {
		var dumps []os.FileInfo
		for _, file := range files {
			if file.Name() == "metadata" {
				if err = d.parseMetadataFile(dir + "/" + file.Name(), w); err != nil {
					return err
				}
			} else {
				dumps = append(dumps, file)
			}
		}
		for _, dump := range dumps {
			if err = d.parseDumpFile(dir + "/" + dump.Name(), w); err != nil {
				return err
			}
		}
	}
	return err
}

func (d *Dumper) parseMetadataFile(meta string, w io.Writer) error {
	log.Infof("Parsing: %s", meta)
	if file, err := os.Open(meta); err != nil {
		return err
	} else {
		defer file.Close()

		scanner := bufio.NewScanner(file)

		binLogExp := regexp.MustCompile("\\s+Log:\\s+(.+)")
		binLogPosExp := regexp.MustCompile("\\s+Pos:\\s+(\\d+)")

		binLog := ""
		binLogPos := ""

		for scanner.Scan() {
			line := scanner.Text()
			if m := binLogExp.FindStringSubmatch(line); len(m) > 0 {
				binLog = m[1]
			} else if m := binLogPosExp.FindStringSubmatch(line); len(m) > 0 {
				binLogPos = m[1]
			}
		}

		if err = scanner.Err(); err != nil {
			return err
		} else {
			stmnt := fmt.Sprintf("CHANGE MASTER TO MASTER_LOG_FILE='%s', MASTER_LOG_POS=%s;\n", binLog, binLogPos)
			log.Debug(stmnt)
			_, err = fmt.Fprintf(w, stmnt)
			return err
		}
	}
}

func (d *Dumper) parseDumpFile(dump string, w io.Writer) error {
	log.Infof("Parsing: %s", dump)
	lastSlash := strings.LastIndex(dump, "/") + 1
	database := strings.Split(dump[lastSlash:len(dump)], ".")[0]
	stmnt := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`;\n\nUSE `%s`;\n", database, database)
	log.Debug(stmnt)
	if _, err := fmt.Fprintf(w, stmnt); err != nil {
		return err
	} else if file, err := os.Open(dump); err != nil {
		return err
	} else {
		defer file.Close()
		scanner := bufio.NewScanner(file)
		scanner.Buffer(make([]byte, 1024 * 1024), 1024 * 1024)
		insertExp := regexp.MustCompile("^INSERT INTO `.+` VALUES$")
		valuesExp := regexp.MustCompile("^\\(.+\\)[;,]$")

		n := 0

		for scanner.Scan() {
			n = n + 1
			if n % 10000 == 0 {
				log.Infof("%d lines parsed ", n)
			}
			line := scanner.Text()
			if insertExp.FindString(line) != "" {
				stmnt := fmt.Sprintf("%s\n", line)
				_, err = w.Write([]byte(stmnt))
			} else if valuesExp.FindString(line) != "" {
				stmnt := fmt.Sprintf("%s\n", line)
				_, err = w.Write([]byte(stmnt))
			}
			if err != nil {
				log.Errorf("Failed after %d lines parsed due to %v: %v", n, err, line)
				return err
			}
		}
		log.Infof("Parsing completed with %d lines parsed", n)
		return scanner.Err()
	}
}

// Dump MySQL and parse immediately
func (d *Dumper) DumpAndParse(h ParseHandler) error {
	r, w := io.Pipe()

	done := make(chan error, 1)
	go func() {
		err := Parse(r, h)
		r.CloseWithError(err)
		done <- err
	}()

	err := d.Dump(w)
	w.CloseWithError(err)

	err = <-done

	return errors.Trace(err)
}
